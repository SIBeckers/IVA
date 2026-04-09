# iva_job/firestarr.py
"""
FireSTARR GeoTIFF retrieval + reprojection + mosaic, using Azure Blob Storage Python SDK.

Key OOM-hardening:
  - Azure downloads are streamed (no readall()).
  - Mosaic is written by rasterio.merge.merge() directly to dst_path (streaming).
  - Optional bounds clipping via FIRESTARR_MOSAIC_BOUNDS="xmin,ymin,xmax,ymax".
  - Hard sanity checks on mosaic grid size.
  - GDAL cache is capped via rasterio.Env(GDAL_CACHEMAX=...).

See rasterio.merge.merge docs for dst_path/dst_kwds/mem_limit usage. [2](https://whitephil.github.io/GIS-workshops/Rasterio/notebooks/2.%20Rasterio%20Clip%20Operations.html)
"""

from __future__ import annotations

import math
import os
import re
import time
import logging
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional, Sequence, Tuple
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import rasterio
from rasterio.coords import BoundingBox
from rasterio.merge import merge
from rasterio.transform import Affine
from rasterio.vrt import WarpedVRT
from rasterio.warp import Resampling

from azure.storage.blob import ContainerClient
from azure.storage.blob import BlobPrefix  # returned by walk_blobs when delimiter is used

log = logging.getLogger("iva.firestarr")
_TS_12_RE = re.compile(r"(\d{12})")  # YYYYMMDDHHMM


# -----------------------
# Logging
# -----------------------
def _setup_logging(force: bool = False) -> None:
    lvl = os.getenv("FIRESTARR_LOG_LEVEL", os.getenv("LOG_LEVEL", "INFO")).upper().strip()
    level = getattr(logging, lvl, logging.INFO)
    root = logging.getLogger()
    if force:
        for h in list(root.handlers):
            root.removeHandler(h)
    if not root.handlers or force:
        logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
    else:
        root.setLevel(level)


def _sample(items: Sequence[str], n: int = 10) -> List[str]:
    items = list(items or [])
    if len(items) <= n:
        return items
    return items[:n] + ["..."]


_setup_logging(force=False)
log.info("Loaded firestarr module from: %s", __file__)


# -----------------------
# Env helpers
# -----------------------
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def _ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _parse_bounds_env(name: str) -> Optional[BoundingBox]:
    """
    Parse bounds env var like: "xmin,ymin,xmax,ymax"
    """
    raw = _env(name, "").strip()
    if not raw:
        return None
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) != 4:
        raise ValueError(f"{name} must be 'xmin,ymin,xmax,ymax' (got: {raw})")
    xmin, ymin, xmax, ymax = map(float, parts)
    if xmax <= xmin or ymax <= ymin:
        raise ValueError(f"{name} invalid bounds: {raw}")
    return BoundingBox(xmin, ymin, xmax, ymax)


def _ensure_container_url_has_sas(container_url: str) -> str:
    p = urlparse(container_url)
    if p.query:
        return container_url
    sas = _env("AZURE_SAS_TOKEN", "").lstrip("?")
    if not sas:
        return container_url
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q_sas = dict(parse_qsl(sas, keep_blank_values=True))
    q.update(q_sas)
    new_p = p._replace(query=urlencode(q))
    return urlunparse(new_p)


def _container_client() -> ContainerClient:
    raw = _env("FIRESTARR_BLOB_URL", "https://sawipsprodca.blob.core.windows.net/firestarr").rstrip("/")
    url = _ensure_container_url_has_sas(raw)
    cc = ContainerClient.from_container_url(url)
    if log.isEnabledFor(logging.DEBUG):
        has_q = bool(urlparse(url).query)
        log.debug("ContainerClient created: url=%s (has_sas=%s)", cc.url, has_q)
    return cc


# -----------------------
# Azure traversal helpers
# -----------------------
def _extract_ts_yyyymmddhhmm(path: str) -> Optional[str]:
    m = _TS_12_RE.search(path)
    return m.group(1) if m else None


def _pick_latest_run_prefix_for_date(prefixes: Sequence[str], run_ymd: str) -> Optional[str]:
    matches: List[str] = []
    for p in prefixes:
        ts = _extract_ts_yyyymmddhhmm(p)
        if ts and ts.startswith(run_ymd):
            matches.append(p)
    return sorted(matches)[-1] if matches else None


def _list_child_prefixes(cc: ContainerClient, base_prefix: str) -> List[str]:
    base_prefix = base_prefix.strip("/") + "/"
    out: List[str] = []
    for item in cc.walk_blobs(name_starts_with=base_prefix, delimiter="/"):
        if isinstance(item, BlobPrefix):
            out.append(item.name)
    return out


def _list_blobs_flat(cc: ContainerClient, prefix: str) -> List[str]:
    prefix = prefix.lstrip("/")
    names: List[str] = []
    for b in cc.list_blobs(name_starts_with=prefix):
        names.append(b.name)
    return names


# -----------------------
# Discovery
# -----------------------
def _discover_archive_blob(run_date: date, horizon: int) -> Optional[str]:
    cc = _container_client()
    archive_prefix = _env("FIRESTARR_ARCHIVE_PREFIX", "archive").strip("/").strip() + "/"
    run_ymd = _ymd(run_date)

    run_prefixes = _list_child_prefixes(cc, archive_prefix)
    run_prefix = _pick_latest_run_prefix_for_date(run_prefixes, run_ymd)
    if not run_prefix:
        return None

    run_ts = _extract_ts_yyyymmddhhmm(run_prefix)
    if not run_ts:
        raise RuntimeError(f"Archive run prefix did not contain YYYYMMDDHHMM: {run_prefix}")

    forecast_ymd = _ymd(run_date + timedelta(days=int(horizon) - 1))
    expected_name = f"firestarr_{run_ts}_day_{int(horizon):02d}_{forecast_ymd}.tif"
    expected_blob = f"{run_prefix}{expected_name}"

    try:
        cc.get_blob_client(expected_blob).get_blob_properties()
        return expected_blob
    except Exception as e:
        blobs = _list_blobs_flat(cc, run_prefix)
        tifs = [b for b in blobs if b.lower().endswith(".tif")]
        raise RuntimeError(
            f"Expected archive file not found: {expected_blob}. Available tifs: {[Path(t).name for t in tifs]}"
        ) from e


def _m3_root_candidates() -> List[str]:
    multi = _env("FIRESTARR_M3_PREFIXES", "").strip()
    single = _env("FIRESTARR_M3_PREFIX", "").strip()
    if multi:
        roots = [r.strip().strip("/") for r in multi.split(",") if r.strip()]
    elif single:
        roots = [single.strip().strip("/")]
    else:
        roots = ["firestarr", "firestarr/firestarr"]
    return [r + "/" for r in roots]


def _discover_m3_blobs(run_date: date, horizon: int) -> Optional[List[str]]:
    cc = _container_client()
    run_ymd = _ymd(run_date)

    chosen_run_prefix: Optional[str] = None
    for root in _m3_root_candidates():
        prefixes = _list_child_prefixes(cc, root)
        m3_prefixes = [p for p in prefixes if "m3_" in p]
        run_prefix = _pick_latest_run_prefix_for_date(m3_prefixes, run_ymd)
        if run_prefix:
            chosen_run_prefix = run_prefix
            break

    if not chosen_run_prefix:
        return None

    forecast_ymd = _ymd(run_date + timedelta(days=int(horizon) - 1))
    forecast_prefix = f"{chosen_run_prefix}{forecast_ymd}/"

    blobs = _list_blobs_flat(cc, forecast_prefix)
    tifs = [b for b in blobs if b.lower().endswith(".tif")]
    return tifs or None


# -----------------------
# Download (OOM-safe streaming)
# -----------------------
def _safe_name(blob_name: str) -> str:
    return Path(blob_name).name


def _download_one(cc: ContainerClient, blob_name: str, dest: Path) -> Path:
    """
    Download blob streaming in chunks to avoid loading entire blobs into RAM.
    NOTE: Your previous version used stream.readall(), which can blow memory with concurrency. [1](https://041gc-my.sharepoint.com/personal/justin_beckers_nrcan-rncan_gc_ca/Documents/Microsoft%20Copilot%20Chat%20Files/firestarr.py)
    """
    tries, delay = 0, 1.0
    chunk_size = int(_env("FIRESTARR_DOWNLOAD_CHUNK_MB", "8")) * 1024 * 1024

    while True:
        tries += 1
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            bc = cc.get_blob_client(blob_name)

            stream = bc.download_blob()
            with open(dest, "wb") as f:
                # Stream in chunks; avoids huge transient allocations.
                for chunk in stream.chunks():
                    f.write(chunk)

            return dest

        except Exception as e:
            if tries >= 5:
                log.error("Download failed after %d tries: %s (%s)", tries, blob_name, e)
                raise
            log.warning(
                "Download failed (try %d/5): %s (%s). Retrying in %.1fs",
                tries, blob_name, e, delay
            )
            time.sleep(delay)
            delay = min(delay * 2, 16.0)


def download_blobs(blob_names: Sequence[str]) -> List[Path]:
    cc = _container_client()
    tmp_root = Path(_env("FIRESTARR_TMP", _env("IVA_TMP", "/tmp")))
    tmpdir = Path(tempfile.mkdtemp(prefix="firestarr_", dir=str(tmp_root)))

    # Concurrency is a memory multiplier. Default it lower.
    max_workers = int(_env("FIRESTARR_MAX_WORKERS", "4"))

    from concurrent.futures import ThreadPoolExecutor, as_completed

    out: List[Path] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = []
        for name in blob_names:
            fn = tmpdir / _safe_name(name)
            futs.append(ex.submit(_download_one, cc, name, fn))
        for fut in as_completed(futs):
            p = fut.result()
            log.info("Downloaded: %s", p.name)
            out.append(p)
    return out


def _set_band_description_compat(dst, bidx: int, desc: str) -> None:
    if hasattr(dst, "set_band_description"):
        dst.set_band_description(bidx, desc)
        return
    try:
        descs = list(getattr(dst, "descriptions", ()) or ())
        while len(descs) < bidx:
            descs.append(None)
        descs[bidx - 1] = desc
        dst.descriptions = tuple(descs)
    except Exception:
        pass


# -----------------------
# Reproject helpers
# -----------------------
def _write_reproject_from_vrt(vrt: WarpedVRT, out_path: Path, *, nodata) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    data = vrt.read(1)
    profile = vrt.profile.copy()
    profile.update(
        driver="GTiff",
        compress="DEFLATE",
        tiled=True,
        blockxsize=256,
        blockysize=256,
        count=1,
        nodata=nodata,
        BIGTIFF="IF_SAFER",
    )
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(data, 1)
        _set_band_description_compat(dst, 1, "probability")
    return out_path


# -----------------------
# Mosaic grid helpers
# -----------------------
def _union_bounds(datasets) -> BoundingBox:
    b0 = datasets[0].bounds
    left, bottom, right, top = b0.left, b0.bottom, b0.right, b0.top
    for ds in datasets[1:]:
        b = ds.bounds
        left = min(left, b.left)
        bottom = min(bottom, b.bottom)
        right = max(right, b.right)
        top = max(top, b.top)
    return BoundingBox(left, bottom, right, top)


def _aligned_bounds(bounds: BoundingBox, res: float) -> BoundingBox:
    left = math.floor(bounds.left / res) * res
    bottom = math.floor(bounds.bottom / res) * res
    right = math.ceil(bounds.right / res) * res
    top = math.ceil(bounds.top / res) * res
    return BoundingBox(left, bottom, right, top)


def _grid_from_bounds(bounds: BoundingBox, res: float) -> tuple[int, int, Affine]:
    width = int(round((bounds.right - bounds.left) / res))
    height = int(round((bounds.top - bounds.bottom) / res))
    transform = Affine(res, 0.0, bounds.left, 0.0, -res, bounds.top)
    return width, height, transform


# -----------------------
# Mosaic (reproject first, streaming merge)
# -----------------------
def mosaic_tiles_reproject_first(
    tile_paths: Sequence[Path],
    out_path: Path,
    *,
    dst_epsg: int = 3978,
    res_m: float = 100.0,
    method: str = "max",
) -> Path:
    """
    Reproject each tile via WarpedVRT (nearest, fixed resolution) and mosaic via rasterio.merge
    streaming to dst_path to avoid huge in-memory arrays. [2](https://whitephil.github.io/GIS-workshops/Rasterio/notebooks/2.%20Rasterio%20Clip%20Operations.html)
    """
    if not tile_paths:
        raise ValueError("No tiles provided for mosaic")

    dst_crs = f"EPSG:{dst_epsg}"
    datasets = []
    vrts = []
    nodata = None

    try:
        for p in tile_paths:
            ds = rasterio.open(p)
            if ds.count != 1:
                raise RuntimeError(f"Expected single-band tile; got {ds.count} bands: {p}")
            if ds.nodata is None:
                raise RuntimeError(f"Tile has no nodata set: {p}")

            if nodata is None:
                nodata = ds.nodata
            elif ds.nodata != nodata:
                raise RuntimeError(f"Tile nodata mismatch: {p} has {ds.nodata}, expected {nodata}")

            vrt = WarpedVRT(
                ds,
                crs=dst_crs,
                resampling=Resampling.nearest,
                resolution=(res_m, res_m),
                nodata=nodata,
            )
            datasets.append(ds)
            vrts.append(vrt)

        res = float(res_m)

        # Prefer explicit bounds if provided to avoid mosaicking huge empty space.
        user_bounds = _parse_bounds_env("FIRESTARR_MOSAIC_BOUNDS")
        ub = user_bounds or _union_bounds(vrts)
        ab = _aligned_bounds(ub, res)
        width, height, transform = _grid_from_bounds(ab, res)

        # Sanity guardrails (fail loudly rather than get OOM-killed)
        max_pixels = int(_env("FIRESTARR_MAX_PIXELS", "2000000000"))  # 2B default
        pixels = width * height
        if pixels <= 0:
            raise RuntimeError(f"Computed invalid mosaic grid: {width}x{height} for bounds={ab}")
        if pixels > max_pixels:
            raise RuntimeError(
                f"Mosaic grid too large: {width}x{height} ({pixels:,} pixels) > FIRESTARR_MAX_PIXELS={max_pixels:,}. "
                f"Set FIRESTARR_MOSAIC_BOUNDS to clip output."
            )

        # Estimate raw payload (not compressed) for intuition
        dtype = vrts[0].dtypes[0]
        bpp = rasterio.dtypes.get_minimum_dtype([dtype]).itemsize if hasattr(dtype, "itemsize") else 4
        est_gb = (pixels * 4) / (1024**3)  # assume ~4 bytes per pixel typical float32
        log.info("Mosaic bounds used: %s (user=%s)", ab, bool(user_bounds))
        log.info("Mosaic grid: %dx%d (~%.2f GB raw for float32-ish)", width, height, est_gb)

        out_path.parent.mkdir(parents=True, exist_ok=True)

        profile = vrts[0].profile.copy()
        profile.update(
            driver="GTiff",
            height=height,
            width=width,
            transform=transform,
            crs=dst_crs,
            count=1,
            nodata=nodata,
            compress="DEFLATE",
            tiled=True,
            blockxsize=256,
            blockysize=256,
            BIGTIFF="IF_SAFER",
        )

        mem_limit_mb = int(_env("FIRESTARR_MERGE_MEM_LIMIT_MB", "128"))
        gdal_cache_mb = int(_env("FIRESTARR_GDAL_CACHEMAX_MB", "256"))
        gdal_threads = _env("FIRESTARR_GDAL_NUM_THREADS", "1")

        log.info("Writing mosaic to %s", out_path)
        log.info(
            "Updated profile for mosaic (streaming), size=%dx%d, mem_limit=%dMB, GDAL_CACHEMAX=%dMB, GDAL_NUM_THREADS=%s",
            width, height, mem_limit_mb, gdal_cache_mb, gdal_threads
        )

        # Cap GDAL cache to avoid silent ballooning.
        with rasterio.Env(GDAL_CACHEMAX=gdal_cache_mb, GDAL_NUM_THREADS=gdal_threads):
            merge(
                vrts,
                method=method,
                nodata=nodata,
                target_aligned_pixels=True,
                dst_path=str(out_path),
                dst_kwds=profile,
                mem_limit=mem_limit_mb,
                # bounds is redundant here because we already baked bounds into transform/width/height.
            )

        with rasterio.open(out_path, "r+") as dst:
            _set_band_description_compat(dst, 1, "probability")

        log.info("Wrote mosaic → %s (method=%s, CRS=%s, res=%.1fm)", out_path, method, dst_crs, res_m)
        return out_path

    finally:
        for vrt in vrts:
            try:
                vrt.close()
            except Exception:
                pass
        for ds in datasets:
            try:
                ds.close()
            except Exception:
                pass


def reproject_single(
    src_path: Path,
    out_path: Path,
    *,
    dst_epsg: int = 3978,
    res_m: float = 100.0,
) -> Path:
    dst_crs = f"EPSG:{dst_epsg}"
    with rasterio.open(src_path) as src:
        if src.count != 1:
            raise RuntimeError(f"Expected single-band raster; got {src.count} bands: {src_path}")
        if src.nodata is None:
            raise RuntimeError(f"Source raster has no nodata set: {src_path}")

        with WarpedVRT(
            src,
            crs=dst_crs,
            resampling=Resampling.nearest,
            resolution=(res_m, res_m),
            nodata=src.nodata,
        ) as vrt:
            return _write_reproject_from_vrt(vrt, out_path, nodata=src.nodata)


# -----------------------
# Public API
# -----------------------
def get_firestarr_mosaic(run_date: date, day: int, out_dir: Path) -> Path:
    dst_epsg = int(_env("FIRESTARR_DST_EPSG", "3978"))
    res_m = float(_env("FIRESTARR_RES_M", "100.0"))

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    out_name = f"firestarr_{_ymd(run_date)}_day_{int(day):02d}_probability_epsg{dst_epsg}_{int(res_m)}m.tif"
    out_path = out_dir / out_name

    # 1) archive
    archive_blob = _discover_archive_blob(run_date, int(day))
    if archive_blob:
        log.info("Using archive blob: %s", archive_blob)
        local = download_blobs([archive_blob])[0]
        return reproject_single(local, out_path, dst_epsg=dst_epsg, res_m=res_m)

    # 2) m3 tiles
    m3_blobs = _discover_m3_blobs(run_date, int(day))
    if m3_blobs:
        log.info("Using m3 blobs: %d tile(s)", len(m3_blobs))
        tiles = download_blobs(m3_blobs)
        return mosaic_tiles_reproject_first(tiles, out_path, dst_epsg=dst_epsg, res_m=res_m, method="max")

    raise RuntimeError(f"No FireSTARR data found for run_date={run_date.isoformat()} day={day} in archive or m3")


def get_firestarr_dayN_and_day7(run_date: date, forecast_day: int, out_dir: Path) -> Tuple[Path, Path]:
    p = get_firestarr_mosaic(run_date, int(forecast_day), out_dir)
    p7 = get_firestarr_mosaic(run_date, 7, out_dir)
    return p, p7


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch FireSTARR and produce EPSG:3978 mosaics.")
    parser.add_argument("--run-date", type=str, default=date.today().isoformat())
    parser.add_argument("--day", type=int, default=3)
    parser.add_argument("--out-dir", type=str, default="./output")
    parser.add_argument("--also-day7", action="store_true")
    parser.add_argument("--log-level", type=str, default=os.getenv("FIRESTARR_LOG_LEVEL", "INFO"))
    args = parser.parse_args()

    os.environ["FIRESTARR_LOG_LEVEL"] = (args.log_level or "INFO").upper().strip()
    _setup_logging(force=True)

    rd = date.fromisoformat(args.run_date)
    out = Path(args.out_dir)
    p = get_firestarr_mosaic(rd, args.day, out)
    log.info("Output: %s", p)
    if args.also_day7 and args.day != 7:
        p7 = get_firestarr_mosaic(rd, 7, out)
        log.info("Output day7: %s", p7)