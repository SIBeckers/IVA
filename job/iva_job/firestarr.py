# job/iva_job/firestarr.py
"""
FireSTARR GeoTIFF retrieval + reprojection + mosaic, using Azure Blob Storage Python SDK.

Blob layouts (searched in this order):
 1) archive/YYYYMMDDHHMM/ -> already-mosaiced GeoTIFF per forecast day
    filenames: firestarr_YYYYMMDDHHMM_day_XX_YYYYMMDD.tif

 2) M3 tiles -> multiple tiles to mosaic. Two common roots exist:
    A) firestarr/m3_YYYYMMDDHHMM/YYYYMMDD/YYN_XXXXX.tif
    B) firestarr/firestarr/m3_YYYYMMDDHHMM/YYYYMMDD/YYN_XXXXX.tif

Selection rules:
 - Choose run folder by exact run_date (YYYYMMDD), selecting the latest HHMM within that date.
 - NO fallback to earlier dates.
 - For archive: pick exactly the file firestarr_{run_ts}_day_{DD}_{forecast_ymd}.tif
   where forecast_ymd = run_date + (day-1).
 - For m3: pick forecast folder forecast_ymd = run_date + (day-1), list all *.tif inside.
 - Reproject to EPSG:3978 @ 100m using nearest (WarpedVRT), then mosaic with rasterio.merge(method="max").

Auth:
 - FIRESTARR_BLOB_URL must be a *container URL*, ideally including SAS token.
 - If FIRESTARR_BLOB_URL has no query string, we will append AZURE_SAS_TOKEN (if set).

SDK:
 - Uses azure-storage-blob ContainerClient.from_container_url(). [1](https://github.com/Azure/azure-storage-python)
 - Uses walk_blobs(delimiter="/") to traverse virtual folders/prefixes. [3](https://041gc-my.sharepoint.com/personal/justin_beckers_nrcan-rncan_gc_ca/Documents/Microsoft%20Copilot%20Chat%20Files/firestarr.py)

Debug:
 - Set FIRESTARR_LOG_LEVEL=DEBUG or pass --log-level DEBUG to see searched prefixes and results.
"""

from __future__ import annotations

import os
import re
import time
import logging
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import rasterio
from rasterio.merge import merge
from rasterio.vrt import WarpedVRT
from rasterio.warp import Resampling

# Azure SDK
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

    log.debug("FireSTARR logging initialized: level=%s", lvl)


def _sample(items: Sequence[str], n: int = 10) -> List[str]:
    items = list(items or [])
    if len(items) <= n:
        return items
    return items[:n] + ["..."]


_setup_logging(force=False)


# -----------------------
# Env helpers
# -----------------------
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def _ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _ensure_container_url_has_sas(container_url: str) -> str:
    """
    If FIRESTARR_BLOB_URL doesn't include a query string, append AZURE_SAS_TOKEN (if present).
    ContainerClient.from_container_url() supports full container URI construction. [1](https://github.com/Azure/azure-storage-python)
    """
    p = urlparse(container_url)
    if p.query:
        return container_url

    sas = _env("AZURE_SAS_TOKEN", "").lstrip("?")
    if not sas:
        # SDK will still work for public containers; for private, this will fail with auth.
        return container_url

    # Merge existing (none) with sas
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q_sas = dict(parse_qsl(sas, keep_blank_values=True))
    q.update(q_sas)
    new_p = p._replace(query=urlencode(q))
    return urlunparse(new_p)


def _container_client() -> ContainerClient:
    """
    Build ContainerClient from the container URL.
    Microsoft docs explicitly recommend from_container_url when you have the full container URI. [1](https://github.com/Azure/azure-storage-python)
    """
    raw = _env("FIRESTARR_BLOB_URL", "https://sawipsprodca.blob.core.windows.net/firestarr").rstrip("/")
    url = _ensure_container_url_has_sas(raw)

    # Note: credential is optional if SAS token is already in the URL. [1](https://github.com/Azure/azure-storage-python)
    cc = ContainerClient.from_container_url(url)
    if log.isEnabledFor(logging.DEBUG):
        # Don’t print SAS values; just indicate presence.
        has_q = bool(urlparse(url).query)
        log.debug("ContainerClient created: url=%s (has_sas=%s)", cc.url, has_q)
    return cc


# -----------------------
# Azure "folder" traversal helpers
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
    """
    Return immediate child prefixes (virtual folders) under base_prefix using delimiter="/".
    walk_blobs(delimiter="/") yields BlobPrefix entries for virtual directories. [3](https://041gc-my.sharepoint.com/personal/justin_beckers_nrcan-rncan_gc_ca/Documents/Microsoft%20Copilot%20Chat%20Files/firestarr.py)
    """
    base_prefix = base_prefix.strip("/") + "/"
    out: List[str] = []

    if log.isEnabledFor(logging.DEBUG):
        log.debug("LIST prefixes under: %s", base_prefix)

    for item in cc.walk_blobs(name_starts_with=base_prefix, delimiter="/"):
        if isinstance(item, BlobPrefix):
            out.append(item.name)

    if log.isEnabledFor(logging.DEBUG):
        log.debug("LIST result: %d prefixes sample=%s", len(out), _sample(out))

    return out


def _list_blobs_flat(cc: ContainerClient, prefix: str) -> List[str]:
    """
    Flat blob listing by prefix.
    Azure SDK supports prefix filtering as the standard way to filter blob names. [5](https://learn.microsoft.com/en-us/python/api/overview/azure/storage-blob-readme?view=azure-python)
    """
    prefix = prefix.lstrip("/")
    names: List[str] = []
    if log.isEnabledFor(logging.DEBUG):
        log.debug("LIST blobs under prefix: %s", prefix)

    for b in cc.list_blobs(name_starts_with=prefix):
        names.append(b.name)

    if log.isEnabledFor(logging.DEBUG):
        log.debug("LIST blobs result: %d blobs sample=%s", len(names), _sample(names))

    return names


# -----------------------
# Discovery
# -----------------------
def _discover_archive_blob(run_date: date, horizon: int) -> Optional[str]:
    """
    archive/YYYYMMDDHHMM/ contains GeoTIFFs named:
      firestarr_YYYYMMDDHHMM_day_XX_YYYYMMDD.tif

    We:
      - choose latest run folder for run_date (YYYYMMDD)
      - compute forecast_ymd = run_date + (horizon-1)
      - check existence of the expected file
    """
    cc = _container_client()

    archive_prefix = _env("FIRESTARR_ARCHIVE_PREFIX", "archive").strip("/").strip()
    archive_prefix = archive_prefix + "/"
    run_ymd = _ymd(run_date)
    log.debug("Discover archive: base=%s run_date=%s horizon=%s", archive_prefix, run_date.isoformat(), horizon)

    run_prefixes = _list_child_prefixes(cc, archive_prefix)
    run_prefix = _pick_latest_run_prefix_for_date(run_prefixes, run_ymd)
    if not run_prefix:
        log.debug("Archive: no run prefix matched run_ymd=%s under %s", run_ymd, archive_prefix)
        return None

    run_ts = _extract_ts_yyyymmddhhmm(run_prefix)
    if not run_ts:
        raise RuntimeError(f"Archive run prefix did not contain YYYYMMDDHHMM: {run_prefix}")

    forecast_ymd = _ymd(run_date + timedelta(days=int(horizon) - 1))
    expected_name = f"firestarr_{run_ts}_day_{int(horizon):02d}_{forecast_ymd}.tif"
    expected_blob = f"{run_prefix}{expected_name}"

    log.debug(
        "Archive: picked run_prefix=%s run_ts=%s forecast_ymd=%s expected=%s",
        run_prefix,
        run_ts,
        forecast_ymd,
        expected_blob,
    )

    # Existence check via get_blob_properties (fast and explicit)
    try:
        cc.get_blob_client(expected_blob).get_blob_properties()
        return expected_blob
    except Exception as e:
        # If naming changes, provide candidates by listing .tif within the run prefix.
        log.debug("Archive: expected blob not found (%s). Listing candidates...", e)
        blobs = _list_blobs_flat(cc, run_prefix)
        tifs = [b for b in blobs if b.lower().endswith(".tif")]
        raise RuntimeError(
            f"Expected archive file not found: {expected_blob}. Available tifs: {[Path(t).name for t in tifs]}"
        ) from e


def _m3_root_candidates() -> List[str]:
    """
    Determine which M3 roots to try.

    Override with:
      - FIRESTARR_M3_PREFIXES="firestarr,firestarr/firestarr"
    or:
      - FIRESTARR_M3_PREFIX="firestarr"
    """
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
    """
    m3 layout:
      <root>/m3_YYYYMMDDHHMM/YYYYMMDD/YYN_XXXXX.tif

    We:
      - choose latest m3 run folder for run_date (YYYYMMDD)
      - forecast_ymd = run_date + (horizon-1)
      - list all .tif under .../<forecast_ymd>/
    """
    cc = _container_client()
    run_ymd = _ymd(run_date)

    log.debug(
        "Discover m3: run_date=%s horizon=%s run_ymd=%s root_candidates=%s",
        run_date.isoformat(),
        horizon,
        run_ymd,
        _m3_root_candidates(),
    )

    chosen_run_prefix: Optional[str] = None

    # Find the run prefix for this date by looking at immediate children under each root.
    for root in _m3_root_candidates():
        # root itself contains m3_* directories
        prefixes = _list_child_prefixes(cc, root)
        m3_prefixes = [p for p in prefixes if "m3_" in p]
        run_prefix = _pick_latest_run_prefix_for_date(m3_prefixes, run_ymd)

        log.debug("M3: root=%s prefixes=%d m3_prefixes=%d picked=%s", root, len(prefixes), len(m3_prefixes), run_prefix)

        if run_prefix:
            chosen_run_prefix = run_prefix
            break

    if not chosen_run_prefix:
        log.debug("M3: no run prefix matched run_ymd=%s in any root", run_ymd)
        return None

    forecast_ymd = _ymd(run_date + timedelta(days=int(horizon) - 1))
    forecast_prefix = f"{chosen_run_prefix}{forecast_ymd}/"

    log.debug("M3: chosen_run_prefix=%s forecast_prefix=%s", chosen_run_prefix, forecast_prefix)

    blobs = _list_blobs_flat(cc, forecast_prefix)
    tifs = [b for b in blobs if b.lower().endswith(".tif")]

    log.debug("M3: found %d tif(s) under %s", len(tifs), forecast_prefix)
    return tifs or None


# -----------------------
# Download
# -----------------------
def _safe_name(blob_name: str) -> str:
    return Path(blob_name).name


def _download_one(cc: ContainerClient, blob_name: str, dest: Path) -> Path:
    """
    Download one blob to dest with simple retry.
    """
    tries, delay = 0, 1.0
    while True:
        tries += 1
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            bc = cc.get_blob_client(blob_name)
            stream = bc.download_blob()
            with open(dest, "wb") as f:
                # Stream download. SDK handles chunking internally.
                f.write(stream.readall())
            return dest
        except Exception as e:
            if tries >= 5:
                log.error("Download failed after %d tries: %s (%s)", tries, blob_name, e)
                raise
            log.warning("Download failed (try %d/5): %s (%s). Retrying in %.1fs", tries, blob_name, e, delay)
            time.sleep(delay)
            delay = min(delay * 2, 16.0)


def download_blobs(blob_names: Sequence[str]) -> List[Path]:
    """
    Download blobs (names inside container) into FIRESTARR_TMP (or IVA_TMP) temp dir.
    """
    cc = _container_client()
    tmp_root = Path(_env("FIRESTARR_TMP", _env("IVA_TMP", "/tmp")))
    tmpdir = Path(tempfile.mkdtemp(prefix="firestarr_", dir=str(tmp_root)))
    max_workers = int(_env("FIRESTARR_MAX_WORKERS", "10"))

    if log.isEnabledFor(logging.DEBUG):
        log.debug("Downloading %d blobs to tmpdir=%s", len(blob_names), tmpdir)
        log.debug("Blob sample=%s", _sample(list(blob_names), n=5))

    # Threaded download (simple). If you prefer async later, azure.storage.blob.aio exists. 
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
    """
    Rasterio compatibility:
      - Prefer dst.set_band_description(bidx, desc) when available. [1](https://gis.stackexchange.com/questions/284179/add-bands-name-and-description-to-the-metadata-when-stacking-using-rasterio)[2](https://stackoverflow.com/questions/66589458/rasterio-set-a-unique-name-for-each-band)
      - Otherwise try setting dst.descriptions tuple. [2](https://stackoverflow.com/questions/66589458/rasterio-set-a-unique-name-for-each-band)
      - If neither is supported, silently skip (description is nice-to-have).
    """
    if hasattr(dst, "set_band_description"):
        dst.set_band_description(bidx, desc)  # Rasterio >= 1.0 [1](https://gis.stackexchange.com/questions/284179/add-bands-name-and-description-to-the-metadata-when-stacking-using-rasterio)[2](https://stackoverflow.com/questions/66589458/rasterio-set-a-unique-name-for-each-band)
        return
    try:
        # Some builds allow direct assignment
        descs = list(getattr(dst, "descriptions", ()) or ())
        while len(descs) < bidx:
            descs.append(None)
        descs[bidx - 1] = desc
        dst.descriptions = tuple(descs)  # [2](https://stackoverflow.com/questions/66589458/rasterio-set-a-unique-name-for-each-band)
    except Exception:
        pass

# -----------------------
# Reproject + mosaic (unchanged behavior from your current approach)
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
    )
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(data, 1)
        _set_band_description_compat(dst, 1, "probability")
    return out_path


def mosaic_tiles_reproject_first(
    tile_paths: Sequence[Path],
    out_path: Path,
    *,
    dst_epsg: int = 3978,
    res_m: float = 100.0,
    method: str = "max",
) -> Path:
    """
    Reproject each tile via WarpedVRT (nearest, fixed resolution) and mosaic via rasterio.merge(method="max").
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

        mosaic, transform = merge(
            vrts,
            method=method,
            res=(res_m, res_m),
            nodata=nodata,
            target_aligned_pixels=True,
        )
        log.info("Reprojected tiles")
        
        out_path.parent.mkdir(parents=True, exist_ok=True)
        log.info("Writing mosaic to %s", out_path.parent)
        profile = vrts[0].profile.copy()
        profile.update(
            driver="GTiff",
            height=mosaic.shape[1],
            width=mosaic.shape[2],
            transform=transform,
            crs=dst_crs,
            count=1,
            nodata=nodata,
            compress="DEFLATE",
            tiled=True,
            blockxsize=256,
            blockysize=256,
        )
        log.info("Profile: %s", profile)
        with rasterio.open (out_path, "w", **profile) as dst:
            dst.write(mosaic, 1)
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


def reproject_single(src_path: Path, out_path: Path, *, dst_epsg: int = 3978, res_m: float = 100.0) -> Path:
    """
    Reproject a single already-mosaiced GeoTIFF to EPSG:dst_epsg at res_m using nearest.
    """
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
    """
    End-to-end:
      1) try archive exact file
      2) else try m3 tiles
      3) else raise
    """
    dst_epsg = int(_env("FIRESTARR_DST_EPSG", "3978"))
    res_m = float(_env("FIRESTARR_RES_M", "100.0"))

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    out_name = f"firestarr_{_ymd(run_date)}_day_{int(day):02d}_probability_epsg{dst_epsg}_{int(res_m)}m.tif"
    out_path = out_dir / out_name

    log.debug("get_firestarr_mosaic(run_date=%s day=%s) out=%s", run_date.isoformat(), day, out_path)

    # 1) archive
    archive_blob = _discover_archive_blob(run_date, int(day))
    if archive_blob:
        log.info("Using archive blob: %s", archive_blob)
        local = download_blobs([archive_blob])[0]
        return reproject_single(local, out_path, dst_epsg=dst_epsg, res_m=res_m)

    # 2) m3
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

    parser = argparse.ArgumentParser(description="Fetch FireSTARR and produce EPSG:3978 100m mosaic(s).")
    parser.add_argument("--run-date", type=str, default=date.today().isoformat(), help="Run date (YYYY-MM-DD).")
    parser.add_argument("--day", type=int, default=3, help="Forecast day horizon (e.g., 3).")
    parser.add_argument("--out-dir", type=str, default="./output", help="Output directory.")
    parser.add_argument("--also-day7", action="store_true", help="Also fetch day 7.")
    parser.add_argument(
        "--log-level",
        type=str,
        default=os.getenv("FIRESTARR_LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG, INFO, WARNING...). Overrides FIRESTARR_LOG_LEVEL.",
    )
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
