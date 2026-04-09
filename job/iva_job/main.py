"""
IVA job runner for FireSTARR risk processing.

Key behaviors:
- Builds/gets FireSTARR mosaic rasters for requested horizons using get_firestarr_mosaic()
- Stores run metadata in risk.runs
- Computes zonal stats over features that INTERSECT the raster bounds (EPSG:3978)
- Sparse writes: only upsert risk.feature_stats rows when there is at least one valid pixel (n > 0)
  - To avoid stale rows on reruns, deletes existing stats for run_id when IVA_CLEAR_RUN_STATS=1 (default)
- Robust to:
  - features outside raster bounds -> filtered out in SQL
  - edge rounding / geometry_window oddities -> mask() ValueError handled
  - overlap with only nodata / NaN / masked -> treated as no valid pixels, skipped

Notes:
- rasterio.mask.mask(... crop=True ...) raises ValueError when shapes do not overlap raster. [1](https://gis.stackexchange.com/questions/450759/masking-in-rasterio-changes-the-values-the-output-file)
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlparse
import os
import json
import logging
import argparse
import requests
import numpy as np
import rasterio
from rasterio.mask import mask

from .db import connect_writer, insert_run, upsert_feature_stats
from .stats import summarize
from .firestarr import get_firestarr_mosaic
from . import firestarr as firestarr_mod

log = logging.getLogger("iva.main")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _parse_csv_env(name: str) -> list[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _parse_horizons(s: str) -> list[int]:
    parts = [p.strip() for p in (s or "").split(",") if p.strip()]
    if not parts:
        return [3, 7]
    out: list[int] = []
    for p in parts:
        try:
            out.append(int(p))
        except ValueError as e:
            raise ValueError(f"Invalid horizon '{p}'. Expected comma-separated integers, e.g. '3,7'.") from e
    return out


def _container_base_no_sas() -> str:
    u = os.getenv("FIRESTARR_BLOB_URL", "https://sawipsprodca.blob.core.windows.net/firestarr").strip()
    p = urlparse(u)
    return f"{p.scheme}://{p.netloc}{p.path}".rstrip("/")


def _unsigned_urls_from_blobs(blob_names: list[str]) -> list[str]:
    base = _container_base_no_sas()
    return [f"{base}/{name}" for name in blob_names]


def _discover_firestarr_blob_names(run_date: date, horizon: int) -> list[str]:
    b = firestarr_mod._discover_archive_blob(run_date, int(horizon))
    if b:
        return [b]
    bs = firestarr_mod._discover_m3_blobs(run_date, int(horizon))
    if bs:
        return list(bs)
    raise RuntimeError(f"No FireSTARR blobs found for run_date={run_date.isoformat()} horizon={horizon}")


def _resolve_feature_set_codes(cur) -> list[str]:
    """
    Decide which feature sets to process.
    Priority:
    1) FEATURE_SET_CODES allow-list (if provided)
    2) else: all feature sets minus IVA_EXCLUDE_FEATURE_SETS (default exclude buildings)
    """
    include = _parse_csv_env("FEATURE_SET_CODES")
    if include:
        return include

    exclude = set(_parse_csv_env("IVA_EXCLUDE_FEATURE_SETS") or ["buildings"])
    cur.execute("SELECT code FROM risk.feature_sets")
    all_codes = [r[0] for r in cur.fetchall()]
    return [c for c in all_codes if c not in exclude]


def _fetch_evac_buffers(cur, buffer_m: float = 2500.0):
    url = os.getenv("EVAC_WFS_URL", "").strip()
    if not url:
        return None

    log.info("Fetching evacuations from EVAC_WFS_URL")
    gj = requests.get(url, timeout=60).json()
    feats = gj.get("features", [])
    if not feats:
        return None

    cur.execute("DROP TABLE IF EXISTS pg_temp.evacs_buf")
    cur.execute("CREATE TEMP TABLE pg_temp.evacs_buf (geom geometry(Polygon,3978)) ON COMMIT DROP")

    for f in feats:
        geom = json.dumps(f.get("geometry"))
        if not geom or geom == "null":
            continue
        cur.execute(
            """
            INSERT INTO pg_temp.evacs_buf(geom)
            SELECT ST_Buffer(
                ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), 3978),
                %s
            )::geometry(Polygon,3978)
            """,
            (geom, buffer_m),
        )
    return "pg_temp.evacs_buf"


def _iter_features_in_bounds(cur, feature_set_codes: list[str], bounds_3978):
    """
    Yield (feature_id, set_code, geom_json) for features intersecting the raster bounds.
    This is a fast pre-filter to avoid rasterio.mask() ValueError for non-overlap. [1](https://gis.stackexchange.com/questions/450759/masking-in-rasterio-changes-the-values-the-output-file)
    """
    left, bottom, right, top = bounds_3978
    cur.execute(
        """
        SELECT
            f.id,
            fs.code,
            ST_AsGeoJSON(ST_Transform(f.geom, 3978))
        FROM risk.features f
        JOIN risk.feature_sets fs ON fs.id = f.feature_set_id
        WHERE fs.code = ANY(%s)
          AND ST_Intersects(
                ST_Transform(f.geom, 3978),
                ST_MakeEnvelope(%s, %s, %s, %s, 3978)
          )
        """,
        (feature_set_codes, left, bottom, right, top),
    )
    for fid, code, g in cur.fetchall():
        yield fid, code, g


def _values_for_geom(ds, geom_obj) -> np.ndarray:
    """
    Extract valid pixel values under geom_obj from dataset ds.
    Robust behavior:
      - If shape doesn't overlap raster (crop=True), rasterio raises ValueError; return empty. [1](https://gis.stackexchange.com/questions/450759/masking-in-rasterio-changes-the-values-the-output-file)
      - If overlap exists but all pixels masked/nodata/NaN, return empty.
    """
    try:
        out, _ = mask(
            ds,
            [geom_obj],
            crop=True,
            all_touched=True,
            filled=False,
            pad=True,        # helps edge rounding near bounds
            pad_width=0.5,
        )
    except ValueError:
        # "Input shapes do not overlap raster." when crop=True [1](https://gis.stackexchange.com/questions/450759/masking-in-rasterio-changes-the-values-the-output-file)
        return np.array([], dtype="float64")

    band = out[0]
    vals = band.compressed() if hasattr(band, "compressed") else np.asarray(band).ravel()
    if vals.size == 0:
        return np.array([], dtype="float64")

    vals = vals.astype("float64", copy=False)

    nodata = ds.nodata
    if nodata is not None:
        vals = vals[vals != nodata]
    vals = vals[~np.isnan(vals)]

    return vals


def run_once(run_date: date, horizons: list[int] | None = None) -> None:
    horizons = horizons or [3, 7]
    out_dir = Path(os.getenv("IVA_TMP", "/tmp"))

    # Sparse write controls
    sparse = os.getenv("IVA_SPARSE_STATS", "1").strip().lower() in ("1", "true", "yes")
    clear_run_stats = os.getenv("IVA_CLEAR_RUN_STATS", "1").strip().lower() in ("1", "true", "yes")
    commit_every = int(os.getenv("IVA_COMMIT_EVERY", "2000"))

    with connect_writer() as conn:
        with conn.cursor() as cur:
            feature_set_codes = _resolve_feature_set_codes(cur)
            log.info("Processing feature sets: %s", feature_set_codes)

            for h in horizons:
                wmstime = run_date + timedelta(days=h - 1)

                # 1) Discover lineage (blob names + unsigned URLs)
                blob_names = _discover_firestarr_blob_names(run_date, h)
                unsigned_urls = _unsigned_urls_from_blobs(blob_names)

                # 2) Produce mosaic GeoTIFF (EPSG:3978, 100 m)
                out_path = get_firestarr_mosaic(run_date, h, out_dir)

                # 3) Insert run metadata
                run_id = insert_run(
                    cur,
                    run_date,
                    h,
                    wmstime,
                    unsigned_urls,
                    res_m=100,
                    srs=3978,
                    blob_names=blob_names,
                )

                # Sparse rerun safety: remove any previous partial rows for this run_id
                if sparse and clear_run_stats:
                    cur.execute("DELETE FROM risk.feature_stats WHERE run_id = %s", (run_id,))
                    conn.commit()
                    log.info("Cleared existing stats for run_id=%s (sparse mode)", run_id)

                evac_tbl = _fetch_evac_buffers(cur)

                # 4) Zonal stats (only intersecting features)
                with rasterio.open(out_path) as ds:
                    if str(ds.crs) not in ("EPSG:3978", "EPSG:3978:"):
                        raise RuntimeError(f"Unexpected CRS in mosaic: {ds.crs}")

                    rb = ds.bounds
                    bounds_3978 = (rb.left, rb.bottom, rb.right, rb.top)
                    log.info(
                        "Raster bounds (EPSG:3978): left=%.2f bottom=%.2f right=%.2f top=%.2f",
                        rb.left, rb.bottom, rb.right, rb.top
                    )

                    processed = 0
                    wrote = 0
                    skipped_empty = 0

                    for feature_id, code, geom_json in _iter_features_in_bounds(cur, feature_set_codes, bounds_3978):
                        processed += 1
                        geom_obj = json.loads(geom_json)

                        vals = _values_for_geom(ds, geom_obj)
                        if vals.size == 0:
                            skipped_empty += 1
                            continue

                        stats = summarize(vals)
                        if sparse and int(stats.get("n") or 0) <= 0:
                            skipped_empty += 1
                            continue

                        evacuated = False
                        if evac_tbl is not None:
                            cur.execute(
                                """
                                SELECT EXISTS (
                                    SELECT 1
                                    FROM pg_temp.evacs_buf e
                                    JOIN risk.features f ON f.id = %s
                                    WHERE ST_Intersects(ST_Transform(f.geom,3978), e.geom)
                                )
                                """,
                                (feature_id,),
                            )
                            evacuated = bool(cur.fetchone()[0])

                        upsert_feature_stats(cur, run_id, feature_id, stats, evacuated=evacuated)
                        wrote += 1

                        if wrote % commit_every == 0:
                            conn.commit()
                            log.info(
                                "Progress run_id=%s horizon=%s: processed=%s wrote=%s skipped_empty=%s",
                                run_id, h, processed, wrote, skipped_empty
                            )

                    conn.commit()
                    log.info(
                        "Done run_id=%s horizon=%s: processed=%s wrote=%s skipped_empty=%s (sparse=%s)",
                        run_id, h, processed, wrote, skipped_empty, sparse
                    )


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="IVA job runner: FireSTARR acquisition + zonal stats.")
    p.add_argument(
        "--run-date",
        type=str,
        default=None,
        help="Run date in YYYY-MM-DD. Defaults to today if omitted.",
    )
    p.add_argument(
        "--horizons",
        type=str,
        default="3,7",
        help="Comma-separated forecast horizons (e.g. '3,7'). Default: '3,7'.",
    )
    return p


if __name__ == "__main__":
    args = _build_arg_parser().parse_args()
    rd = date.today() if not args.run_date else date.fromisoformat(args.run_date)
    horizons = _parse_horizons(args.horizons)
    run_once(rd, horizons=horizons)