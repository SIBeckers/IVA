"""
IVA job runner for FireSTARR risk processing.

**RASTER-FIRST PHASE 1**:
- Building features now use weighted raster (probability × count) instead of
  individual 2M building vectors
- Other features (ecumene, highways, rail, facilities) still use vector→raster zonal stats
- Next phase: convert remaining features to raster workflows

Key behaviors:
- Builds/gets FireSTARR mosaic rasters for requested horizons using get_firestarr_mosaic()
- Creates intermediate weighted raster: FireSTARR × building-count
- Stores run metadata in risk.runs
- Computes zonal stats:
  - Buildings: raster zonal stats over weighted raster (fast)
  - Other features: vector zonal stats over FireSTARR (unchanged for now)
- Sparse writes: only upsert risk.feature_stats rows when there is at least one valid pixel (n > 0)
  - To avoid stale rows on reruns, deletes existing stats for run_id when IVA_CLEAR_RUN_STATS=1 (default)
- Robust to:
  - features outside raster bounds → filtered out in SQL
  - edge rounding / geometry_window oddities → mask() ValueError handled
  - overlap with only nodata / NaN / masked → treated as no valid pixels, skipped
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
from collections import defaultdict
from .stats_raster import compute_building_counts

# Cache for raster intersections
raster_intersection_cache = defaultdict(list)

from .db import connect_writer, insert_run, upsert_feature_stats
from .stats import summarize
from .stats_raster import create_weighted_raster, zonal_stats_raster
from .firestarr import get_firestarr_mosaic
from . import firestarr as firestarr_mod
from .loaders import _should_ingest_buildings

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


def _zonal_stats_on_raster(cur, feature_set_codes: list[str], raster_path: Path, 
                           bounds_3978, run_id, commit_every, conn, feature_type: str = 'vector') -> tuple[int, int]:
    """
    Compute zonal statistics of raster over feature polygons.
    
    Args:
      feature_set_codes: list of codes to process (e.g. ['ecumene', 'first_nations'])
      raster_path: path to probability or multiplied raster
      bounds_3978: (left, bottom, right, top) bounds of raster in EPSG:3978
      feature_type: 'vector' (polygon features) or 'raster_polygon' (pre-computed zones)
    
    Returns: (processed_count, wrote_count) for stats rows
    """
    processed = 0
    wrote = 0
    
    with rasterio.open(raster_path) as ds:
        for feature_id, code, geom_json in _iter_features_in_bounds(cur, feature_set_codes, bounds_3978):
            processed += 1
            geom_obj = json.loads(geom_json)
            
            vals = _values_for_geom(ds, geom_obj)
            if vals.size == 0:
                continue
            
            stats = summarize(vals)
            if int(stats.get("n") or 0) <= 0:
                continue
            
            # Note: evacuated logic omitted for raster path initially;
            # can be reintroduced via spatial join if needed
            upsert_feature_stats(cur, run_id, feature_id, stats, evacuated=False)
            wrote += 1
            
            if wrote % commit_every == 0:
                conn.commit()
    
    return processed, wrote


def run_once(run_date: date, horizons: list[int] | None = None) -> None:
    import time
    try:
        import psutil
        _psutil = True
    except ImportError:
        _psutil = False
    horizons = horizons or [3, 7]
    out_dir = Path(os.getenv("IVA_TMP", "/tmp"))
    bld_count_path = Path(os.getenv("IVA_BUILDINGCOUNT_RASTER", "/data/IVA_buildingcount_100m.tif"))

    sparse = os.getenv("IVA_SPARSE_STATS", "1").strip().lower() in ("1", "true", "yes")
    clear_run_stats = os.getenv("IVA_CLEAR_RUN_STATS", "1").strip().lower() in ("1", "true", "yes")
    commit_every = int(os.getenv("IVA_COMMIT_EVERY", "2000"))

    # Control which feature sets use raster vs vector path
    raster_feature_sets = set(os.getenv("IVA_RASTER_FEATURE_SETS", "").strip().split(","))
    vector_feature_sets_only = set(os.getenv("IVA_VECTOR_FEATURE_SETS", "ecumene,first_nations,highways,rail,facilities").strip().split(","))

    # Remove building-specific raster multiplication logic
    bld_mult_path = None

    # Remove building-specific zonal stats logic
    if 'buildings' in raster_feature_sets:
        raster_feature_sets.remove('buildings')

    def _log_resource_usage(msg):
        if _psutil:
            p = psutil.Process(os.getpid())
            mem = p.memory_info().rss / (1024 * 1024)
            cpu = p.cpu_percent(interval=0.1)
            log.info(f"[RES] {msg} | mem={mem:.1f}MB cpu={cpu:.1f}%%")
        else:
            log.info(f"[RES] {msg} | os.times={os.times()}")

    t0 = time.time()
    _log_resource_usage("START run_once")
    with connect_writer() as conn:
        with conn.cursor() as cur:
            feature_set_codes = _resolve_feature_set_codes(cur)
            # Filter out buildings if disabled via config
            if _should_ingest_buildings() is False:
                feature_set_codes = [c for c in feature_set_codes if c != 'buildings']
            log.info("Processing feature sets: %s (raster=%s, vector=%s)", 
                    feature_set_codes, raster_feature_sets, vector_feature_sets_only)

            for h in horizons:
                t_h = time.time()
                _log_resource_usage(f"START horizon {h}")
                wmstime = run_date + timedelta(days=h - 1)

                # 1) Discover + download FireSTARR mosaic
                t1 = time.time()
                blob_names = _discover_firestarr_blob_names(run_date, h)
                unsigned_urls = _unsigned_urls_from_blobs(blob_names)
                out_path = get_firestarr_mosaic(run_date, h, out_dir)
                log.info(f"[TIMER] FireSTARR mosaic for horizon {h}: {time.time()-t1:.2f}s")
                _log_resource_usage(f"After FireSTARR mosaic {h}")

                # 2) If buildings in feature sets AND raster path enabled:
                #    Multiply probability × building-count raster
                bld_mult_path = None
                if 'buildings' in raster_feature_sets and 'buildings' in feature_set_codes:
                    try:
                        from .firestarr import load_building_count_raster, multiply_rasters
                        bld_mult_path = out_dir / f"firestarr_{date.today().isoformat()}_day_{int(h):02d}_x_buildings.tif"
                        t2 = time.time()
                        multiply_rasters(out_path, bld_count_path, bld_mult_path)
                        log.info("Created multiplied raster: %s (%.2fs)", bld_mult_path, time.time()-t2)
                    except Exception as e:
                        log.warning("Failed to create building-count multiplied raster; falling back to vector: %s", e)
                        bld_mult_path = None

                # 3) Insert run metadata
                run_id = insert_run(cur, run_date, h, wmstime, unsigned_urls, 
                                  res_m=100, srs=3978, blob_names=blob_names)

                if sparse and clear_run_stats:
                    cur.execute("DELETE FROM risk.feature_stats WHERE run_id = %s", (run_id,))
                    conn.commit()
                    log.info("Cleared existing stats for run_id=%s", run_id)

                # 4) Zonal stats: vector features on probability raster
                t3 = time.time()
                with rasterio.open(out_path) as ds:
                    rb = ds.bounds
                    bounds_3978 = (rb.left, rb.bottom, rb.right, rb.top)

                    vec_codes = [c for c in feature_set_codes if c not in raster_feature_sets]
                    if vec_codes:
                        log.info("Vector zonal stats: %s", vec_codes)
                        processed, wrote = _zonal_stats_on_raster(
                            cur, vec_codes, out_path, bounds_3978, run_id, commit_every, conn
                        )
                        conn.commit()
                        log.info("Vector stats done: processed=%s, wrote=%s (%.2fs)", processed, wrote, time.time()-t3)
                        _log_resource_usage(f"After vector zonal stats {h}")

                # 5) Zonal stats: buildings on multiplied raster (if available)
                if bld_mult_path:
                    log.info("Buildings raster zonal stats")
                    t4 = time.time()
                    with rasterio.open(bld_mult_path) as bld_mult_ds:
                        processed, wrote = _zonal_stats_on_raster(
                            cur, ['buildings'], bld_mult_path, bounds_3978, run_id, commit_every, conn
                        )
                        conn.commit()
                        log.info("Buildings raster stats done: processed=%s, wrote=%s (%.2fs)", processed, wrote, time.time()-t4)
                        _log_resource_usage(f"After buildings raster zonal stats {h}")

                # 6) Compute building counts using building count raster
                if bld_count_path.exists():
                    log.info("Computing building counts using building count raster")
                    t5 = time.time()
                    if not raster_intersection_cache:
                        log.info("Building raster intersection cache")
                        with rasterio.open(bld_count_path) as bld_ds:
                            for feature_id, code, geom_json in _iter_features_in_bounds(cur, ['ecumene', 'first_nations', 'census'], bounds_3978):
                                geom_obj = json.loads(geom_json)
                                raster_intersection_cache[feature_id].append(geom_obj)

                    zones = [{'feature_id': fid, 'geometry': geom} for fid, geoms in raster_intersection_cache.items() for geom in geoms]
                    compute_building_counts(cur, bld_count_path, zones, run_id)
                    conn.commit()
                    log.info("Building counts computed and committed for run_id=%s (%.2fs)", run_id, time.time()-t5)
                    _log_resource_usage(f"After building counts {h}")

                log.info("Completed horizon %s for run_id=%s (%.2fs)", h, run_id, time.time()-t_h)
                _log_resource_usage(f"END horizon {h}")
    log.info("run_once complete (%.2fs)", time.time()-t0)
    _log_resource_usage("END run_once")


# Move _build_arg_parser above __main__
def _build_arg_parser():
    parser = argparse.ArgumentParser(description="IVA FireSTARR risk processing job runner")
    parser.add_argument(
        "--date",
        dest="run_date",
        type=str,
        default=None,
        help="Run date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--horizons",
        type=str,
        default=None,
        help="Comma-separated list of forecast horizons (default: 3,7)",
    )
    return parser

if __name__ == "__main__":
    args = _build_arg_parser().parse_args()
    rd = date.today() if not args.run_date else date.fromisoformat(args.run_date)
    horizons = _parse_horizons(args.horizons)
    run_once(rd, horizons=horizons)