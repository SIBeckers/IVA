import os
import sys
import glob
import time
import math
import logging
from datetime import date, datetime
from typing import Optional, Dict, Any, Iterable, Tuple

import geopandas as gpd
import psycopg
from psycopg.types.json import Json
from psycopg import Binary

# Optional: only used if ECUMENE filtering is enabled
try:
    from shapely import wkb as shapely_wkb
    from shapely.prepared import prep as shapely_prep
except Exception:  # pragma: no cover
    shapely_wkb = None
    shapely_prep = None


EPSG = 3979

log = logging.getLogger("iva.loaders")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)

# ---- Global state for multiprocessing ECUMENE filter (set by pool initializer) ----
_ECUMENE_PREPARED = None  # prepared geometry for fast intersects
_ECUMENE_ENABLED = False


def _jsonable(v: Any) -> Any:
    """
    Make values safe for JSON encoding:
      - NaN -> None
      - numpy/pandas scalars -> python scalars (via .item())
      - dates/timestamps -> ISO8601 strings
    """
    if v is None:
        return None

    # NaN handling
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass

    # numpy/pandas scalar -> python scalar
    if hasattr(v, "item"):
        try:
            v = v.item()
        except Exception:
            pass

    # timestamps/dates -> ISO string
    if isinstance(v, (datetime, date)):
        return v.isoformat()

    return v


def _db_params_from_env() -> Dict[str, Any]:
    return dict(
        host=os.getenv("PGHOST", "postgis"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "impacted_values"),
        user=os.getenv("PGUSER", "iva_job"),
        password=os.getenv("PGPASSWORD", "changeme-job"),
        connect_timeout=int(os.getenv("PGCONNECT_TIMEOUT", "5")),
    )


def connect_with_retry(max_attempts: int = 10, backoff_sec: int = 2) -> psycopg.Connection:
    """Retry database connection with exponential backoff."""
    params = _db_params_from_env()
    for attempt in range(1, max_attempts + 1):
        try:
            log.info(f"connecting to database (attempt {attempt}/{max_attempts})")
            conn = psycopg.connect(**params)
            log.info("connected successfully")
            return conn
        except (psycopg.OperationalError, psycopg.Error) as e:
            if attempt < max_attempts:
                wait = backoff_sec * (2 ** (attempt - 1))
                log.warning(f"connection failed: {e}; retrying in {wait}s")
                time.sleep(wait)
            else:
                log.error(f"connection failed after {max_attempts} attempts")
                raise


def upsert_features(
    gdf: gpd.GeoDataFrame,
    conn: psycopg.Connection,
    set_code: str,
    pk_col: str,
    name_col: Optional[str] = None,
    commit_every: int = 5000,
) -> None:
    """
    Upsert features into risk.features for a given feature set code.
    - attrs written as JSON (Json wrapper).
    - geometry inserted via ST_GeomFromWKB with SRID set to EPSG.
    """
    if gdf is None or len(gdf) == 0:
        log.info(f"no features to upsert for set {set_code}")
        return

    gdf = gdf.copy()

    # Reproject to expected CRS
    if gdf.crs is None or gdf.crs.to_epsg() != EPSG:
        gdf = gdf.to_crs(EPSG)

    geom_col = gdf.geometry.name

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM risk.feature_sets WHERE code=%s", (set_code,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"feature set code not found in risk.feature_sets: {set_code}")
        set_id = row[0]

        total = len(gdf)
        log.info(f"upserting {total} features into set {set_code}")

        count = 0
        for _, r in gdf.iterrows():
            # Build attrs dict excluding pk/name/geom
            drop_cols = [pk_col, geom_col]
            if name_col:
                drop_cols.append(name_col)

            attrs = r.drop(labels=drop_cols, errors="ignore").to_dict()
            attrs = {k: _jsonable(v) for k, v in attrs.items()}

            # Extract values
            source_pk = str(r[pk_col]) if pk_col in r else str(r.iloc[0])
            name_val = (r[name_col] if (name_col and name_col in r) else None)

            geom = r[geom_col]
            if geom is None or geom.is_empty:
                # Skip empty geometries
                continue

            geom_wkb = geom.wkb  # bytes

            cur.execute(
                """
                INSERT INTO risk.features(feature_set_id, source_pk, name, attrs, geom)
                VALUES (%s, %s, %s, %s, ST_SetSRID(ST_GeomFromWKB(%s), %s))
                ON CONFLICT (feature_set_id, source_pk)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    attrs = EXCLUDED.attrs,
                    geom = EXCLUDED.geom
                """,
                (
                    set_id,
                    source_pk,
                    name_val,
                    Json(attrs),
                    Binary(geom_wkb),
                    EPSG,
                ),
            )

            count += 1
            if commit_every and count % commit_every == 0:
                conn.commit()
                log.info(f" processed {count}/{total}")

        conn.commit()
        log.info(f"completed upsert of {count} features into {set_code}")


def _pool_init_ecumene(ecumene_union_wkb: Optional[bytes], enable: bool) -> None:
    """
    Initializer for multiprocessing Pool:
    Loads and prepares ECUMENE union geometry once per worker.
    """
    global _ECUMENE_PREPARED, _ECUMENE_ENABLED

    _ECUMENE_ENABLED = bool(enable)
    _ECUMENE_PREPARED = None

    if not _ECUMENE_ENABLED:
        return

    if ecumene_union_wkb is None:
        log.warning("ECUMENE filter enabled, but no union WKB provided; disabling filter")
        _ECUMENE_ENABLED = False
        return

    if shapely_wkb is None or shapely_prep is None:
        log.warning("shapely wkb/prepared not available; disabling ECUMENE filter")
        _ECUMENE_ENABLED = False
        return

    try:
        union_geom = shapely_wkb.loads(ecumene_union_wkb)
        _ECUMENE_PREPARED = shapely_prep(union_geom)
        log.info("ECUMENE filter prepared in worker")
    except Exception as e:
        log.warning(f"failed to prepare ECUMENE filter in worker: {e}; disabling")
        _ECUMENE_ENABLED = False
        _ECUMENE_PREPARED = None


def _process_building(path: str) -> Tuple[str, int, int]:
    """
    Worker-safe building processor.
    Must be module top-level for multiprocessing pickling/importability.
    Returns (path, initial_count, inserted_count).
    """
    log.info(f"loading building file {path}")
    bld_gdf = gpd.read_file(path)
    initial_count = len(bld_gdf)

    if initial_count == 0:
        log.info(f" empty building file: {path}")
        return (path, 0, 0)

    # Reproject if needed (upsert_features also reprojects, but this helps filter correctness)
    if bld_gdf.crs is None or bld_gdf.crs.to_epsg() != EPSG:
        bld_gdf = bld_gdf.to_crs(EPSG)

    # Optional ECUMENE filter (exclude buildings intersecting ECUMENE)
    if _ECUMENE_ENABLED and _ECUMENE_PREPARED is not None:
        try:
            mask = bld_gdf.geometry.apply(lambda g: not _ECUMENE_PREPARED.intersects(g) if g is not None else False)
            bld_gdf = bld_gdf[mask]
            log.info(f" filtered {initial_count} -> {len(bld_gdf)} buildings (excluded {initial_count - len(bld_gdf)})")
        except Exception as e:
            log.warning(f"ECUMENE filter failed for {path}: {e}; proceeding without filter")

    if len(bld_gdf) == 0:
        log.info(f" no buildings remain after filtering: {path}")
        return (path, initial_count, 0)

    # Determine pk/name columns
    pk = "id" if "id" in bld_gdf.columns else bld_gdf.columns[0]
    nm = "name" if "name" in bld_gdf.columns else None

    # New connection per worker
    params = _db_params_from_env()
    with psycopg.connect(**params) as c2:
        upsert_features(
            bld_gdf,
            c2,
            "buildings",
            pk_col=pk,
            name_col=nm,
            commit_every=int(os.getenv("BUILDING_COMMIT_EVERY", "5000")),
        )

    return (path, initial_count, len(bld_gdf))


def _read_gpkg(data_dir: str, filename: str) -> gpd.GeoDataFrame:
    path = os.path.join(data_dir, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing required file: {path}")
    return gpd.read_file(path)


if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "./data"
    log.info(f"loading reference features from {data_dir}")

    # Base connection for non-building layers
    conn = connect_with_retry()

    # Adjust filenames/columns to your local data
    # Ecumene
    ec = _read_gpkg(data_dir, "ECUMENE_V3.gpkg")
    upsert_features(ec, conn, "ecumene", pk_col="OBJECTID_1", name_col="EcuName")

    # First Nations
    fn = _read_gpkg(data_dir, "FirstNations.gpkg")
    upsert_features(fn, conn, "first_nations", pk_col="ID", name_col="BAND_NAME")

    # Highways
    hw = _read_gpkg(data_dir, "highways_v2.gpkg")
    upsert_features(hw, conn, "highways", pk_col="ID", name_col="rtenum1")

    # Rail
    rl = _read_gpkg(data_dir, "railways_v2.gpkg")
    upsert_features(rl, conn, "rail", pk_col="ID", name_col="subnam1_en")

    # Facilities
    fc = _read_gpkg(data_dir, "facilities.gpkg")
    upsert_features(fc, conn, "facilities", pk_col="ID", name_col="Name")

    # Buildings
    blds = sorted(glob.glob(os.path.join(data_dir, "*_structures_en.gpkg")))
    if not blds:
        log.warning(f"no building files found matching *_structures_en.gpkg in {data_dir}")
        conn.close()
        sys.exit(0)

    workers = int(os.getenv("BUILDING_WORKERS", "0"))
    enable_ecumene_filter = os.getenv("BUILDING_FILTER_ECUMENE", "0").strip() in ("1", "true", "TRUE", "yes", "YES")

    ecumene_union_wkb = None
    if enable_ecumene_filter:
        log.info("preparing ECUMENE union for buildings filter")
        try:
            # shapely 2: unary_union attribute; geopandas sometimes provides unary_union;
            # keep compatibility: prefer unary_union, fallback to unary_all if present.
            if hasattr(ec, "unary_union"):
                union_geom = ec.unary_union
            else:
                union_geom = getattr(ec, "unary_all")  # may raise
            ecumene_union_wkb = union_geom.wkb
            log.info("ECUMENE union prepared")
        except Exception as e:
            log.warning(f"failed to prepare ECUMENE union; disabling filter: {e}")
            enable_ecumene_filter = False
            ecumene_union_wkb = None

    # Run building imports
    if workers and workers > 1:
        from multiprocessing import get_context

        log.info(f"importing {len(blds)} building files with {workers} workers")
        # Use default start method (often forkserver in some environments); function is top-level so it's OK.
        ctx = get_context()  # default
        with ctx.Pool(
            processes=workers,
            initializer=_pool_init_ecumene,
            initargs=(ecumene_union_wkb, enable_ecumene_filter),
        ) as pool:
            results = pool.map(_process_building, blds)

        inserted = sum(r[2] for r in results)
        log.info(f"completed buildings import: inserted {inserted} buildings from {len(blds)} files")
    else:
        # Sequential: still honor filter if enabled (init global state once)
        _pool_init_ecumene(ecumene_union_wkb, enable_ecumene_filter)
        inserted = 0
        for b in blds:
            _, _, ins = _process_building(b)
            inserted += ins
        log.info(f"completed buildings import: inserted {inserted} buildings from {len(blds)} files")

    conn.close()