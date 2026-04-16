import os
import sys
import glob
import time
import math
import logging
import shutil
import tempfile
import json
import yaml
from pathlib import Path
from datetime import date, datetime
from typing import Optional, Dict, Any, Tuple, List

import geopandas as gpd
import psycopg

EPSG = 3978
log = logging.getLogger("iva.loaders")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)

# Optional: only used if ECUMENE filtering is enabled
try:
    from shapely import wkb as shapely_wkb
    from shapely.prepared import prep as shapely_prep
except Exception:  # pragma: no cover
    shapely_wkb = None
    shapely_prep = None

# ---- Global state for multiprocessing ECUMENE filter (set by pool initializer) ----
_ECUMENE_PREPARED = None  # prepared geometry for fast intersects
_ECUMENE_ENABLED = False


# -----------------------
# Utilities
# -----------------------
def _jsonable(v: Any) -> Any:
    """
    Make values safe for JSON encoding:
    - NaN -> None
    - numpy/pandas scalars -> python scalars (via .item())
    - dates/timestamps -> ISO8601 strings
    """
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass
    if hasattr(v, "item"):
        try:
            v = v.item()
        except Exception:
            pass
    if isinstance(v, (datetime, date)):
        return v.isoformat()


def _should_ingest_buildings(config_path="config.yaml"):
    try:
        with open(os.path.join(os.path.dirname(__file__), config_path), "r") as f:
            config = yaml.safe_load(f)
        return config.get("buildings_ingest", False)
    except Exception:
        return False
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
            log.info("connecting to database (attempt %d/%d)", attempt, max_attempts)
            conn = psycopg.connect(**params)
            log.info("connected successfully")
            return conn
        except (psycopg.OperationalError, psycopg.Error) as e:
            if attempt < max_attempts:
                wait = backoff_sec * (2 ** (attempt - 1))
                log.warning("connection failed: %s; retrying in %ss", e, wait)
                time.sleep(wait)
            else:
                log.error("connection failed after %d attempts", max_attempts)
                raise


def _ensure_ingest_state_table(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS risk.ingest_state (
              dataset_key  text PRIMARY KEY,
              fingerprint  text NOT NULL,
              loaded_at    timestamptz NOT NULL DEFAULT now()
            )
            """
        )
    conn.commit()


def _fingerprint_file(path: str) -> str:
    """Fast fingerprint: file size + mtime."""
    st = os.stat(path)
    return f"{st.st_size}:{int(st.st_mtime)}"


def _force_reload_all() -> bool:
    return os.getenv("IVA_FORCE_RELOAD", "0").strip().lower() in ("1", "true", "yes")


def _force_datasets() -> set[str]:
    raw = os.getenv("IVA_FORCE_DATASETS", "").strip()
    if not raw:
        return set()
    return {x.strip() for x in raw.split(",") if x.strip()}


def _dataset_key(set_code: str, src_path: str) -> str:
    return f"{set_code}:{Path(src_path).name}"


def _already_ingested(conn: psycopg.Connection, dataset_key: str, fingerprint: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM risk.ingest_state WHERE dataset_key=%s AND fingerprint=%s",
            (dataset_key, fingerprint),
        )
        return cur.fetchone() is not None


def _mark_ingested(conn: psycopg.Connection, dataset_key: str, fingerprint: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO risk.ingest_state(dataset_key, fingerprint)
            VALUES (%s, %s)
            ON CONFLICT (dataset_key)
            DO UPDATE SET fingerprint=EXCLUDED.fingerprint, loaded_at=now()
            """,
            (dataset_key, fingerprint),
        )
    conn.commit()


def _copy_to_writable_tmp(src_path: str) -> str:
    """
    Your /data mount can be read-only; WAL-enabled GPKGs can fail in read-only dirs.
    Copy to /tmp first to avoid sqlite WAL issues. 
    """
    tmpdir = Path(tempfile.mkdtemp(prefix="iva_gpkg_"))
    dst = tmpdir / Path(src_path).name
    shutil.copy2(src_path, dst)
    return str(dst)


def _use_bulk() -> bool:
    """
    Default ON: bulk COPY + staging + set-based upsert.
    Turn off with IVA_BULK_LOAD=0 for debugging.
    """
    return os.getenv("IVA_BULK_LOAD", "1").strip().lower() in ("1", "true", "yes")


def _read_gpkg_fast(path: str, columns: Optional[List[str]] = None) -> gpd.GeoDataFrame:
    """
    Use geopandas/pyogrio engine with optional column selection and Arrow acceleration when available.
    GeoPandas forwards kwargs to pyogrio; use_arrow=True can speed reads. [1](https://dev.to/octasoft-ltd/running-podman-on-windows-with-wsl-a-practical-guide-4jl8)[2](https://learn.microsoft.com/en-us/windows/wsl/wsl-config)
    """
    kwargs = {"engine": "pyogrio"}
    if columns:
        kwargs["columns"] = columns
    # Arrow speeds up bulk reads when pyarrow is installed; harmless if not supported.
    kwargs["use_arrow"] = True
    return gpd.read_file(path, **kwargs)


# -----------------------
# Bulk load: COPY -> temp staging -> set-based UPSERT
# -----------------------
def _bulk_upsert_features(
    gdf: gpd.GeoDataFrame,
    conn: psycopg.Connection,
    set_code: str,
    pk_col: str,
    name_col: Optional[str] = None,
    attrs_allowlist: Optional[List[str]] = None,
    pk_prefix: Optional[str] = None,
) -> None:
    """
    Fast ingest path:
      1) COPY rows into pg_temp.features_stage
      2) Single INSERT..SELECT..ON CONFLICT into risk.features

    Geometry is loaded as hex WKB and reconstructed in SQL.

    attrs_allowlist:
      - if provided, only those columns will be kept in attrs jsonb
      - otherwise attrs includes all non-pk/non-name/non-geom columns
    """
    if gdf is None or len(gdf) == 0:
        log.info("no features to upsert for set %s", set_code)
        return

    gdf = gdf.copy()

    # Reproject to expected CRS
    if gdf.crs is None or gdf.crs.to_epsg() != EPSG:
        gdf = gdf.to_crs(EPSG)

    geom_col = gdf.geometry.name
    cols = list(gdf.columns)

    if pk_col not in cols:
        raise RuntimeError(f"pk_col '{pk_col}' not found in {set_code} columns={cols}")

    # Determine attribute columns
    exclude = {pk_col, geom_col}
    if name_col:
        exclude.add(name_col)

    if attrs_allowlist is not None:
        attrs_cols = [c for c in attrs_allowlist if c in cols and c not in exclude]
    else:
        attrs_cols = [c for c in cols if c not in exclude]

    col_to_idx = {c: i for i, c in enumerate(cols)}
    pk_idx = col_to_idx[pk_col]
    geom_idx = col_to_idx[geom_col]
    name_idx = col_to_idx.get(name_col) if name_col else None
    attrs_idx = [col_to_idx[c] for c in attrs_cols]

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM risk.feature_sets WHERE code=%s", (set_code,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"feature set code not found in risk.feature_sets: {set_code}")
        set_id = row[0]

        total = len(gdf)
        log.info("BULK upserting %d features into set %s (attrs=%s)", total, set_code, attrs_cols)

        cur.execute("DROP TABLE IF EXISTS pg_temp.features_stage")
        cur.execute(
            """
            CREATE TEMP TABLE pg_temp.features_stage (
              feature_set_id smallint NOT NULL,
              source_pk      text     NOT NULL,
              name           text,
              attrs          jsonb,
              geom_wkb_hex   text     NOT NULL
            ) ON COMMIT DROP
            """
        )

        with cur.copy(
            "COPY pg_temp.features_stage (feature_set_id, source_pk, name, attrs, geom_wkb_hex) FROM STDIN"
        ) as cp:
            for r in gdf.itertuples(index=False, name=None):
                geom = r[geom_idx]
                if geom is None or geom.is_empty:
                    continue

                raw_pk = r[pk_idx]
                if raw_pk is None:
                    continue
                source_pk = str(raw_pk)
                if pk_prefix:
                    source_pk = f"{pk_prefix}:{source_pk}"

                name_val = str(r[name_idx]) if (name_idx is not None and r[name_idx] is not None) else None

                if attrs_idx:
                    attrs = {}
                    for c, idx in zip(attrs_cols, attrs_idx):
                        attrs[c] = _jsonable(r[idx])
                    attrs_json = json.dumps(attrs, ensure_ascii=False, separators=(",", ":"))
                else:
                    attrs_json = None

                geom_hex = geom.wkb_hex if hasattr(geom, "wkb_hex") else geom.wkb.hex()

                cp.write_row([set_id, source_pk, name_val, attrs_json, geom_hex])

        cur.execute(
            f"""
            INSERT INTO risk.features(feature_set_id, source_pk, name, attrs, geom)
            SELECT
              feature_set_id,
              source_pk,
              name,
              attrs,
              ST_SetSRID(ST_GeomFromWKB(decode(geom_wkb_hex, 'hex')), {EPSG})::geometry(Geometry,{EPSG})
            FROM pg_temp.features_stage
            ON CONFLICT (feature_set_id, source_pk)
            DO UPDATE SET
              name = EXCLUDED.name,
              attrs = EXCLUDED.attrs,
              geom = EXCLUDED.geom
            """
        )

    conn.commit()
    log.info("BULK upsert complete for %s", set_code)


def upsert_features(
    gdf: gpd.GeoDataFrame,
    conn: psycopg.Connection,
    set_code: str,
    pk_col: str,
    name_col: Optional[str] = None,
    attrs_allowlist: Optional[List[str]] = None,
    pk_prefix: Optional[str] = None,
) -> None:
    if not _use_bulk():
        raise RuntimeError("Row-by-row upsert disabled in this build. Set IVA_BULK_LOAD=1 (default).")
    return _bulk_upsert_features(
        gdf,
        conn,
        set_code,
        pk_col=pk_col,
        name_col=name_col,
        attrs_allowlist=attrs_allowlist,
        pk_prefix=pk_prefix,
    )


# -----------------------
# ECUMENE filter (optional)
# -----------------------
def _pool_init_ecumene(ecumene_union_wkb: Optional[bytes], enable: bool) -> None:
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
        log.warning("failed to prepare ECUMENE filter in worker: %s; disabling", e)
        _ECUMENE_ENABLED = False
        _ECUMENE_PREPARED = None


# -----------------------
# Idempotent ingest wrappers
# -----------------------
def _maybe_ingest_layer(
    conn: psycopg.Connection,
    *,
    data_dir: str,
    filename: str,
    set_code: str,
    pk_col: str,
    name_col: Optional[str] = None,
    copy_to_tmp: bool = True,
) -> Optional[gpd.GeoDataFrame]:
    src_path = os.path.join(data_dir, filename)
    if not os.path.exists(src_path):
        raise FileNotFoundError(f"Missing required file: {src_path}")

    fp = _fingerprint_file(src_path)
    key = _dataset_key(set_code, src_path)

    force = _force_reload_all() or (set_code in _force_datasets()) or (key in _force_datasets())
    if not force and _already_ingested(conn, key, fp):
        log.info("SKIP %s (unchanged, fingerprint=%s)", key, fp)
        return None

    log.info("INGEST %s (fingerprint=%s, force=%s, bulk=%s)", key, fp, force, _use_bulk())

    safe_path = _copy_to_writable_tmp(src_path) if copy_to_tmp else src_path
    gdf = _read_gpkg_fast(safe_path)

    upsert_features(gdf, conn, set_code, pk_col=pk_col, name_col=name_col)

    _mark_ingested(conn, key, fp)
    return gdf


def _province_from_filename(path: str) -> str:
    """
    Try to infer province/territory code from filename like 'ab_structures_en.gpkg'.
    Fallback to stem.
    """
    stem = Path(path).stem.lower()
    # crude but effective: first token before underscore
    token = stem.split("_", 1)[0]
    return token.upper()


def _process_building(path: str) -> Tuple[str, int, int]:
    """
    Worker-safe building processor. Idempotent per file via ingest_state.
    Returns (path, initial_count, inserted_count_approx). Skipped -> (path, 0, 0).
    """
    params = _db_params_from_env()
    with psycopg.connect(**params) as c2:
        _ensure_ingest_state_table(c2)

        fp = _fingerprint_file(path)
        key = _dataset_key("buildings", path)

        force = _force_reload_all() or ("buildings" in _force_datasets()) or (key in _force_datasets())
        if not force and _already_ingested(c2, key, fp):
            log.info("SKIP %s (unchanged, fingerprint=%s)", key, fp)
            return (path, 0, 0)

        log.info("INGEST %s (fingerprint=%s, force=%s, bulk=%s)", key, fp, force, _use_bulk())

        safe_path = _copy_to_writable_tmp(path)

        # Buildings: read only needed columns (plus CS_ID used as PK)
        # GeoPandas forwards kwargs to pyogrio and supports use_arrow=True for speed. [1](https://dev.to/octasoft-ltd/running-podman-on-windows-with-wsl-a-practical-guide-4jl8)[2](https://learn.microsoft.com/en-us/windows/wsl/wsl-config)
        desired = ["CS_ID", "Province", "Area", "Height", "Perimeter"]
        bld_gdf = _read_gpkg_fast(safe_path, columns=[c for c in desired if c])

        initial_count = len(bld_gdf)
        if initial_count == 0:
            log.info(" empty building file: %s", path)
            _mark_ingested(c2, key, fp)
            return (path, 0, 0)

        if bld_gdf.crs is None or bld_gdf.crs.to_epsg() != EPSG:
            bld_gdf = bld_gdf.to_crs(EPSG)

        # Ensure Province exists; if not present in file, derive from filename
        if "Province" not in bld_gdf.columns:
            bld_gdf["Province"] = _province_from_filename(path)

        # Optional ECUMENE filter (exclude buildings intersecting ECUMENE)
        if _ECUMENE_ENABLED and _ECUMENE_PREPARED is not None:
            try:
                keep = bld_gdf.geometry.apply(lambda g: not _ECUMENE_PREPARED.intersects(g) if g is not None else False)
                bld_gdf = bld_gdf[keep]
                log.info(" filtered %d -> %d buildings", initial_count, len(bld_gdf))
            except Exception as e:
                log.warning("ECUMENE filter failed for %s: %s; proceeding without filter", path, e)

        if len(bld_gdf) == 0:
            log.info(" no buildings remain after filtering: %s", path)
            _mark_ingested(c2, key, fp)
            return (path, initial_count, 0)

        # Use CS_ID as source_pk (avoid cross-province collisions)
        pk = "CS_ID" if "CS_ID" in bld_gdf.columns else (bld_gdf.columns[0])
        prov_code = _province_from_filename(path)

        # Only keep a few attrs in JSONB
        building_attrs = ["Province", "Area", "Height", "Perimeter", "CS_ID"]

        upsert_features(
            bld_gdf,
            c2,
            "buildings",
            pk_col=pk,
            name_col=None,
            attrs_allowlist=building_attrs,
            pk_prefix=None if pk == "CS_ID" else prov_code,  # if no CS_ID, prefix fallback pk with province
        )

        _mark_ingested(c2, key, fp)
        return (path, initial_count, len(bld_gdf))


# -----------------------
# Main runner
# -----------------------
if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "./data"
    log.info("loading reference features from %s", data_dir)

    conn = connect_with_retry()
    _ensure_ingest_state_table(conn)

    # Non-building layers (idempotent)
    ec = _maybe_ingest_layer(
        conn,
        data_dir=data_dir,
        filename="ECUMENE_V3.gpkg",
        set_code="ecumene",
        pk_col="OBJECTID_1",
        name_col="EcuName",
        copy_to_tmp=True,
    )
    _maybe_ingest_layer(
        conn,
        data_dir=data_dir,
        filename="FirstNations.gpkg",
        set_code="first_nations",
        pk_col="ID",
        name_col="BAND_NAME",
        copy_to_tmp=True,
    )
    _maybe_ingest_layer(
        conn,
        data_dir=data_dir,
        filename="highways_v2.gpkg",
        set_code="highways",
        pk_col="ID",
        name_col="rtenum1",
        copy_to_tmp=True,
    )
    _maybe_ingest_layer(
        conn,
        data_dir=data_dir,
        filename="railways_v2.gpkg",
        set_code="rail",
        pk_col="ID",
        name_col="subnam1_en",
        copy_to_tmp=True,
    )
    _maybe_ingest_layer(
        conn,
        data_dir=data_dir,
        filename="facilities.gpkg",
        set_code="facilities",
        pk_col="ID",
        name_col="Name",
        copy_to_tmp=True,
    )

    # Buildings files (idempotent per file) - now optional
    if _should_ingest_buildings():
        log.info("Building polygon ingestion enabled via config.yaml")
        blds = sorted(glob.glob(os.path.join(data_dir, "*_structures_en.gpkg")))
        if not blds:
            log.warning("no building files found matching *_structures_en.gpkg in %s", data_dir)
            conn.close()
            sys.exit(0)

        import time
        try:
            import psutil
            _psutil = True
        except ImportError:
            _psutil = False
        workers = int(os.getenv("BUILDING_WORKERS", "2"))

        def _log_resource_usage(msg):
            if _psutil:
                p = psutil.Process(os.getpid())
                mem = p.memory_info().rss / (1024 * 1024)
                cpu = p.cpu_percent(interval=0.1)
                log.info(f"[RES] {msg} | mem={mem:.1f}MB cpu={cpu:.1f}%%")
            else:
                log.info(f"[RES] {msg} | os.times={os.times()}")
        enable_ecumene_filter = os.getenv("BUILDING_FILTER_ECUMENE", "0").strip().lower() in ("1", "true", "yes")

        # Prepare ecumene union if filter enabled
        ecumene_union_wkb = None
        if enable_ecumene_filter:
            log.info("preparing ECUMENE union for buildings filter")
            try:
                if ec is None:
                    ec_path = os.path.join(data_dir, "ECUMENE_V3.gpkg")
                    ec_safe = _copy_to_writable_tmp(ec_path)
                    ec = _read_gpkg_fast(ec_safe)
                if ec.crs is None or ec.crs.to_epsg() != EPSG:
                    ec = ec.to_crs(EPSG)

                union_geom = ec.unary_union if hasattr(ec, "unary_union") else getattr(ec, "unary_all")
                ecumene_union_wkb = union_geom.wkb
                log.info("ECUMENE union prepared")
            except Exception as e:
                log.warning("failed to prepare ECUMENE union; disabling filter: %s", e)
                enable_ecumene_filter = False
                ecumene_union_wkb = None

        # Run building imports
        if workers and workers > 1:
            from multiprocessing import get_context
            log.info("importing %d building files with %d workers", len(blds), workers)
            t0 = time.time()
            _log_resource_usage("START building import (parallel)")
            ctx = get_context()
            with ctx.Pool(
                processes=workers,
                initializer=_pool_init_ecumene,
                initargs=(ecumene_union_wkb, enable_ecumene_filter),
            ) as pool:
                results = pool.map(_process_building, blds)
                inserted = sum(r[2] for r in results)
                skipped = sum(1 for r in results if r[1] == 0 and r[2] == 0)
                log.info("completed buildings import: approx_inserted=%d files=%d skipped_files=%d (%.2fs)", inserted, len(blds), skipped, time.time()-t0)
                _log_resource_usage("END building import (parallel)")
        else:
            t0 = time.time()
            _log_resource_usage("START building import (serial)")
            _pool_init_ecumene(ecumene_union_wkb, enable_ecumene_filter)
            inserted = 0
            skipped = 0
            for b in blds:
                _, init_n, ins = _process_building(b)
                inserted += ins
                if init_n == 0 and ins == 0:
                    skipped += 1
            log.info("completed buildings import: approx_inserted=%d files=%d skipped_files=%d (%.2fs)", inserted, len(blds), skipped, time.time()-t0)
            _log_resource_usage("END building import (serial)")
    else:
        log.info("Building polygon ingestion disabled via config.yaml - skipping")

    conn.close()