from __future__ import annotations

import json
import logging
import math
import os
import time
from pathlib import Path
from typing import Any

import geopandas as gpd
import psycopg
import pyogrio

log = logging.getLogger("iva.loaders")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)

EPSG = 3978


REQUIRED_FEATURE_SET_CODES = {
    "ecumene",
    "first_nations",
    "highways",
    "rail",
    "facilities",
    "census",
}


def _loaded_feature_set_codes(conn: psycopg.Connection) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT fs.code
            FROM risk.features f
            JOIN risk.feature_sets fs
              ON fs.id = f.feature_set_id
            """
        )
        return {row[0] for row in cur.fetchall()}


def _zones_already_loaded(conn: psycopg.Connection) -> bool:
    loaded = _loaded_feature_set_codes(conn)
    return REQUIRED_FEATURE_SET_CODES.issubset(loaded)

# ---------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------

def _db_params_from_env() -> dict[str, Any]:
    return dict(
        host=os.getenv("PGHOST", "postgis"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "impacted_values"),
        user=os.getenv("PGUSER", "iva_job"),
        password=os.getenv("PGPASSWORD", "changeme-job"),
        connect_timeout=int(os.getenv("PGCONNECT_TIMEOUT", "5")),
    )


def connect_with_retry(max_attempts: int = 10, backoff_sec: int = 2) -> psycopg.Connection:
    params = _db_params_from_env()
    for attempt in range(1, max_attempts + 1):
        try:
            log.info("Connecting to database (attempt %d/%d)", attempt, max_attempts)
            conn = psycopg.connect(**params)
            log.info("Connected successfully")
            return conn
        except psycopg.OperationalError as e:
            if attempt == max_attempts:
                raise
            wait = backoff_sec * (2 ** (attempt - 1))
            log.warning("Connection failed (%s), retrying in %ss", e, wait)
            time.sleep(wait)


# ---------------------------------------------------------------------
# Generic value sanitization for JSON attrs
# ---------------------------------------------------------------------

def _jsonable(value: Any) -> Any:
    """
    Convert values into JSON-safe Python values.

    Rules:
    - NaN / NaT -> None
    - numpy/pandas scalar -> python scalar where possible
    - bytes -> utf-8 string if possible, else hex
    - everything else -> left as-is if JSON-native, else str()
    """
    if value is None:
        return None

    # numpy / pandas scalar -> native python
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass

    # pandas / numpy missing sentinels frequently stringify badly
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except Exception:
        pass

    # guard for NaT-like / pandas missing-ish stringified values
    try:
        sval = str(value)
        if sval in {"NaT", "nan", "NaN", "<NA>"}:
            return None
    except Exception:
        pass

    if isinstance(value, (str, int, float, bool, list, dict)):
        return value

    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return value.hex()

    return str(value)


def _attrs_from_row(gdf: gpd.GeoDataFrame, row) -> str:
    """
    Build a JSON-safe attrs payload for one GeoDataFrame row.
    Returns a JSON string with allow_nan=False so invalid values fail here,
    not inside PostgreSQL.
    """
    attrs: dict[str, Any] = {}
    geom_col = gdf.geometry.name

    for col in gdf.columns:
        if col == geom_col:
            continue
        attrs[col] = _jsonable(row[col])

    return json.dumps(attrs, allow_nan=False)


# ---------------------------------------------------------------------
# Source reading helpers
# ---------------------------------------------------------------------

def _first_present(columns: list[str], candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in columns}
    for c in candidates:
        if c.lower() in cols:
            return cols[c.lower()]
    return None


def _read_gpkg(path: Path) -> gpd.GeoDataFrame:
    layers = pyogrio.list_layers(path)
    if len(layers) == 0:
        raise RuntimeError(f"No layers found in {path}")

    layer_name = layers[0][0]
    gdf = gpd.read_file(path, layer=layer_name, engine="pyogrio", use_arrow=True)

    if gdf.crs is None:
        raise RuntimeError(f"{path} has no CRS")

    if gdf.crs.to_epsg() != EPSG:
        gdf = gdf.to_crs(EPSG)

    gdf = gdf[gdf.geometry.notnull()].copy()
    if gdf.empty:
        raise RuntimeError(f"{path} contains no non-null geometries")

    return gdf


# ---------------------------------------------------------------------
# Upserts
# ---------------------------------------------------------------------

def _upsert_census_table(conn: psycopg.Connection, gdf: gpd.GeoDataFrame) -> None:
    pk_col = _first_present(list(gdf.columns), ["CSDUID", "csduid"])
    name_col = _first_present(list(gdf.columns), ["CSDNAME", "name"])
    prname_col = _first_present(list(gdf.columns), ["PRNAME", "prname"])

    if not pk_col or not name_col:
        raise RuntimeError("Census source is missing CSDUID/CSDNAME columns")

    with conn.cursor() as cur:
        for _, row in gdf.iterrows():
            cur.execute(
                """
                INSERT INTO public.census_subdivisions_2025 (csduid, name, prname, geom)
                VALUES (%s, %s, %s, ST_Multi(ST_GeomFromWKB(%s, 3978)))
                ON CONFLICT (csduid)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    prname = EXCLUDED.prname,
                    geom = EXCLUDED.geom
                """,
                (
                    str(row[pk_col]),
                    None if row[name_col] is None else str(row[name_col]),
                    None if prname_col is None or row[prname_col] is None else str(row[prname_col]),
                    bytes(row.geometry.wkb),
                ),
            )
    conn.commit()


def _upsert_features(
    conn: psycopg.Connection,
    gdf: gpd.GeoDataFrame,
    feature_set_code: str,
    pk_candidates: list[str],
    name_candidates: list[str],
) -> None:
    pk_col = _first_present(list(gdf.columns), pk_candidates)
    name_col = _first_present(list(gdf.columns), name_candidates)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM risk.feature_sets WHERE code = %s",
            (feature_set_code,),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Unknown feature_set_code: {feature_set_code}")
        feature_set_id = row[0]

        for idx, rec in gdf.iterrows():
            source_pk = (
                str(rec[pk_col])
                if pk_col and rec[pk_col] is not None
                else f"{feature_set_code}:{idx}"
            )
            name = (
                str(rec[name_col])
                if name_col and rec[name_col] is not None
                else None
            )

            attrs_json = _attrs_from_row(gdf, rec)

            cur.execute(
                """
                INSERT INTO risk.features (feature_set_id, source_pk, name, attrs, geom)
                VALUES (%s, %s, %s, %s::jsonb, ST_Multi(ST_GeomFromWKB(%s, 3978)))
                ON CONFLICT (feature_set_id, source_pk)
                DO UPDATE SET
                    name  = EXCLUDED.name,
                    attrs = EXCLUDED.attrs,
                    geom  = EXCLUDED.geom
                """,
                (
                    feature_set_id,
                    source_pk,
                    name,
                    attrs_json,
                    bytes(rec.geometry.wkb),
                ),
            )
    conn.commit()


# ---------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------

def load_all(data_dir: str = "/data") -> None:
    conn = connect_with_retry()

    force_reload = os.getenv("FORCE_RELOAD_ZONES", "").strip().lower() in {"1", "true", "yes"}
    
    if not force_reload and _zones_already_loaded(conn):
        log.info("Zones already loaded; skipping iva-load-zones")
        conn.close()
        return

    data_dir = str(data_dir)

    try:
        sources = [
            ("ecumene", Path(data_dir) / "ECUMENE_V3.gpkg"),
            ("first_nations", Path(data_dir) / "FirstNations.gpkg"),
            ("highways", Path(data_dir) / "highways_v2.gpkg"),
            ("rail", Path(data_dir) / "railways_v2.gpkg"),
            ("facilities", Path(data_dir) / "facilities.gpkg"),
            ("census", Path(data_dir) / "lcsd000a25p_e.gpkg"),
        ]

        for code, path in sources:
            if not path.exists():
                raise FileNotFoundError(f"Missing source file: {path}")

            log.info("Loading %s from %s", code, path)
            gdf = _read_gpkg(path)

            if code == "census":
                _upsert_census_table(conn, gdf)
                _upsert_features(
                    conn,
                    gdf,
                    feature_set_code="census",
                    pk_candidates=["CSDUID", "csduid"],
                    name_candidates=["CSDNAME", "name"],
                )
            else:
                _upsert_features(
                    conn,
                    gdf,
                    feature_set_code=code,
                    pk_candidates=["SOURCE_PK", "OBJECTID", "ID", "id", "fid", "FID"],
                    name_candidates=["NAME", "name", "ENG_NAME", "FULLNAME", "OSM_NAME", "FIRST_NATION_NAME"],
                )
    finally:
        conn.close()
        log.info("Zone loading complete")


if __name__ == "__main__":
    import sys

    data_dir = sys.argv[1] if len(sys.argv) > 1 else "/data"
    load_all(data_dir)