from __future__ import annotations

import io
from typing import Iterable, Tuple

import psycopg
import pyarrow.csv as pacsv
from shapely import wkb
from shapely.geometry import mapping


def fetch_zones_arrow(
    conn: psycopg.Connection,
    feature_set_ids: list[int],
    dst_epsg: int = 3978,
    bounds_3978: tuple[float, float, float, float] | None = None,
) -> Iterable[Tuple[int, int, dict]]:
    """
    Valid Arrow-based zone fetch.

    Strategy:
    - PostgreSQL COPY -> CSV bytes
    - pyarrow.csv parses to columnar arrays
    - geometry is transferred as hex WKB
    - yield (feature_id, feature_set_id, geojson_mapping)

    bounds_3978:
      Optional (left, bottom, right, top) envelope used to pre-filter zones
      against the current FireSTARR raster extent.
    """

    if bounds_3978 is None:
        sql = f"""
        COPY (
            SELECT
                f.id AS feature_id,
                f.feature_set_id,
                encode(ST_AsBinary(ST_Transform(f.geom, {dst_epsg})), 'hex') AS geom_hex
            FROM risk.features f
            WHERE f.feature_set_id = ANY(%s)
        ) TO STDOUT WITH (FORMAT CSV)
        """
        params = (feature_set_ids,)
    else:
        left, bottom, right, top = bounds_3978
        sql = f"""
        COPY (
            SELECT
                f.id AS feature_id,
                f.feature_set_id,
                encode(ST_AsBinary(ST_Transform(f.geom, {dst_epsg})), 'hex') AS geom_hex
            FROM risk.features f
            WHERE f.feature_set_id = ANY(%s)
              AND ST_Intersects(
                    f.geom,
                    ST_MakeEnvelope(%s, %s, %s, %s, 3978)
                  )
        ) TO STDOUT WITH (FORMAT CSV)
        """
        params = (feature_set_ids, left, bottom, right, top)

    buf = io.BytesIO()
    with conn.cursor() as cur:
        with cur.copy(sql, params) as copy:
            for data in copy:
                buf.write(data)

    buf.seek(0)
    table = pacsv.read_csv(
        buf,
        read_options=pacsv.ReadOptions(
            column_names=["feature_id", "feature_set_id", "geom_hex"],
            block_size=16*1024*1024
        ),
        convert_options=pacsv.ConvertOptions(
            column_types={
                "feature_id": "int64",
                "feature_set_id": "int64",
                "geom_hex": "string",
            }
        ),
    )

    feature_ids = table["feature_id"].to_pylist()
    feature_set_ids_col = table["feature_set_id"].to_pylist()
    geom_hexes = table["geom_hex"].to_pylist()

    for fid, fsid, geom_hex in zip(feature_ids, feature_set_ids_col, geom_hexes):
        geom = wkb.loads(bytes.fromhex(geom_hex))
        yield int(fid), int(fsid), mapping(geom)
