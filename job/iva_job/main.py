
"""IVA job runner for FireSTARR data processing.

Strict parity with justTheIntersection.R for mosaics and non-building feature sets:
  - MAX mosaic across overlaps
  - NEAREST resampling
  - EXACT 100 m snapped grid
  - EPSG:3978
  - Zonal extraction uses all_touched=True (terra touches=TRUE)

Performance:
  - By default, computes feature_stats only for feature sets used by justTheIntersection.R:
    ecumene, first_nations, highways, rail, facilities
  - Buildings are polygons and can be millions of features; per-feature stats are disabled by default.
    Use FEATURE_SET_CODES to include buildings only if you accept the runtime/storage cost.

Evacuation flagging:
  Optional. Provide EVAC_WFS_URL returning GeoJSON geometries in EPSG:4326.

FireSTARR discovery:
  Set FIRESTARR_DISCOVER_LATEST=1 to mimic the R list_blobs/tail/latest folder selection.
"""

from datetime import date, timedelta
from pathlib import Path
import os
import logging
import json
import requests
import numpy as np
import rasterio
from rasterio.mask import mask

from .db import connect_writer, insert_run, upsert_feature_stats
from .stats import summarize
from .firestarr import build_blob_urls, download_tiles, mosaic_to_grid

log = logging.getLogger('iva.main')
logging.basicConfig(level=logging.INFO)

PREFIX = os.getenv('FIRESTARR_PREFIX', 'canada')

GRID_ENV = os.getenv('FIRESTARR_GRID', '')
if GRID_ENV:
    GRID = [(int(a), int(b)) for a, b in (pair.split(',') for pair in GRID_ENV.split(';'))]
else:
    GRID = [(0, 0), (0, 1), (1, 0), (1, 1)]

FEATURE_SET_CODES = [c.strip() for c in os.getenv('FEATURE_SET_CODES', 'ecumene,first_nations,highways,rail,facilities').split(',') if c.strip()]


def _fetch_evac_buffers(cur, buffer_m: float = 2500.0):
    url = os.getenv('EVAC_WFS_URL', '').strip()
    if not url:
        return None

    log.info('Fetching evacuations from EVAC_WFS_URL')
    gj = requests.get(url, timeout=60).json()
    feats = gj.get('features', [])
    if not feats:
        return None

    cur.execute('DROP TABLE IF EXISTS pg_temp.evacs_buf')
    cur.execute('CREATE TEMP TABLE pg_temp.evacs_buf (geom geometry(Polygon,3978)) ON COMMIT DROP')

    for f in feats:
        geom = json.dumps(f.get('geometry'))
        if not geom or geom == 'null':
            continue
        cur.execute(
            """INSERT INTO pg_temp.evacs_buf(geom)
               SELECT ST_Buffer(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), 3978), %s)::geometry(Polygon,3978)""",
            (geom, buffer_m),
        )

    return 'pg_temp.evacs_buf'


def _iter_features(cur):
    cur.execute(
        """SELECT f.id, fs.code, ST_AsGeoJSON(ST_Transform(f.geom,3978))
           FROM risk.features f
           JOIN risk.feature_sets fs ON fs.id = f.feature_set_id
           WHERE fs.code = ANY(%s)""",
        (FEATURE_SET_CODES,),
    )
    for fid, code, g in cur.fetchall():
        yield fid, code, g


def _values_for_geom(ds, geom_obj):
    gtype = geom_obj.get('type')

    # For polygons/lines: crop with all_touched=True like terra::extract(... touches=TRUE)
    out, _ = mask(ds, [geom_obj], crop=True, all_touched=True, filled=False)
    band = out[0]
    vals = band.compressed() if hasattr(band, 'compressed') else np.asarray(band).ravel()

    vals = vals.astype('float64')
    nodata = ds.nodata
    if nodata is not None:
        vals = vals[vals != nodata]
    vals = vals[~np.isnan(vals)]
    return vals


def run_once(run_date: date):
    horizons = [3, 7]
    out_dir = Path(os.getenv('IVA_TMP', '/tmp'))

    with connect_writer() as conn:
        with conn.cursor() as cur:
            for h in horizons:
                wmstime = run_date + timedelta(days=h - 1)

                urls = build_blob_urls(run_date, h, PREFIX, GRID)
                tile_paths = download_tiles(urls)

                out_path = out_dir / f'firestarr_{run_date}_D{h}_3978_100m.tif'
                mosaic_to_grid(tile_paths, out_path, dst_epsg=3978, res_m=100.0, mosaic_method='max')

                run_id = insert_run(cur, run_date, h, wmstime, [str(u) for u in urls], res_m=100, srs=3978)

                evac_tbl = _fetch_evac_buffers(cur)

                with rasterio.open(out_path) as ds:
                    count = 0
                    for feature_id, code, geom_json in _iter_features(cur):
                        geom_obj = json.loads(geom_json)
                        vals = _values_for_geom(ds, geom_obj)
                        stats = summarize(vals)

                        evacuated = False
                        if evac_tbl is not None:
                            cur.execute(
                                """SELECT EXISTS (
                                      SELECT 1 FROM pg_temp.evacs_buf e
                                      WHERE ST_Intersects(ST_Transform(f.geom,3978), e.geom)
                                    )
                                    FROM risk.features f
                                    WHERE f.id = %s""",
                                (feature_id,),
                            )
                            evacuated = bool(cur.fetchone()[0])

                        upsert_feature_stats(cur, run_id, feature_id, stats, evacuated=evacuated)
                        count += 1

                    log.info(f'Upserted stats for {count} features (run_id={run_id}, horizon={h}, feature_sets={FEATURE_SET_CODES})')

                conn.commit()


if __name__ == '__main__':
    run_once(date.today())
