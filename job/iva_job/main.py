"""IVA job runner for FireSTARR data processing.

This module downloads FireSTARR tiles, creates mosaics, and computes
zonal statistics for risk features.
"""
from datetime import date, timedelta
from pathlib import Path
import os
import logging
import numpy as np
from .db import connect_writer, insert_run, upsert_feature_stats
from .stats import summarize
from .firestarr import build_blob_urls, download_tiles, mosaic_to_3979


log = logging.getLogger("iva.main")
logging.basicConfig(level=logging.INFO)

# Configure your FireSTARR layout in .env to avoid hard-coding here:
# FIRESTARR_BLOB_URL, AZURE_SAS_TOKEN (optional), TILE_TEMPLATE
# Example TILE_TEMPLATE: "{prefix}/{ymd}/D{horizon}/tile_{ix}_{iy}.tif"
# and provide a grid of tile indices you expect for Canada or a region.

PREFIX = os.getenv("FIRESTARR_PREFIX", "canada")
# Example grid; replace with the real index set you need
GRID = [(0, 0), (0, 1), (1, 0), (1, 1)]


def run_once(run_date: date):
    """Execute a single FireSTARR data processing pipeline run.

    This function orchestrates the complete workflow for a given run date:
    1. Downloads FireSTARR tiles for multiple forecast horizons
    2. Mosaics and reprojects tiles to EPSG:3979 (100m resolution)
    3. Stores run metadata and blob URIs in the database
    4. Computes and upserts zonal statistics for all features

    Args:
        run_date (date): The date for which to process FireSTARR data.
                        Used as the base date for tile selection and naming.

    Returns:
        None

    Side Effects:
        - Downloads tiles from blob storage
        - Creates temporary GeoTIFF files in /tmp directory
        - Inserts run records and feature statistics into the connected
          database
        - Commits changes to the database connection

    Note:
        Currently, zonal statistics are computed as empty summaries (no data).
        This is a placeholder for future implementation of actual spatial 
        analysis.

    Args:
        run_date (date): The date for which to process FireSTARR data.
    """
    horizons = [3, 7]
    with connect_writer() as conn:
        with conn.cursor() as cur:
            for h in horizons:
                wmstime = run_date + timedelta(days=h-1)

                # 1) Build + download FireSTARR tiles
                urls = build_blob_urls(run_date, h, PREFIX, GRID)
                tile_paths = download_tiles(urls)

                # 2) Mosaic + reproject to EPSG:3979 (100 m)
                out_path = Path(f"/tmp/firestarr_{run_date}_D{h}_3979.tif")
                mosaic_to_3979(tile_paths, out_path, dst_crs="EPSG:3979", res_m=100.0)

                # 3) Save run metadata (store blob URIs)
                run_id = insert_run(cur, run_date, h, wmstime, [str(u) for u in urls])

                # 4) TODO: compute zonal stats per feature using the mosaic
                # For now, write empty stats like before (no data)
                cur.execute('SELECT id FROM risk.features')
                for (feature_id,) in cur.fetchall():
                    stats = summarize(np.array([]))
                    upsert_feature_stats(cur, run_id, feature_id, stats, evacuated=False)

            conn.commit()


if __name__ == '__main__':
    run_once(date.today())
