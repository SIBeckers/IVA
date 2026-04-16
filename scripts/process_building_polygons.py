"""
Script to process and ingest building polygons from *_structures_en.gpkg files.
This is split from the main IVA workflow to allow raster-only operation in production.
"""

import sys
import os
import glob
import logging
from pathlib import Path

# Import relevant functions from original loaders.py
from iva_job.loaders import _process_building, _province_from_filename

log = logging.getLogger("iva.process_building_polygons")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "./data"
    blds = sorted(glob.glob(os.path.join(data_dir, "*_structures_en.gpkg")))
    if not blds:
        log.warning("no building files found matching *_structures_en.gpkg in %s", data_dir)
        sys.exit(0)

    for b in blds:
        path, initial_count, inserted = _process_building(b)
        log.info(f"Processed {path}: initial={initial_count}, inserted={inserted}")

log.info("Building polygon processing complete.")
