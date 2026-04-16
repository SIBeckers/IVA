#!/usr/bin/env python
"""
Clean CLI entrypoint for scheduled IVA job execution.

Usage:
  python -m iva_job.cli --run-date 2026-04-09 --horizons 3,7
  python -m iva_job.cli  # defaults to today, horizons 3,7

Environment controls:
  IVA_SPARSE_STATS=1           (default: skip features with n <= 0)
  IVA_CLEAR_RUN_STATS=1        (default: delete prior partial stats)
  IVA_RASTER_FEATURE_SETS      (default: buildings)
  PGHOST, PGPORT, PGUSER, etc. (PostgreSQL connection)
"""

import sys
import logging
from datetime import date
from pathlib import Path

from .main import run_once
from .loaders import connect_with_retry, _ensure_ingest_state_table

log = logging.getLogger("iva.cli")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="IVA job: ingest reference features, compute FireSTARR zonal stats."
    )
    parser.add_argument(
        "--run-date",
        type=str,
        default=None,
        help="Run date (YYYY-MM-DD). Default: today.",
    )
    parser.add_argument(
        "--horizons",
        type=str,
        default="3,7",
        help="Forecast horizons (e.g. '3,7'). Default: '3,7'.",
    )
    parser.add_argument(
        "--skip-loader",
        action="store_true",
        help="Skip reference feature ingestion (loaders.py). Use if already loaded.",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="/data",
        help="Path to data directory with GeoPackages. Default: /data",
    )
    
    args = parser.parse_args()
    
    rd = date.today() if not args.run_date else date.fromisoformat(args.run_date)
    horizons = [int(h.strip()) for h in args.horizons.split(",")]
    
    try:
        # 1) Ensure database is ready
        conn = connect_with_retry()
        _ensure_ingest_state_table(conn)
        conn.close()
        log.info("Database ready")
        
        # 2) Ingest reference features if not skipped
        if not args.skip_loader:
            log.info("Ingesting reference features from %s", args.data_dir)
            from .loaders import (
                _maybe_ingest_layer, _process_building, glob, os,
                _pool_init_ecumene, _force_reload_all
            )
            # (Re-use loaders.py __main__ logic; could refactor into standalone function)
            log.warning("--skip-loader not yet implemented; implement if needed")
        
        # 3) Run analysis
        log.info("Starting FireSTARR analysis for run_date=%s, horizons=%s", rd, horizons)
        run_once(rd, horizons=horizons)
        log.info("IVA job completed successfully")
        return 0
        
    except Exception as e:
        log.error("IVA job failed: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())