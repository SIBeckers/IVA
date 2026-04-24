from __future__ import annotations

import argparse
import logging
import math
import os
import time
from datetime import date
from pathlib import Path

import numpy as np
import rasterio
from rasterio.mask import mask

from iva_job.arrow_fetch import fetch_zones_arrow
from iva_job.db import (
    clear_run_outputs,
    connect_writer,
    insert_run,
    upsert_building_zone_exposure,
    upsert_building_zone_stats,
    upsert_feature_stats,
)
from iva_job.firestarr import get_firestarr_mosaic_info
from iva_job.stats import summarize
from iva_job.stats_raster import building_zone_metrics

log = logging.getLogger("iva.main")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)

# Only run building stats for zones that intersect at least one FireSTARR pixel
# in this inclusive range.
BUILDING_TRIGGER_MIN = 0.0
BUILDING_TRIGGER_MAX = 4.0


def _parse_horizons(s: str | None) -> list[int]:
    if not s:
        return [3, 7]
    out: list[int] = []
    for part in s.split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    return out


def _lookup_feature_set_ids(conn, feature_set_codes: list[str]) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT code, id FROM risk.feature_sets WHERE code = ANY(%s)",
            (feature_set_codes,),
        )
        rows = cur.fetchall()
    mapping = dict(rows)
    missing = set(feature_set_codes) - set(mapping)
    if missing:
        raise RuntimeError(f"Unknown feature_set_codes: {sorted(missing)}")
    return mapping


def _values_for_geom(ds, geom_obj) -> np.ndarray:
    try:
        arr, _ = mask(
            ds,
            [geom_obj],
            crop=True,
            all_touched=True,
            filled=False,
        )
    except ValueError:
        return np.array([], dtype="float64")

    band = np.ma.asarray(arr[0], dtype="float64")
    if np.ma.isMaskedArray(band):
        return band.compressed()
    return band[~np.isnan(band)]


def run_once(run_date: date, horizons: list[int]) -> None:
    conn = connect_writer()

    try:
        feature_set_codes = os.getenv(
            "FEATURE_SET_CODES",
            "ecumene,first_nations,highways,rail,facilities,census",
        ).split(",")
        feature_set_id_map = _lookup_feature_set_ids(conn, feature_set_codes)
        feature_set_ids = list(feature_set_id_map.values())

        bld_path = Path(
            os.getenv(
                "IVA_BUILDINGCOUNT_RASTER",
                "/data/IVA_buildingcount_100m_epsg3978.tif",
            )
        )
        if not bld_path.exists():
            raise RuntimeError(f"Building count raster not found: {bld_path}")

        tmp_dir = Path(os.getenv("IVA_TMP", "/tmp"))

        for horizon in horizons:
            log.info("Processing run_date=%s horizon=%s", run_date, horizon)

            fire_path, fire_meta = get_firestarr_mosaic_info(
                run_date=run_date,
                day=horizon,
                out_dir=tmp_dir,
            )

            with conn.cursor() as cur:
                run_id = insert_run(
                    cur,
                    run_date=run_date,
                    forecast_day=horizon,
                    forecast_for_date=fire_meta.forecast_for_date,
                    wmstime=fire_meta.run_ts.date(),  # coarse compatibility field
                    firestarr_source_kind=fire_meta.source_kind,
                    firestarr_run_token=fire_meta.run_token,
                    firestarr_run_ts=fire_meta.run_ts,
                    firestarr_run_prefix=fire_meta.run_prefix,
                    unsigned_urls=[],
                    blob_names=fire_meta.blob_names,
                )
                clear_run_outputs(cur, run_id)
            conn.commit()

            with rasterio.open(fire_path) as fire_ds:
                bounds = (
                    fire_ds.bounds.left,
                    fire_ds.bounds.bottom,
                    fire_ds.bounds.right,
                    fire_ds.bounds.top,
                )
                t_fetch0 = time.perf_counter()
                zones = list(
                    fetch_zones_arrow(
                        conn,
                        feature_set_ids=feature_set_ids,
                        dst_epsg=fire_ds.crs.to_epsg(),
                        bounds_3978=bounds,
                    )
                )
                t_fetch = time.perf_counter() - t_fetch0
                log.info(
                    "Fetched %d zones for horizon %s in %.2fs",
                    len(zones),
                    horizon,
                    t_fetch,
                )

            # --------------------------------------------------------------
            # Vector zonal stats
            # --------------------------------------------------------------
            with rasterio.open(fire_path) as fire_ds:
                log.info("Computing vector zonal stats")
                t_vec0 = time.perf_counter()
                vec_rows_written = 0
                building_candidate_zones: list[tuple[int, int, object]] = []

                with conn.cursor() as cur:
                    for feature_id, feature_set_id, geom in zones:
                        values = _values_for_geom(fire_ds, geom)
                        stats = summarize(values)
                        if stats["n"] == 0:
                            continue

                        upsert_feature_stats(cur, run_id, feature_id, stats)
                        vec_rows_written += 1

                        if np.any(
                            (values >= BUILDING_TRIGGER_MIN)
                            & (values <= BUILDING_TRIGGER_MAX)
                        ):
                            building_candidate_zones.append(
                                (feature_id, feature_set_id, geom)
                            )

                conn.commit()
                t_vec = time.perf_counter() - t_vec0
                log.info(
                    "Vector zonal stats complete: horizon=%s zones=%d rows_written=%d "
                    "building_candidates=%d elapsed=%.2fs",
                    horizon,
                    len(zones),
                    vec_rows_written,
                    len(building_candidate_zones),
                    t_vec,
                )

            # --------------------------------------------------------------
            # Building count + exposure
            # --------------------------------------------------------------
            with rasterio.open(fire_path) as fire_ds, rasterio.open(bld_path) as bld_ds:
                log.info("FireSTARR CRS: %s", fire_ds.crs)
                log.info("Building CRS: %s", bld_ds.crs)
                log.info("FireSTARR res: %s", fire_ds.res)
                log.info("Building res: %s", bld_ds.res)
                log.info("FireSTARR transform: %s", fire_ds.transform)
                log.info("Building transform: %s", bld_ds.transform)

                if fire_ds.crs != bld_ds.crs:
                    raise RuntimeError(
                        f"FireSTARR and building raster CRS mismatch: "
                        f"fire={fire_ds.crs}, building={bld_ds.crs}"
                    )

                fx, fy = fire_ds.res
                bx, by = bld_ds.res
                if not (
                    math.isclose(abs(fx), abs(bx), rel_tol=0.0, abs_tol=1e-6)
                    and math.isclose(abs(fy), abs(by), rel_tol=0.0, abs_tol=1e-6)
                ):
                    raise RuntimeError(
                        f"FireSTARR and building raster resolution mismatch: "
                        f"fire={fire_ds.res}, building={bld_ds.res}"
                    )

                def aligned_mod(a: float, b: float, res: float, tol: float = 1e-6) -> bool:
                    d = abs(a - b) % res
                    return math.isclose(d, 0.0, abs_tol=tol) or math.isclose(
                        d, res, abs_tol=tol
                    )

                fire_left = fire_ds.transform.c
                fire_top = fire_ds.transform.f
                bld_left = bld_ds.transform.c
                bld_top = bld_ds.transform.f

                if not (
                    aligned_mod(fire_left, bld_left, abs(fx))
                    and aligned_mod(fire_top, bld_top, abs(fy))
                ):
                    raise RuntimeError(
                        f"FireSTARR and building raster grid misalignment: "
                        f"fire origin=({fire_left}, {fire_top}), "
                        f"building origin=({bld_left}, {bld_top})"
                    )

                log.info(
                    "Computing building count and exposure for %d candidate zones "
                    "(from %d total zones)",
                    len(building_candidate_zones),
                    len(zones),
                )

                t_bld0 = time.perf_counter()
                bld_rows_written = 0

                with conn.cursor() as cur:
                    for feature_id, feature_set_id, geom in building_candidate_zones:
                        try:
                            count, expected = building_zone_metrics(fire_ds, bld_ds, geom)
                        except Exception as e:
                            raise RuntimeError(
                                f"building_zone_metrics failed for run_id={run_id}, "
                                f"feature_set_id={feature_set_id}, feature_id={feature_id}"
                            ) from e

                        if count == 0 and expected == 0.0:
                            continue

                        upsert_building_zone_stats(
                            cur, run_id, feature_set_id, feature_id, count
                        )
                        upsert_building_zone_exposure(
                            cur, run_id, feature_set_id, feature_id, expected
                        )
                        bld_rows_written += 1

                conn.commit()
                t_bld = time.perf_counter() - t_bld0
                log.info(
                    "Building count/exposure complete: horizon=%s candidate_zones=%d "
                    "rows_written=%d elapsed=%.2fs",
                    horizon,
                    len(building_candidate_zones),
                    bld_rows_written,
                    t_bld,
                )

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            COUNT(*) AS zones,
                            COALESCE(SUM(bz.building_count), 0) AS total_buildings,
                            COALESCE(SUM(be.expected_buildings), 0.0) AS total_expected
                        FROM risk.building_zone_stats bz
                        JOIN risk.building_zone_exposure be
                          ON bz.run_id = be.run_id
                         AND bz.feature_id = be.feature_id
                         AND bz.feature_set_id = be.feature_set_id
                        WHERE bz.run_id = %s
                        """,
                        (run_id,),
                    )
                    zones_written, total_buildings, total_expected = cur.fetchone()

                log.info(
                    "Validation run_id=%s zones=%s total_buildings=%s total_expected=%.3f",
                    run_id,
                    zones_written,
                    total_buildings,
                    total_expected,
                )

                if len(building_candidate_zones) == 0:
                    log.info(
                        "No building candidate zones for run_id=%s; skipping empty-output error",
                        run_id,
                    )
                elif zones_written == 0:
                    raise RuntimeError(
                        f"No building zone outputs were written for run_id={run_id} "
                        f"despite {len(building_candidate_zones)} candidate zones"
                    )

            log.info(
                "Completed horizon %s (firestarr_source_kind=%s run_token=%s forecast_for_date=%s)",
                horizon,
                fire_meta.source_kind,
                fire_meta.run_token,
                fire_meta.forecast_for_date,
            )

    finally:
        conn.close()


def _build_arg_parser():
    p = argparse.ArgumentParser("iva_job")
    p.add_argument("--date", type=str, default=None)
    p.add_argument("--horizons", type=str, default=None)
    return p


if __name__ == "__main__":
    args = _build_arg_parser().parse_args()
    rd = date.today() if not args.date else date.fromisoformat(args.date)
    horizons = _parse_horizons(args.horizons)
    run_once(rd, horizons)