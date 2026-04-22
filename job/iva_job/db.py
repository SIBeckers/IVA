from __future__ import annotations

import os
from typing import Any

import psycopg


def connect_writer() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("PGHOST", "postgis"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "impacted_values"),
        user=os.getenv("PGUSER", "iva_job"),
        password=os.getenv("PGPASSWORD", "changeme-job"),
    )


def insert_run(
    cur,
    run_date,
    forecast_day,
    wmstime,
    unsigned_urls,
    res_m: int = 100,
    srs: int = 3978,
    blob_names: Any = None,
) -> int:
    cur.execute(
        """
        INSERT INTO risk.runs (
            run_date, forecast_day, wmstime, srs, res_m, blob_uris, blob_names
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (run_date, forecast_day)
        DO UPDATE SET
            wmstime   = EXCLUDED.wmstime,
            srs       = EXCLUDED.srs,
            res_m     = EXCLUDED.res_m,
            blob_uris = EXCLUDED.blob_uris,
            blob_names = EXCLUDED.blob_names
        RETURNING id
        """,
        (run_date, forecast_day, wmstime, srs, res_m, unsigned_urls or [], blob_names),
    )
    return cur.fetchone()[0]


def clear_run_outputs(cur, run_id: int) -> None:
    cur.execute("DELETE FROM risk.building_zone_exposure WHERE run_id = %s", (run_id,))
    cur.execute("DELETE FROM risk.building_zone_stats WHERE run_id = %s", (run_id,))
    cur.execute("DELETE FROM risk.feature_stats WHERE run_id = %s", (run_id,))


def upsert_feature_stats(cur, run_id, feature_id, stats, evacuated: bool = False) -> None:
    cur.execute(
        """
        INSERT INTO risk.feature_stats (
            run_id, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max, evacuated
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id, feature_id)
        DO UPDATE SET
            n         = EXCLUDED.n,
            v_min     = EXCLUDED.v_min,
            p05       = EXCLUDED.p05,
            p25       = EXCLUDED.p25,
            p50       = EXCLUDED.p50,
            v_mean    = EXCLUDED.v_mean,
            p75       = EXCLUDED.p75,
            p95       = EXCLUDED.p95,
            v_max     = EXCLUDED.v_max,
            evacuated = EXCLUDED.evacuated
        """,
        (
            run_id,
            feature_id,
            stats.get("n"),
            stats.get("v_min"),
            stats.get("p05"),
            stats.get("p25"),
            stats.get("p50"),
            stats.get("v_mean"),
            stats.get("p75"),
            stats.get("p95"),
            stats.get("v_max"),
            evacuated,
        ),
    )


def upsert_building_zone_stats(
    cur,
    run_id: int,
    feature_set_id: int,
    feature_id: int,
    building_count: int,
) -> None:
    cur.execute(
        """
        INSERT INTO risk.building_zone_stats
            (run_id, feature_set_id, feature_id, building_count)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (run_id, feature_set_id, feature_id)
        DO UPDATE SET building_count = EXCLUDED.building_count
        """,
        (run_id, feature_set_id, feature_id, building_count),
    )


def upsert_building_zone_exposure(
    cur,
    run_id: int,
    feature_set_id: int,
    feature_id: int,
    expected_buildings: float,
) -> None:
    cur.execute(
        """
        INSERT INTO risk.building_zone_exposure
            (run_id, feature_set_id, feature_id, expected_buildings)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (run_id, feature_set_id, feature_id)
        DO UPDATE SET expected_buildings = EXCLUDED.expected_buildings
        """,
        (run_id, feature_set_id, feature_id, expected_buildings),
    )