import os
import psycopg
from psycopg.types.json import Json

def connect_writer():
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
    res_m=100,
    srs=3978,
    blob_names=None,
):
    """
    Insert/update a run row.

    Backward compatible behavior:
      - Always writes unsigned_urls into risk.runs.blob_uris (existing column). [1](https://041gc-my.sharepoint.com/personal/justin_beckers_nrcan-rncan_gc_ca/Documents/Microsoft%20Copilot%20Chat%20Files/db.py)

    Extended behavior:
      - If blob_names is provided, tries to store it in risk.runs.blob_names (json/jsonb).
      - If that column doesn't exist, falls back to storing both arrays in blob_uris as JSON object:
          {"unsigned_urls": [...], "blob_names": [...]}
    """
    blob_names = blob_names or []

    # First: do the normal upsert using blob_uris as a list of strings (unsigned URLs)
    cur.execute(
        """INSERT INTO risk.runs(run_date, forecast_day, wmstime, blob_uris, res_m, srs)
           VALUES (%s,%s,%s,%s,%s,%s)
           ON CONFLICT (run_date, forecast_day)
           DO UPDATE SET wmstime=EXCLUDED.wmstime,
                         blob_uris=EXCLUDED.blob_uris,
                         res_m=EXCLUDED.res_m,
                         srs=EXCLUDED.srs
           RETURNING id""",
        (run_date, forecast_day, wmstime, unsigned_urls, res_m, srs),
    )
    run_id = cur.fetchone()[0]

    if blob_names:
        # Try to store blob names in a dedicated column if present.
        try:
            cur.execute(
                """UPDATE risk.runs
                   SET blob_names = %s
                   WHERE id = %s""",
                (Json(blob_names), run_id),
            )
        except psycopg.Error:
            # Column likely doesn't exist. Fall back: store both lists together in blob_uris.
            payload = {"unsigned_urls": unsigned_urls, "blob_names": blob_names}
            cur.execute(
                """UPDATE risk.runs
                   SET blob_uris = %s
                   WHERE id = %s""",
                (Json(payload), run_id),
            )

    return run_id

def upsert_feature_stats(cur, run_id, feature_id, stats, evacuated=False):
    cur.execute(
        """INSERT INTO risk.feature_stats(run_id, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max, evacuated)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (run_id, feature_id)
           DO UPDATE SET n=EXCLUDED.n,
                         v_min=EXCLUDED.v_min,
                         p05=EXCLUDED.p05,
                         p25=EXCLUDED.p25,
                         p50=EXCLUDED.p50,
                         v_mean=EXCLUDED.v_mean,
                         p75=EXCLUDED.p75,
                         p95=EXCLUDED.p95,
                         v_max=EXCLUDED.v_max,
                         evacuated=EXCLUDED.evacuated""",
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