
import os
import psycopg

def connect_writer():
    return psycopg.connect(
        host=os.getenv('PGHOST','postgis'),
        port=os.getenv('PGPORT','5432'),
        dbname=os.getenv('PGDATABASE','impacted_values'),
        user=os.getenv('PGUSER','iva_job'),
        password=os.getenv('PGPASSWORD','changeme-job')
    )

def insert_run(cur, run_date, forecast_day, wmstime, blob_uris, res_m=100, srs=3979):
    cur.execute(
        """INSERT INTO risk.runs(run_date, forecast_day, wmstime, blob_uris, res_m, srs)
           VALUES (%s,%s,%s,%s,%s,%s)
           ON CONFLICT (run_date, forecast_day)
           DO UPDATE SET blob_uris=EXCLUDED.blob_uris
           RETURNING id""",
        (run_date, forecast_day, wmstime, blob_uris, res_m, srs)
    )
    return cur.fetchone()[0]

def upsert_feature_stats(cur, run_id, feature_id, stats, evacuated=False):
    cur.execute(
        """INSERT INTO risk.feature_stats(run_id, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max, evacuated)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (run_id, feature_id)
           DO UPDATE SET n=EXCLUDED.n, v_min=EXCLUDED.v_min, p05=EXCLUDED.p05, p25=EXCLUDED.p25,
                         p50=EXCLUDED.p50, v_mean=EXCLUDED.v_mean, p75=EXCLUDED.p75, p95=EXCLUDED.p95,
                         v_max=EXCLUDED.v_max, evacuated=EXCLUDED.evacuated""",
        (run_id, feature_id, stats.get('n'), stats.get('v_min'), stats.get('p05'), stats.get('p25'),
         stats.get('p50'), stats.get('v_mean'), stats.get('p75'), stats.get('p95'), stats.get('v_max'), evacuated)
    )
