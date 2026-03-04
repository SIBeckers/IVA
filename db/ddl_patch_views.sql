
-- db/ddl_patch_views.sql
-- Latest-per-horizon-per-theme views + is_new flag.

CREATE OR REPLACE VIEW risk.v_prev_runs AS
SELECT id, run_date, forecast_day
FROM (
  SELECT r.*, ROW_NUMBER() OVER (PARTITION BY forecast_day ORDER BY run_date DESC, id DESC) AS rn
  FROM risk.runs r
) q
WHERE rn = 2;

CREATE OR REPLACE VIEW risk.v_latest_feature_stats_with_prev AS
SELECT
  cur.run_date,
  cur.forecast_day,
  cur.feature_id,
  cur.n, cur.v_min, cur.p05, cur.p25, cur.p50, cur.v_mean, cur.p75, cur.p95, cur.v_max,
  cur.evacuated,
  cur.feature_set_id,
  cur.name,
  cur.attrs,
  cur.geom,
  (COALESCE(cur.n,0) > 0 AND COALESCE(prev.n,0) = 0) AS is_new
FROM risk.v_latest_feature_stats cur
LEFT JOIN risk.v_prev_runs pr
  ON pr.forecast_day = cur.forecast_day
LEFT JOIN risk.feature_stats prev
  ON prev.run_id = pr.id AND prev.feature_id = cur.feature_id;

-- Ecumene
CREATE OR REPLACE VIEW risk.v_latest_ecumene_d3 AS
SELECT run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max,
       evacuated, is_new, feature_set_id, name, attrs,
       ST_Multi(ST_Transform(geom,3978))::geometry(MultiPolygon,3978) AS geom
FROM risk.v_latest_feature_stats_with_prev
WHERE forecast_day=3 AND feature_set_id=(SELECT id FROM risk.feature_sets WHERE code='ecumene');

CREATE OR REPLACE VIEW risk.v_latest_ecumene_d7 AS
SELECT run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max,
       evacuated, is_new, feature_set_id, name, attrs,
       ST_Multi(ST_Transform(geom,3978))::geometry(MultiPolygon,3978) AS geom
FROM risk.v_latest_feature_stats_with_prev
WHERE forecast_day=7 AND feature_set_id=(SELECT id FROM risk.feature_sets WHERE code='ecumene');

-- First Nations
CREATE OR REPLACE VIEW risk.v_latest_first_nations_d3 AS
SELECT run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max,
       evacuated, is_new, feature_set_id, name, attrs,
       ST_Multi(ST_Transform(geom,3978))::geometry(MultiPolygon,3978) AS geom
FROM risk.v_latest_feature_stats_with_prev
WHERE forecast_day=3 AND feature_set_id=(SELECT id FROM risk.feature_sets WHERE code='first_nations');

CREATE OR REPLACE VIEW risk.v_latest_first_nations_d7 AS
SELECT run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max,
       evacuated, is_new, feature_set_id, name, attrs,
       ST_Multi(ST_Transform(geom,3978))::geometry(MultiPolygon,3978) AS geom
FROM risk.v_latest_feature_stats_with_prev
WHERE forecast_day=7 AND feature_set_id=(SELECT id FROM risk.feature_sets WHERE code='first_nations');

-- Facilities
CREATE OR REPLACE VIEW risk.v_latest_facilities_d3 AS
SELECT run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max,
       evacuated, is_new, feature_set_id, name, attrs,
       ST_Multi(ST_Transform(geom,3978))::geometry(MultiPolygon,3978) AS geom
FROM risk.v_latest_feature_stats_with_prev
WHERE forecast_day=3 AND feature_set_id=(SELECT id FROM risk.feature_sets WHERE code='facilities');

CREATE OR REPLACE VIEW risk.v_latest_facilities_d7 AS
SELECT run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max,
       evacuated, is_new, feature_set_id, name, attrs,
       ST_Multi(ST_Transform(geom,3978))::geometry(MultiPolygon,3978) AS geom
FROM risk.v_latest_feature_stats_with_prev
WHERE forecast_day=7 AND feature_set_id=(SELECT id FROM risk.feature_sets WHERE code='facilities');

-- Highways
CREATE OR REPLACE VIEW risk.v_latest_highways_d3 AS
SELECT run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max,
       evacuated, is_new, feature_set_id, name, attrs,
       ST_Multi(ST_Transform(geom,3978))::geometry(MultiLineString,3978) AS geom
FROM risk.v_latest_feature_stats_with_prev
WHERE forecast_day=3 AND feature_set_id=(SELECT id FROM risk.feature_sets WHERE code='highways');

CREATE OR REPLACE VIEW risk.v_latest_highways_d7 AS
SELECT run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max,
       evacuated, is_new, feature_set_id, name, attrs,
       ST_Multi(ST_Transform(geom,3978))::geometry(MultiLineString,3978) AS geom
FROM risk.v_latest_feature_stats_with_prev
WHERE forecast_day=7 AND feature_set_id=(SELECT id FROM risk.feature_sets WHERE code='highways');

-- Rail
CREATE OR REPLACE VIEW risk.v_latest_rail_d3 AS
SELECT run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max,
       evacuated, is_new, feature_set_id, name, attrs,
       ST_Multi(ST_Transform(geom,3978))::geometry(MultiLineString,3978) AS geom
FROM risk.v_latest_feature_stats_with_prev
WHERE forecast_day=3 AND feature_set_id=(SELECT id FROM risk.feature_sets WHERE code='rail');

CREATE OR REPLACE VIEW risk.v_latest_rail_d7 AS
SELECT run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max,
       evacuated, is_new, feature_set_id, name, attrs,
       ST_Multi(ST_Transform(geom,3978))::geometry(MultiLineString,3978) AS geom
FROM risk.v_latest_feature_stats_with_prev
WHERE forecast_day=7 AND feature_set_id=(SELECT id FROM risk.feature_sets WHERE code='rail');

-- Buildings (raw layer available separately; d3/d7 provide identical structure)
CREATE OR REPLACE VIEW risk.v_latest_buildings_d3 AS
SELECT run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max,
       evacuated, is_new, feature_set_id, name, attrs,
       ST_Multi(ST_Transform(geom,3978))::geometry(MultiPolygon,3978) AS geom
FROM risk.v_latest_feature_stats_with_prev
WHERE forecast_day=3 AND feature_set_id=(SELECT id FROM risk.feature_sets WHERE code='buildings');

CREATE OR REPLACE VIEW risk.v_latest_buildings_d7 AS
SELECT run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max,
       evacuated, is_new, feature_set_id, name, attrs,
       ST_Multi(ST_Transform(geom,3978))::geometry(MultiPolygon,3978) AS geom
FROM risk.v_latest_feature_stats_with_prev
WHERE forecast_day=7 AND feature_set_id=(SELECT id FROM risk.feature_sets WHERE code='buildings');

GRANT SELECT ON risk.v_prev_runs,
  risk.v_latest_feature_stats_with_prev,
  risk.v_latest_ecumene_d3, risk.v_latest_ecumene_d7,
  risk.v_latest_first_nations_d3, risk.v_latest_first_nations_d7,
  risk.v_latest_facilities_d3, risk.v_latest_facilities_d7,
  risk.v_latest_highways_d3, risk.v_latest_highways_d7,
  risk.v_latest_rail_d3, risk.v_latest_rail_d7,
  risk.v_latest_buildings_d3, risk.v_latest_buildings_d7
TO iva_app;
