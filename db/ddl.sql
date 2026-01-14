
-- =========================================
-- Impacted Values Analysis (IVA) bootstrap DDL
-- - Roles (iva_app read-only, iva_job writer)
-- - SCRAM passwords via psql variables : 'app_pass' and 'job_pass'
-- - Schema + tables + indexes + views + matviews + grants
-- =========================================

-- Ensure newly-set passwords are stored as SCRAM (PG14+ default).
-- ALTER SYSTEM requires a superuser; the POSTGRES_USER (iva_job) created
-- by the container is superuser by default.
ALTER SYSTEM SET password_encryption = 'scram-sha-256';
SELECT pg_reload_conf();

-- Optional: PostGIS extension (image usually loads it already).
-- CREATE EXTENSION IF NOT EXISTS postgis;

-- ---- Roles (idempotent) ----
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iva_app') THEN
    CREATE ROLE iva_app LOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iva_job') THEN
    CREATE ROLE iva_job LOGIN;
  END IF;
END$$;

-- Set role passwords via psql variables (passed as -v var=value):
-- Use :'var' so psql quotes the literal safely.
ALTER ROLE iva_app WITH PASSWORD :'app_pass';
ALTER ROLE iva_job WITH PASSWORD :'job_pass';

-- ---- Schema ----
CREATE SCHEMA IF NOT EXISTS risk;

-- ---- Lookup: feature sets ----
CREATE TABLE IF NOT EXISTS risk.feature_sets (
  id   smallserial PRIMARY KEY,
  code text UNIQUE NOT NULL,  -- 'ecumene','first_nations','highways','rail','facilities','buildings'
  name text NOT NULL
);

INSERT INTO risk.feature_sets(code,name) VALUES
  ('ecumene','Ecumene'),
  ('first_nations','First Nations'),
  ('highways','Highways'),
  ('rail','Railways'),
  ('facilities','Facilities'),
  ('buildings','Buildings')
ON CONFLICT (code) DO NOTHING;

-- ---- Features (all geometries EPSG:3979) ----
CREATE TABLE IF NOT EXISTS risk.features (
  id              bigserial PRIMARY KEY,
  feature_set_id  smallint NOT NULL REFERENCES risk.feature_sets(id),
  source_pk       text NOT NULL,
  name            text,
  attrs           jsonb,
  geom            geometry(Geometry,3979) NOT NULL,
  created_at      timestamptz DEFAULT now(),
  UNIQUE (feature_set_id, source_pk)
);
CREATE INDEX IF NOT EXISTS features_geom_gix ON risk.features USING gist (geom);

-- ---- Simulation runs (per run_date + forecast_day) ----
CREATE TABLE IF NOT EXISTS risk.runs (
  id            bigserial PRIMARY KEY,
  run_date      date NOT NULL,
  forecast_day  integer NOT NULL CHECK (forecast_day IN (3,7)),
  wmstime       date NOT NULL,                 -- run_date + (horizon-1)
  srs           integer NOT NULL DEFAULT 3979,
  res_m         integer NOT NULL DEFAULT 100,
  blob_uris     text[] NOT NULL,               -- URIs to FireSTARR tiles used
  created_at    timestamptz DEFAULT now(),
  UNIQUE (run_date, forecast_day)
);

-- ---- Per-feature stats per run ----
CREATE TABLE IF NOT EXISTS risk.feature_stats (
  id         bigserial PRIMARY KEY,
  run_id     bigint NOT NULL REFERENCES risk.runs(id) ON DELETE CASCADE,
  feature_id bigint NOT NULL REFERENCES risk.features(id) ON DELETE CASCADE,
  n          integer,
  v_min      double precision,
  p05        double precision,
  p25        double precision,
  p50        double precision,
  v_mean     double precision,
  p75        double precision,
  p95        double precision,
  v_max      double precision,
  evacuated  boolean DEFAULT false,
  created_at timestamptz DEFAULT now(),
  UNIQUE (run_id, feature_id)
);
CREATE INDEX IF NOT EXISTS feature_stats_run_idx     ON risk.feature_stats (run_id);
CREATE INDEX IF NOT EXISTS feature_stats_feature_idx ON risk.feature_stats (feature_id);

-- ---- Latest run per horizon ----
CREATE OR REPLACE VIEW risk.v_latest_runs AS
SELECT DISTINCT ON (forecast_day)
       id, run_date, forecast_day, wmstime, created_at
FROM risk.runs
ORDER BY forecast_day, run_date DESC, id DESC;

-- ---- Latest feature stats for each horizon ----
CREATE OR REPLACE VIEW risk.v_latest_feature_stats AS
SELECT r.run_date, r.forecast_day, s.feature_id, s.n, s.v_min, s.p05, s.p25, s.p50,
       s.v_mean, s.p75, s.p95, s.v_max, s.evacuated,
       f.feature_set_id, f.name, f.attrs, f.geom
FROM risk.feature_stats s
JOIN risk.runs r ON r.id = s.run_id
JOIN risk.v_latest_runs lr ON lr.id = s.run_id
JOIN risk.features f ON f.id = s.feature_id;

-- ---- Quick flags (has any intersection) ----
CREATE OR REPLACE VIEW risk.v_latest_flags AS
SELECT feature_id,
       BOOL_OR(forecast_day=3 AND n>0) AS has_d3,
       BOOL_OR(forecast_day=7 AND n>0) AS has_d7
FROM risk.v_latest_feature_stats
GROUP BY feature_id;

-- ---- Per-layer latest views ----
CREATE OR REPLACE VIEW risk.v_latest_ecumene AS
  SELECT * FROM risk.v_latest_feature_stats
  WHERE feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='ecumene');

CREATE OR REPLACE VIEW risk.v_latest_first_nations AS
  SELECT * FROM risk.v_latest_feature_stats
  WHERE feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='first_nations');

CREATE OR REPLACE VIEW risk.v_latest_highways AS
  SELECT * FROM risk.v_latest_feature_stats
  WHERE feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='highways');

CREATE OR REPLACE VIEW risk.v_latest_rail AS
  SELECT * FROM risk.v_latest_feature_stats
  WHERE feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='rail');

CREATE OR REPLACE VIEW risk.v_latest_facilities AS
  SELECT * FROM risk.v_latest_feature_stats
  WHERE feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='facilities');

-- ---- Aggregates: Buildings → CTs/Ecumene/FN (per run) ----
-- Expect a CT layer available as: public.census_tracts_2021(dguid TEXT, name TEXT, geom geometry(Polygon,3979))
CREATE MATERIALIZED VIEW IF NOT EXISTS risk.mv_buildings_ct_agg AS
SELECT s.run_id,
       ct.dguid AS ct_dguid,
       COUNT(*) AS bld_count,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY s.v_mean) AS v_mean_p50,
       MAX(s.v_max) AS max_prob
FROM risk.feature_stats s
JOIN risk.features f ON f.id = s.feature_id
JOIN public.census_tracts_2021 ct ON ST_Intersects(f.geom, ct.geom)
WHERE f.feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='buildings')
GROUP BY s.run_id, ct.dguid;
CREATE INDEX IF NOT EXISTS mv_buildings_ct_agg_idx ON risk.mv_buildings_ct_agg (run_id, ct_dguid);

CREATE MATERIALIZED VIEW IF NOT EXISTS risk.mv_buildings_ecumene_agg AS
SELECT s.run_id,
       fz.source_pk AS ecumene_pk,
       COUNT(*) AS bld_count,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY s.v_mean) AS v_mean_p50,
       MAX(s.v_max) AS max_prob
FROM risk.feature_stats s
JOIN risk.features f ON f.id = s.feature_id
JOIN risk.features fz ON fz.feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='ecumene')
                     AND ST_Intersects(f.geom, fz.geom)
WHERE f.feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='buildings')
GROUP BY s.run_id, fz.source_pk;
CREATE INDEX IF NOT EXISTS mv_buildings_ecumene_agg_idx ON risk.mv_buildings_ecumene_agg (run_id, ecumene_pk);

CREATE MATERIALIZED VIEW IF NOT EXISTS risk.mv_buildings_fn_agg AS
SELECT s.run_id,
       fn.source_pk AS fn_pk,
       COUNT(*) AS bld_count,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY s.v_mean) AS v_mean_p50,
       MAX(s.v_max) AS max_prob
FROM risk.feature_stats s
JOIN risk.features f  ON f.id = s.feature_id
JOIN risk.features fn ON fn.feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='first_nations')
                      AND ST_Intersects(f.geom, fn.geom)
WHERE f.feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='buildings')
GROUP BY s.run_id, fn.source_pk;
CREATE INDEX IF NOT EXISTS mv_buildings_fn_agg_idx ON risk.mv_buildings_fn_agg (run_id, fn_pk);

-- ---- Grants ----
GRANT USAGE ON SCHEMA risk TO iva_job, iva_app;

-- writer on all existing tables; keep for new ones via default privileges
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA risk TO iva_job;
ALTER DEFAULT PRIVILEGES IN SCHEMA risk
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO iva_job;

-- app read-only on all existing tables/views (adjust if you want finer control)
GRANT SELECT ON ALL TABLES IN SCHEMA risk TO iva_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA risk
  GRANT SELECT ON TABLES TO iva_app;
