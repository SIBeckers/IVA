-- =====================================================================
-- IVA Database DDL (Full Replacement, Raster-First Buildings, CSD Zones)
-- CRS: EPSG:3978
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- Extensions & roles
-- ---------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS postgis;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iva_app') THEN
    CREATE ROLE iva_app LOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iva_job') THEN
    CREATE ROLE iva_job LOGIN;
  END IF;
END$$;

ALTER ROLE iva_app WITH PASSWORD :'app_pass';
ALTER ROLE iva_job WITH PASSWORD :'job_pass';

-- ---------------------------------------------------------------------
-- Schemas
-- ---------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS risk;

GRANT USAGE ON SCHEMA risk TO iva_app, iva_job;

-- ---------------------------------------------------------------------
-- Feature sets
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risk.feature_sets (
  id   smallserial PRIMARY KEY,
  code text UNIQUE NOT NULL,
  name text NOT NULL
);

INSERT INTO risk.feature_sets (code, name) VALUES
  ('ecumene', 'Ecumene'),
  ('first_nations', 'First Nations'),
  ('highways', 'Highways'),
  ('rail', 'Rail'),
  ('facilities', 'Facilities'),
  ('census', 'Census Subdivisions')
ON CONFLICT (code) DO NOTHING;

-- ---------------------------------------------------------------------
-- Features (zones only; buildings are NOT features)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risk.features (
  id              bigserial PRIMARY KEY,
  feature_set_id  smallint NOT NULL REFERENCES risk.feature_sets(id),
  source_pk       text NOT NULL,
  name            text,
  attrs           jsonb,
  geom            geometry(Geometry, 3978) NOT NULL,
  created_at      timestamptz DEFAULT now(),
  UNIQUE (feature_set_id, source_pk)
);

CREATE INDEX IF NOT EXISTS features_geom_gix
  ON risk.features USING gist (geom);

-- ---------------------------------------------------------------------
-- Runs
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risk.runs (
  id            bigserial PRIMARY KEY,
  run_date      date NOT NULL,
  forecast_day  integer NOT NULL CHECK (forecast_day IN (3,7)),
  wmstime       date NOT NULL,
  srs           integer NOT NULL DEFAULT 3978,
  res_m         integer NOT NULL DEFAULT 100,
  blob_uris     text[] NOT NULL DEFAULT '{}',
  blob_names    jsonb,
  created_at    timestamptz DEFAULT now(),
  UNIQUE (run_date, forecast_day)
);

-- ---------------------------------------------------------------------
-- FireSTARR zonal statistics (continuous probability stats)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risk.feature_stats (
  run_id      bigint NOT NULL REFERENCES risk.runs(id) ON DELETE CASCADE,
  feature_id  bigint NOT NULL REFERENCES risk.features(id) ON DELETE CASCADE,
  n           integer,
  v_min       double precision,
  p05         double precision,
  p25         double precision,
  p50         double precision,
  v_mean      double precision,
  p75         double precision,
  p95         double precision,
  v_max       double precision,
  evacuated   boolean DEFAULT false,
  created_at  timestamptz DEFAULT now(),
  PRIMARY KEY (run_id, feature_id)
);

CREATE INDEX IF NOT EXISTS feature_stats_feature_idx
  ON risk.feature_stats (feature_id);
CREATE INDEX IF NOT EXISTS feature_stats_run_idx
  ON risk.feature_stats (run_id);

-- ---------------------------------------------------------------------
-- Raster-first building outputs
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risk.building_zone_stats (
  run_id         bigint    NOT NULL REFERENCES risk.runs(id) ON DELETE CASCADE,
  feature_set_id smallint  NOT NULL REFERENCES risk.feature_sets(id),
  feature_id     bigint    NOT NULL REFERENCES risk.features(id) ON DELETE CASCADE,
  building_count integer   NOT NULL,
  created_at     timestamptz DEFAULT now(),
  PRIMARY KEY (run_id, feature_set_id, feature_id)
);

CREATE INDEX IF NOT EXISTS building_zone_stats_set_idx
  ON risk.building_zone_stats (feature_set_id);
CREATE INDEX IF NOT EXISTS building_zone_stats_feature_idx
  ON risk.building_zone_stats (feature_id);
CREATE INDEX IF NOT EXISTS building_zone_stats_run_idx
  ON risk.building_zone_stats (run_id);

CREATE TABLE IF NOT EXISTS risk.building_zone_exposure (
  run_id             bigint    NOT NULL REFERENCES risk.runs(id) ON DELETE CASCADE,
  feature_set_id     smallint  NOT NULL REFERENCES risk.feature_sets(id),
  feature_id         bigint    NOT NULL REFERENCES risk.features(id) ON DELETE CASCADE,
  expected_buildings double precision NOT NULL,
  created_at         timestamptz DEFAULT now(),
  PRIMARY KEY (run_id, feature_set_id, feature_id)
);

CREATE INDEX IF NOT EXISTS building_zone_exposure_set_idx
  ON risk.building_zone_exposure (feature_set_id);
CREATE INDEX IF NOT EXISTS building_zone_exposure_feature_idx
  ON risk.building_zone_exposure (feature_id);
CREATE INDEX IF NOT EXISTS building_zone_exposure_run_idx
  ON risk.building_zone_exposure (run_id);

-- ---------------------------------------------------------------------
-- Canonical census table
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.census_subdivisions_2025 (
  csduid text PRIMARY KEY,
  name   text,
  prname text,
  geom   geometry(MultiPolygon, 3978) NOT NULL
);

CREATE INDEX IF NOT EXISTS csd_2025_geom_gix
  ON public.census_subdivisions_2025 USING gist (geom);

GRANT SELECT ON public.census_subdivisions_2025 TO iva_app, iva_job;

-- ---------------------------------------------------------------------
-- Latest-run helpers
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW risk.v_latest_runs AS
SELECT DISTINCT ON (forecast_day)
  id, run_date, forecast_day, wmstime, srs, res_m, created_at
FROM risk.runs
ORDER BY forecast_day, run_date DESC, id DESC;

-- ---------------------------------------------------------------------
-- Latest joined zone stats
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW risk.v_latest_zone_stats AS
SELECT
  r.run_date,
  r.forecast_day,
  f.id AS feature_id,
  fs.code AS feature_set,
  f.source_pk,
  f.name,
  f.attrs,
  s.n,
  s.v_min,
  s.p05,
  s.p25,
  s.p50,
  s.v_mean,
  s.p75,
  s.p95,
  s.v_max,
  s.evacuated,
  bz.building_count,
  be.expected_buildings,
  f.geom
FROM risk.v_latest_runs r
JOIN risk.feature_stats s
  ON s.run_id = r.id
JOIN risk.features f
  ON f.id = s.feature_id
JOIN risk.feature_sets fs
  ON fs.id = f.feature_set_id
LEFT JOIN risk.building_zone_stats bz
  ON bz.run_id = r.id
 AND bz.feature_id = f.id
LEFT JOIN risk.building_zone_exposure be
  ON be.run_id = r.id
 AND be.feature_id = f.id;

CREATE OR REPLACE VIEW risk.v_latest_ecumene
AS SELECT * FROM risk.v_latest_zone_stats WHERE feature_set = 'ecumene';

CREATE OR REPLACE VIEW risk.v_latest_first_nations
AS SELECT * FROM risk.v_latest_zone_stats WHERE feature_set = 'first_nations';

CREATE OR REPLACE VIEW risk.v_latest_highways
AS SELECT * FROM risk.v_latest_zone_stats WHERE feature_set = 'highways';

CREATE OR REPLACE VIEW risk.v_latest_rail
AS SELECT * FROM risk.v_latest_zone_stats WHERE feature_set = 'rail';

CREATE OR REPLACE VIEW risk.v_latest_facilities
AS SELECT * FROM risk.v_latest_zone_stats WHERE feature_set = 'facilities';

CREATE OR REPLACE VIEW risk.v_latest_census
AS SELECT * FROM risk.v_latest_zone_stats WHERE feature_set = 'census';

-- ---------------------------------------------------------------------
-- Grants
-- ---------------------------------------------------------------------
GRANT SELECT ON ALL TABLES IN SCHEMA risk TO iva_app;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA risk TO iva_app;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA risk TO iva_job;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA risk TO iva_job;

ALTER DEFAULT PRIVILEGES IN SCHEMA risk
  GRANT SELECT ON TABLES TO iva_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA risk
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO iva_job;

ALTER DEFAULT PRIVILEGES IN SCHEMA risk
  GRANT USAGE, SELECT ON SEQUENCES TO iva_job;

COMMIT;
