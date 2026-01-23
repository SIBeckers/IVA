
-- =====================================================================
-- IVA Database DDL (Full Replacement)
-- Adds: SQL-only ingestion of CSD 2025 & reference GeoPackages using ogr_fdw
--       Raw publishable views per feature set + latest intersection views
-- SRID: EPSG:3979
-- =====================================================================

-- ------------------------------------------------------------
-- Base extensions / roles
-- ------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS ogr_fdw;

ALTER SYSTEM SET password_encryption = 'scram-sha-256';
SELECT pg_reload_conf();

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iva_app') THEN
    CREATE ROLE iva_app LOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iva_job') THEN
    CREATE ROLE iva_job LOGIN;
  END IF;
END$$;

-- Set passwords via psql variables:
--   psql -v app_pass='...' -v job_pass='...' -f db/ddl.sql
ALTER ROLE iva_app WITH PASSWORD :'app_pass';
ALTER ROLE iva_job WITH PASSWORD :'job_pass';

-- ------------------------------------------------------------
-- Schemas
-- ------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS risk;
CREATE SCHEMA IF NOT EXISTS fdw;  -- foreign tables imported here

-- ------------------------------------------------------------
-- Lookup: feature sets
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risk.feature_sets (
  id smallserial PRIMARY KEY,
  code text UNIQUE NOT NULL,
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

-- ------------------------------------------------------------
-- Features / Runs / Stats
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risk.features (
  id bigserial PRIMARY KEY,
  feature_set_id smallint NOT NULL REFERENCES risk.feature_sets(id),
  source_pk text NOT NULL,
  name text,
  attrs jsonb,
  geom geometry(Geometry,3979) NOT NULL,
  created_at timestamptz DEFAULT now(),
  UNIQUE (feature_set_id, source_pk)
);
CREATE INDEX IF NOT EXISTS features_geom_gix ON risk.features USING gist (geom);

CREATE TABLE IF NOT EXISTS risk.runs (
  id bigserial PRIMARY KEY,
  run_date date NOT NULL,
  forecast_day integer NOT NULL CHECK (forecast_day IN (3,7)),
  wmstime date NOT NULL,
  srs integer NOT NULL DEFAULT 3979,
  res_m integer NOT NULL DEFAULT 100,
  blob_uris text[] NOT NULL,
  created_at timestamptz DEFAULT now(),
  UNIQUE (run_date, forecast_day)
);

CREATE TABLE IF NOT EXISTS risk.feature_stats (
  id bigserial PRIMARY KEY,
  run_id bigint NOT NULL REFERENCES risk.runs(id) ON DELETE CASCADE,
  feature_id bigint NOT NULL REFERENCES risk.features(id) ON DELETE CASCADE,
  n integer,
  v_min double precision,
  p05 double precision,
  p25 double precision,
  p50 double precision,
  v_mean double precision,
  p75 double precision,
  p95 double precision,
  v_max double precision,
  evacuated boolean DEFAULT false,
  created_at timestamptz DEFAULT now(),
  UNIQUE (run_id, feature_id)
);
CREATE INDEX IF NOT EXISTS feature_stats_run_idx ON risk.feature_stats (run_id);
CREATE INDEX IF NOT EXISTS feature_stats_feature_idx ON risk.feature_stats (feature_id);

-- ------------------------------------------------------------
-- Latest views and raw views
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW risk.v_latest_runs AS
SELECT DISTINCT ON (forecast_day)
  id, run_date, forecast_day, wmstime, created_at
FROM risk.runs
ORDER BY forecast_day, run_date DESC, id DESC;

CREATE OR REPLACE VIEW risk.v_latest_feature_stats AS
SELECT r.run_date, r.forecast_day, s.feature_id, s.n, s.v_min, s.p05, s.p25, s.p50,
       s.v_mean, s.p75, s.p95, s.v_max, s.evacuated,
       f.feature_set_id, f.name, f.attrs, f.geom
FROM risk.feature_stats s
JOIN risk.runs r ON r.id = s.run_id
JOIN risk.v_latest_runs lr ON lr.id = s.run_id
JOIN risk.features f ON f.id = s.feature_id;

CREATE OR REPLACE VIEW risk.v_latest_flags AS
SELECT feature_id,
  BOOL_OR(forecast_day=3 AND n>0) AS has_d3,
  BOOL_OR(forecast_day=7 AND n>0) AS has_d7
FROM risk.v_latest_feature_stats
GROUP BY feature_id;


CREATE OR REPLACE VIEW risk.v_latest_ecumene AS
SELECT
  run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max, evacuated,
  feature_set_id, name, attrs,
  ST_Multi(ST_Transform(geom, 3978))::geometry(MultiPolygon, 3978) AS geom
FROM risk.v_latest_feature_stats
WHERE feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='ecumene');

CREATE OR REPLACE VIEW risk.v_latest_first_nations AS
SELECT run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max, evacuated,
  feature_set_id, name, attrs,
  ST_Multi(ST_Transform(geom, 3978))::geometry(MultiPolygon, 3978) AS geom
FROM risk.v_latest_feature_stats
WHERE feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='first_nations');

CREATE OR REPLACE VIEW risk.v_latest_highways AS
SELECT run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max, evacuated,
  feature_set_id, name, attrs,
  ST_Multi(ST_Transform(geom, 3978))::geometry(MultiLineString, 3978) AS geom
FROM risk.v_latest_feature_stats
WHERE feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='highways');

CREATE OR REPLACE VIEW risk.v_latest_rail AS
SELECT run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max, evacuated,
  feature_set_id, name, attrs,
  ST_Multi(ST_Transform(geom, 3978))::geometry(MultiLineString, 3978) AS geom
FROM risk.v_latest_feature_stats
WHERE feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='rail');

CREATE OR REPLACE VIEW risk.v_latest_facilities AS
SELECT run_date, forecast_day, feature_id, n, v_min, p05, p25, p50, v_mean, p75, p95, v_max, evacuated,
  feature_set_id, name, attrs,
  ST_Multi(ST_Transform(geom, 3978))::geometry(MultiPolygon, 3978) AS geom
FROM risk.v_latest_feature_stats
  WHERE feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='facilities');

CREATE OR REPLACE VIEW risk.v_features_raw AS
SELECT f.id, fs.code AS feature_set_code, f.source_pk, f.name, f.attrs, f.geom, f.created_at
FROM risk.features f
JOIN risk.feature_sets fs ON fs.id = f.feature_set_id;

CREATE OR REPLACE VIEW risk.v_feature_stats_all AS
SELECT s.id AS stat_id, s.run_id, r.run_date, r.forecast_day, r.wmstime,
       s.feature_id, fs.code AS feature_set_code,
       s.n, s.v_min, s.p05, s.p25, s.p50, s.v_mean, s.p75, s.p95, s.v_max, s.evacuated,
       f.name AS feature_name, f.attrs AS feature_attrs,
       ST_Transform(f.geom, 3978)::geometry(Geometry, 3978) AS geom,
       s.created_at AS stat_created_at
FROM risk.feature_stats s
JOIN risk.runs r ON r.id = s.run_id
JOIN risk.features f ON f.id = s.feature_id
JOIN risk.feature_sets fs ON fs.id = f.feature_set_id;

CREATE OR REPLACE VIEW risk.v_feature_stats_d3 AS
  SELECT * FROM risk.v_feature_stats_all WHERE forecast_day=3;
CREATE OR REPLACE VIEW risk.v_feature_stats_d7 AS
  SELECT * FROM risk.v_feature_stats_all WHERE forecast_day=7;

-- ------------------------------------------------------------
-- CSD 2025 canonical tables & ingest function (Option B SQL)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.census_subdivisions_2025 (
  csduid text PRIMARY KEY,
  name   text,
  prname text,
  geom   geometry(MultiPolygon, 3979)
);
CREATE INDEX IF NOT EXISTS csd_2025_geom_gix ON public.census_subdivisions_2025 USING GIST (geom);


CREATE OR REPLACE VIEW public.census_subdivisions AS
SELECT
  csduid,
  name,
  prname,
  ST_Transform(geom, 3978)::geometry(MultiPolygon, 3978) AS geom
FROM public.census_subdivisions_2025;


GRANT SELECT ON public.census_subdivisions_2025, public.census_subdivisions TO iva_app;




-- CSD 2025 ingest using exact FDW column names from lcsd000a25p_e.gpkg
-- Geometry column: geom; Attributes: CSDUID, CSDNAME, PRNAME
CREATE OR REPLACE FUNCTION public.ingest_csd_2025_from(src regclass)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  src_srid int;
  srid_sql text;
BEGIN
  -- Detect SRID from a sample row's geom; fall back to EPSG:3347 (StatsCan Albers)
  srid_sql := format($q$SELECT ST_SRID(geom) FROM %s WHERE geom IS NOT NULL LIMIT 1$q$, src::text);
  EXECUTE srid_sql INTO src_srid;
  IF src_srid IS NULL OR src_srid = 0 THEN
    src_srid := 3347;
  END IF;

  -- Upsert into canonical table in EPSG:3979 with POLYGON->MULTIPOLYGON normalization
  
  EXECUTE format($q$
    INSERT INTO public.census_subdivisions_2025 (csduid, name, prname, geom)
    SELECT
      t.csduid::text  AS csduid,
      t.csdname::text AS name,
      t.prname::text  AS prname,
      CASE
        WHEN %1$s = 3979 THEN
          CASE WHEN GeometryType(t.geom) LIKE 'POLYGON%%' THEN ST_Multi(t.geom) ELSE t.geom END
        ELSE
          CASE WHEN GeometryType(t.geom) LIKE 'POLYGON%%' THEN ST_Multi(ST_Transform(t.geom,3979))
              ELSE ST_Transform(t.geom,3979) END
      END AS geom
    FROM %2$s AS t
    WHERE t.geom IS NOT NULL
      AND t.csduid IS NOT NULL
      AND t.csduid <> ''
    ON CONFLICT (csduid) DO UPDATE
      SET name   = EXCLUDED.name,
          prname = EXCLUDED.prname,
          geom   = EXCLUDED.geom
  $q$, src_srid, src::text);
END$$;



-- ------------------------------------------------------------
-- Aggregates: Buildings → CSD / Ecumene / First Nations (per run)
-- ------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS risk.mv_buildings_csd_agg AS
SELECT s.run_id,
       csd.csduid AS csd_id,
       COUNT(*) AS bld_count,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY s.v_mean) AS v_mean_p50,
       MAX(s.v_max) AS max_prob
FROM risk.feature_stats s
JOIN risk.features f ON f.id = s.feature_id
JOIN public.census_subdivisions_2025 csd ON ST_Intersects(f.geom, csd.geom)
WHERE f.feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='buildings')
GROUP BY s.run_id, csd.csduid;
CREATE UNIQUE INDEX IF NOT EXISTS mv_buildings_csd_agg_idx ON risk.mv_buildings_csd_agg (run_id, csd_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS risk.mv_buildings_ecumene_agg AS
SELECT s.run_id,
       fz.source_pk AS ecumene_pk,
       COUNT(*) AS bld_count,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY s.v_mean) AS v_mean_p50,
       MAX(s.v_max) AS max_prob
FROM risk.feature_stats s
JOIN risk.features f ON f.id = s.feature_id
JOIN risk.features fz
  ON fz.feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='ecumene')
 AND ST_Intersects(f.geom, fz.geom)
WHERE f.feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='buildings')
GROUP BY s.run_id, fz.source_pk;
CREATE UNIQUE INDEX IF NOT EXISTS mv_buildings_ecumene_agg_idx ON risk.mv_buildings_ecumene_agg (run_id, ecumene_pk);

CREATE MATERIALIZED VIEW IF NOT EXISTS risk.mv_buildings_fn_agg AS
SELECT s.run_id,
       fn.source_pk AS fn_pk,
       COUNT(*) AS bld_count,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY s.v_mean) AS v_mean_p50,
       MAX(s.v_max) AS max_prob
FROM risk.feature_stats s
JOIN risk.features f ON f.id = s.feature_id
JOIN risk.features fn
  ON fn.feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='first_nations')
 AND ST_Intersects(f.geom, fn.geom)
WHERE f.feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='buildings')
GROUP BY s.run_id, fn.source_pk;
CREATE UNIQUE INDEX IF NOT EXISTS mv_buildings_fn_agg_idx ON risk.mv_buildings_fn_agg (run_id, fn_pk);

-- Latest (geom-included) intersection views for publishing
CREATE OR REPLACE VIEW risk.v_buildings_csd_agg_latest AS
SELECT r.run_date, r.forecast_day, a.csd_id, a.bld_count, a.v_mean_p50, a.max_prob, 
  ST_Multi(ST_Transform(csd.geom, 3978))::geometry(MultiPolygon, 3978) AS geom
FROM risk.mv_buildings_csd_agg a
JOIN risk.runs r ON r.id = a.run_id
JOIN risk.v_latest_runs lr ON lr.id = a.run_id
JOIN public.census_subdivisions csd ON csd.csduid = a.csd_id;

CREATE OR REPLACE VIEW risk.v_buildings_ecumene_agg_latest AS
SELECT r.run_date, r.forecast_day, a.ecumene_pk, a.bld_count, a.v_mean_p50, a.max_prob, 
  ST_Multi(ST_Transform(fz.geom, 3978))::geometry(MultiPolygon, 3978) AS geom
FROM risk.mv_buildings_ecumene_agg a
JOIN risk.runs r ON r.id = a.run_id
JOIN risk.v_latest_runs lr ON lr.id = a.run_id
JOIN risk.features fz
  ON fz.feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='ecumene')
 AND fz.source_pk = a.ecumene_pk;

CREATE OR REPLACE VIEW risk.v_buildings_fn_agg_latest AS
SELECT r.run_date, r.forecast_day, a.fn_pk, a.bld_count, a.v_mean_p50, a.max_prob, 
  ST_Multi(ST_Transform(fn.geom, 3978))::geometry(MultiPolygon, 3978) AS geom
FROM risk.mv_buildings_fn_agg a
JOIN risk.runs r ON r.id = a.run_id
JOIN risk.v_latest_runs lr ON lr.id = a.run_id
JOIN risk.features fn
  ON fn.feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='first_nations')
 AND fn.source_pk = a.fn_pk;

-- ------------------------------------------------------------
-- ogr_fdw Servers for your GeoPackages (files are at /data)
-- ------------------------------------------------------------
-- NOTE: These IMPORT FOREIGN SCHEMA steps introspect the file and create
--       foreign tables in schema "fdw" named from the layers inside.

CREATE SERVER IF NOT EXISTS fdw_csd
  FOREIGN DATA WRAPPER ogr_fdw
  OPTIONS (datasource '/data/lcsd000a25p_e.gpkg', format 'GPKG');
IMPORT FOREIGN SCHEMA ogr_all FROM SERVER fdw_csd INTO fdw;

CREATE SERVER IF NOT EXISTS fdw_ecumene
  FOREIGN DATA WRAPPER ogr_fdw
  OPTIONS (datasource '/data/ECUMENE_V3.gpkg', format 'GPKG');
IMPORT FOREIGN SCHEMA ogr_all FROM SERVER fdw_ecumene INTO fdw;

CREATE SERVER IF NOT EXISTS fdw_facilities
  FOREIGN DATA WRAPPER ogr_fdw
  OPTIONS (datasource '/data/facilities.gpkg', format 'GPKG');
IMPORT FOREIGN SCHEMA ogr_all FROM SERVER fdw_facilities INTO fdw;

CREATE SERVER IF NOT EXISTS fdw_firstnations
  FOREIGN DATA WRAPPER ogr_fdw
  OPTIONS (datasource '/data/FirstNations.gpkg', format 'GPKG');
IMPORT FOREIGN SCHEMA ogr_all FROM SERVER fdw_firstnations INTO fdw;

CREATE SERVER IF NOT EXISTS fdw_highways
  FOREIGN DATA WRAPPER ogr_fdw
  OPTIONS (datasource '/data/highways_v2.gpkg', format 'GPKG');
IMPORT FOREIGN SCHEMA ogr_all FROM SERVER fdw_highways INTO fdw;

CREATE SERVER IF NOT EXISTS fdw_railways
  FOREIGN DATA WRAPPER ogr_fdw
  OPTIONS (datasource '/data/railways_v2.gpkg', format 'GPKG');
IMPORT FOREIGN SCHEMA ogr_all FROM SERVER fdw_railways INTO fdw;

-- ------------------------------------------------------------
-- Generic FDW → risk.features ingestion function
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION risk.ingest_features_from_fdw(
  src regclass,
  feature_set_code text,
  srid int DEFAULT 3979
) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
  fsid smallint;
  sql text;
  rows bigint;
BEGIN
  SELECT id INTO fsid FROM risk.feature_sets WHERE code=feature_set_code;
  IF fsid IS NULL THEN
    RAISE EXCEPTION 'Unknown feature_set_code: %', feature_set_code;
  END IF;

  sql := format($q$
    INSERT INTO risk.features (feature_set_id, source_pk, name, attrs, geom)
    SELECT %s::smallint AS feature_set_id,
           COALESCE(j->>'SOURCE_PK', j->>'CSDUID', j->>'OBJECTID', j->>'ID', j->>'fid', md5(ST_AsEWKB(geom))) AS source_pk,
           COALESCE(j->>'NAME', j->>'name', j->>'CSDNAME', j->>'ENG_NAME', j->>'FULLNAME', j->>'FIRST_NATION_NAME') AS name,
           (j - 'geom' - 'geometry')::jsonb AS attrs,
           CASE
             WHEN ST_SRID(geom) = %s THEN
               CASE WHEN GeometryType(geom) LIKE 'POLYGON%%' THEN ST_Multi(geom) ELSE geom END
             ELSE
               CASE WHEN GeometryType(geom) LIKE 'POLYGON%%' THEN ST_Multi(ST_Transform(geom,%s))
                    ELSE ST_Transform(geom,%s) END
           END AS geom
    FROM (
      SELECT to_jsonb(t) AS j, t.geom AS geom
      FROM %s AS t
      WHERE t.geom IS NOT NULL
    ) q
    ON CONFLICT (feature_set_id, source_pk) DO UPDATE
      SET name = EXCLUDED.name,
          attrs = EXCLUDED.attrs,
          geom  = EXCLUDED.geom
    RETURNING 1
  $q$, fsid, srid, srid, srid, src::text);

  EXECUTE sql;
  GET DIAGNOSTICS rows = ROW_COUNT;
  RETURN rows;
END$$;

-- ------------------------------------------------------------
-- Perform imports during build
-- If a layer name differs after IMPORT, adjust the regclass below.
-- ------------------------------------------------------------

-- CSD 2025 from fdw (expected foreign table name: fdw.lcsd000a25p_e)
DO $$
BEGIN
  PERFORM public.ingest_csd_2025_from('fdw.lcsd000a25p_e'::regclass);
EXCEPTION WHEN undefined_table THEN
  RAISE NOTICE 'FDW table for CSD not found (fdw.lcsd000a25p_e). Check actual layer name in schema "fdw".';
END$$;

-- Reference features into risk.features
DO $$ BEGIN
  PERFORM risk.ingest_features_from_fdw('fdw.ECUMENE_V3'::regclass,    'ecumene');
EXCEPTION WHEN undefined_table THEN
  RAISE NOTICE 'FDW table for ECUMENE not found. Adjust to the actual layer name in fdw.*';
END$$;

DO $$ BEGIN
  PERFORM risk.ingest_features_from_fdw('fdw.facilities'::regclass,     'facilities');
EXCEPTION WHEN undefined_table THEN
  RAISE NOTICE 'FDW table for facilities not found. Adjust fdw.* layer name.';
END$$;

DO $$ BEGIN
  PERFORM risk.ingest_features_from_fdw('fdw.FirstNations'::regclass,   'first_nations');
EXCEPTION WHEN undefined_table THEN
  RAISE NOTICE 'FDW table for First Nations not found. Adjust fdw.* layer name.';
END$$;

DO $$ BEGIN
  PERFORM risk.ingest_features_from_fdw('fdw.highways_v2'::regclass,    'highways');
EXCEPTION WHEN undefined_table THEN
  RAISE NOTICE 'FDW table for highways not found. Adjust fdw.* layer name.';
END$$;

DO $$ BEGIN
  PERFORM risk.ingest_features_from_fdw('fdw.railways_v2'::regclass,    'rail');
EXCEPTION WHEN undefined_table THEN
  RAISE NOTICE 'FDW table for railways not found. Adjust fdw.* layer name.';
END$$;

-- ------------------------------------------------------------
-- Per-set RAW views (nice endpoints for tiles/feature services)
-- ------------------------------------------------------------

CREATE OR REPLACE VIEW risk.v_features_ecumene_raw AS
SELECT
  id, feature_set_code, source_pk, name, attrs, created_at,
  ST_Multi(ST_Transform(geom, 3978))::geometry(MultiPolygon, 3978) AS geom
FROM risk.v_features_raw WHERE feature_set_code='ecumene';

CREATE OR REPLACE VIEW risk.v_features_first_nations_raw AS
SELECT
  id, feature_set_code, source_pk, name, attrs, created_at,
  ST_Multi(ST_Transform(geom, 3978))::geometry(MultiPolygon, 3978) AS geom
FROM risk.v_features_raw WHERE feature_set_code='first_nations';

CREATE OR REPLACE VIEW risk.v_features_highways_raw AS
SELECT
  id, feature_set_code, source_pk, name, attrs, created_at,
  ST_Multi(ST_Transform(geom, 3978))::geometry(MultiLineString, 3978) AS geom
FROM risk.v_features_raw WHERE feature_set_code='highways';

CREATE OR REPLACE VIEW risk.v_features_rail_raw AS
SELECT
  id, feature_set_code, source_pk, name, attrs, created_at,
  ST_Multi(ST_Transform(geom, 3978))::geometry(MultiLineString, 3978) AS geom
FROM risk.v_features_raw WHERE feature_set_code='rail';

CREATE OR REPLACE VIEW risk.v_features_facilities_raw AS
SELECT 
  id, feature_set_code, source_pk, name, attrs, created_at,
  ST_Multi(ST_Transform(geom, 3978))::geometry(MultiPolygon, 3978) AS geom
FROM risk.v_features_raw WHERE feature_set_code='facilities';

-- ------------------------------------------------------------
-- Grants / defaults
-- ------------------------------------------------------------
GRANT USAGE ON SCHEMA risk TO iva_job, iva_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA risk TO iva_job;
ALTER DEFAULT PRIVILEGES IN SCHEMA risk
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO iva_job;

CREATE OR REPLACE VIEW risk.v_runs_raw AS
SELECT id, run_date, forecast_day, wmstime, srs, res_m, blob_uris, created_at
FROM risk.runs;

GRANT SELECT ON ALL TABLES IN SCHEMA risk TO iva_app;
GRANT SELECT ON
  risk.v_features_raw,
  risk.v_feature_stats_all,
  risk.v_feature_stats_d3,
  risk.v_feature_stats_d7,
  risk.v_latest_feature_stats,
  risk.v_runs_raw,
  risk.v_latest_ecumene,
  risk.v_latest_first_nations,
  risk.v_latest_highways,
  risk.v_latest_rail,
  risk.v_latest_facilities,
  risk.v_buildings_csd_agg_latest,
  risk.v_buildings_ecumene_agg_latest,
  risk.v_buildings_fn_agg_latest,
  risk.v_features_ecumene_raw,
  risk.v_features_first_nations_raw,
  risk.v_features_highways_raw,
  risk.v_features_rail_raw,
  risk.v_features_facilities_raw
TO iva_app;

-- Optional helper to refresh MVs
CREATE OR REPLACE FUNCTION risk.refresh_all_mvs()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY risk.mv_buildings_csd_agg;
  REFRESH MATERIALIZED VIEW CONCURRENTLY risk.mv_buildings_ecumene_agg;
  REFRESH MATERIALIZED VIEW CONCURRENTLY risk.mv_buildings_fn_agg;
END$$;

-- Keep a raw runs view at the end (for completeness)

