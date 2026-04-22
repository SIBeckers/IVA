\pset pager off
\pset null '(null)'

-- Change these if needed
-- Current successful runs from the log:
--   run_id = 1   (horizon 3)
--   run_id = 30  (horizon 7)

\echo
\echo =========================
\echo 1) Row counts by table
\echo =========================
SELECT 'feature_stats' AS table_name, run_id, COUNT(*) AS n_rows
FROM risk.feature_stats
WHERE run_id IN (1, 30)
GROUP BY run_id

UNION ALL

SELECT 'building_zone_stats' AS table_name, run_id, COUNT(*) AS n_rows
FROM risk.building_zone_stats
WHERE run_id IN (1, 30)
GROUP BY run_id

UNION ALL

SELECT 'building_zone_exposure' AS table_name, run_id, COUNT(*) AS n_rows
FROM risk.building_zone_exposure
WHERE run_id IN (1, 30)
GROUP BY run_id

ORDER BY table_name, run_id;

\echo
\echo ==========================================
\echo 2) Validation totals (should match app log)
\echo ==========================================
SELECT
    bz.run_id,
    COUNT(*) AS zones,
    COALESCE(SUM(bz.building_count), 0) AS total_buildings,
    ROUND(COALESCE(SUM(be.expected_buildings), 0.0)::numeric, 3) AS total_expected
FROM risk.building_zone_stats bz
JOIN risk.building_zone_exposure be
  ON bz.run_id = be.run_id
 AND bz.feature_id = be.feature_id
 AND bz.feature_set_id = be.feature_set_id
WHERE bz.run_id IN (1, 30)
GROUP BY bz.run_id
ORDER BY bz.run_id;

\echo
\echo ==================================
\echo 3) Feature-set coverage by run
\echo ==================================
SELECT
    bz.run_id,
    fs.code AS feature_set_code,
    COUNT(*) AS n_features,
    COALESCE(SUM(bz.building_count), 0) AS total_buildings,
    ROUND(COALESCE(SUM(be.expected_buildings), 0.0)::numeric, 3) AS total_expected
FROM risk.building_zone_stats bz
JOIN risk.building_zone_exposure be
  ON bz.run_id = be.run_id
 AND bz.feature_id = be.feature_id
 AND bz.feature_set_id = be.feature_set_id
JOIN risk.feature_sets fs
  ON bz.feature_set_id = fs.id
WHERE bz.run_id IN (1, 30)
GROUP BY bz.run_id, fs.code
ORDER BY bz.run_id, fs.code;

\echo
\echo ======================================
\echo 4) Negative / null sanity check
\echo ======================================
SELECT
    bz.run_id,
    bz.feature_set_id,
    bz.feature_id,
    bz.building_count,
    be.expected_buildings
FROM risk.building_zone_stats bz
JOIN risk.building_zone_exposure be
  ON bz.run_id = be.run_id
 AND bz.feature_id = be.feature_id
 AND bz.feature_set_id = be.feature_set_id
WHERE bz.run_id IN (1, 30)
  AND (
      bz.building_count < 0
      OR be.expected_buildings < 0
      OR bz.building_count IS NULL
      OR be.expected_buildings IS NULL
  );

\echo
\echo ============================
\echo 5) Top exposed features
\echo ============================
SELECT
    be.run_id,
    fs.code AS feature_set_code,
    be.feature_id,
    bz.building_count,
    ROUND(be.expected_buildings::numeric, 3) AS expected_buildings
FROM risk.building_zone_exposure be
JOIN risk.building_zone_stats bz
  ON bz.run_id = be.run_id
 AND bz.feature_id = be.feature_id
 AND bz.feature_set_id = be.feature_set_id
JOIN risk.feature_sets fs
  ON be.feature_set_id = fs.id
WHERE be.run_id IN (1, 30)
ORDER BY be.expected_buildings DESC
LIMIT 20;