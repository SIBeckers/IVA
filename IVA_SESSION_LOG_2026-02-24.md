# IVA Session Log — 2026-02-24

> Purpose: Document work on building feature population, layer consolidation, and setup for FireSTARR integration.

## 1) Context & Goals

- **Primary objective:** Ensure buildings are loaded into `risk.features` as a queryable layer, eliminating partial duplication between `iva_job/loaders.py` and `db/ddl.sql`.
- **Secondary objective:** Implement ECUMENE-aware building filtering (load only buildings NOT inside ECUMENE polygons).
- **Tertiary objective:** Prepare infrastructure for `firestarr.py` work to grab data and run zonal intersections.

## 2) Issues Identified (Start of Session)

### 2.1 Partial duplication

**Problem:** Buildings ingestion logic existed in two places:
- `iva_job/loaders.py`: Python loop over `*_structures_en.gpkg` files calling `upsert_features()` (row-by-row INSERT … ON CONFLICT).
- `db/ddl.sql` / `db/ddl_patch_views.sql`: FDW servers + per-set views for other features (ecumene, highways, etc.) but **no corresponding buildings import**.

**Symptom:** User ran `SELECT COUNT(*) FROM risk.v_features_raw WHERE feature_set_code='buildings'` → 0 rows, despite buildings being in `/data/`.

### 2.2 Missing buildings layer for web map

**Problem:** No `risk.v_features_buildings_raw` view existed, so:
- pg_tileserv could not publish a buildings tile layer.
- Buildings aggregation views (v_buildings_csd_agg_latest, etc.) existed but had no source data.

## 3) Root Cause Analysis

### 3.1 FDW approach failures

Initial attempt added dynamic FDW server creation + `IMPORT FOREIGN SCHEMA` + `risk.ingest_features_from_fdw()` calls for all 13 provincial building GPKGs in `ddl.sql`.

**Failure mode:** 
- ogr_fdw could not read GPKG files from the read-only `/data` mount in the PostGIS container.
- GDAL warning: `unable to open database file: this file is a WAL-enabled database. It cannot be opened because it is presumably read-only or in a read-only directory.`
- Foreign table imports returned zero tables per file.

**Conclusion:** FDW+ogr_fdw is unsuitable for read-only data mounts; fallback to Python loader.

### 3.2 Compose/startup race condition

- `iva-job` container started before `initdb` DDL had fully completed.
- Python `psycopg.connect()` failed with "connection refused" on first attempt.
- Original `compose.yaml` entrypoint syntax was incorrect (array of items instead of chained commands).

## 4) Changes Delivered

### 4.1 Database layer (`db/ddl.sql`)

#### Addition: Buildings raw view
```sql
CREATE OR REPLACE VIEW risk.v_features_buildings_raw AS
SELECT
  id, feature_set_code, source_pk, name, attrs, created_at,
  ST_Multi(ST_Transform(geom, 3978))::geometry(MultiPolygon, 3978) AS geom
FROM risk.v_features_raw WHERE feature_set_code='buildings';
```
- Added to the per-set raw views section.
- Grants updated to include `iva_app` read access.

#### Attempted: FDW-based building import (static list with error handling)
```sql
DO $$
DECLARE
    layer text;
    fname text;
    rows bigint;
    layers text[] := ARRAY[
      'ab_structures_en','bc_structures_en', … , 'yk_structures_en'
    ];
BEGIN
    FOR layer IN SELECT unnest(layers) LOOP
        RAISE NOTICE 'building import: attempting file %', fname;
        rows := risk.ingest_features_from_fdw('fdw.'||layer::regclass, 'buildings');
        RAISE NOTICE '  -> % rows inserted/updated', rows;
    END LOOP;
END$$;
```
- Wrapped each step in `BEGIN…EXCEPTION` for graceful failure.
- Added progress logging via `RAISE NOTICE`.
- **Status:** Created but ultimately unused (FDW failed due to mount issues).

### 4.2 Database layer (`db/ddl_patch_views.sql`)

#### Addition: Buildings per-horizon views
```sql
CREATE OR REPLACE VIEW risk.v_latest_buildings_d3 AS
SELECT run_date, forecast_day, feature_id, n, v_min, …, is_new, …, geom
FROM risk.v_latest_feature_stats_with_prev
WHERE forecast_day=3 AND feature_set_id=(SELECT id FROM risk.feature_sets WHERE code='buildings');

CREATE OR REPLACE VIEW risk.v_latest_buildings_d7 AS …
```
- Matches pattern of other feature sets (ecumene, highways, etc.).
- Includes `is_new` flag (true if feature had stats in current run but not prior run).
- Updated grants to allow `iva_app` read access.

### 4.3 Python loader (`job/iva_job/loaders.py`)

#### Addition: Connection retry logic with exponential backoff
```python
def connect_with_retry(max_attempts=10, backoff_sec=2):
    """Retry database connection with exponential backoff."""
    for attempt in range(1, max_attempts + 1):
        try:
            conn = psycopg.connect(…, connect_timeout=5)
            return conn
        except (psycopg.OperationalError, psycopg.Error) as e:
            if attempt < max_attempts:
                wait = backoff_sec * (2 ** (attempt - 1))
                log.warning(f'connection failed: {e}; retrying in {wait}s')
                time.sleep(wait)
            else:
                raise
```
- Allows 10 attempts with exponential backoff (2s, 4s, 8s, …).
- Emits progress log messages for troubleshooting.

#### Enhancement: Logging throughout upsert
```python
log.info(f'upserting {total} features into set {set_code}')
for _, row in gdf.iterrows():
    …
    count += 1
    if count % 10000 == 0:
        log.info(f'  processed {count}/{total}')
conn.commit()
log.info(f'completed upsert of {count} features into {set_code}')
```
- **Why:** For a 10M+ building dataset, progress reports are critical for monitoring and troubleshooting long-running imports.

#### Partial implementation: Parallel building import (BUILDING_WORKERS)
```python
workers = int(os.getenv('BUILDING_WORKERS', '0'))
if workers and workers > 1:
    from multiprocessing import Pool
    log.info(f'importing {len(blds)} building files with {workers} workers')
    with Pool(processes=workers) as pool:
        pool.map(_process_building, blds)
else:
    for b in blds:
        _process_building(b)
```
- Supports optional parallel import (disabled by default: `BUILDING_WORKERS=0`).
- Can be enabled via environment variable in compose or .env.

#### Partial implementation: ECUMENE filtering
```python
def _load_with_ecumene_filter(bld_gdf, ecumene_gdf):
    """Keep only buildings NOT inside any ECUMENE polygon."""
    filtered = bld_gdf[~bld_gdf.geometry.within(ecumene_gdf.unary_union)]
    return filtered
```
- **Status:** Function skeleton added; integration into loader loop not yet completed.
- **Goal:** Load ECUMENE_V3.gpkg once and use `.within()` to exclude ECUMENE-interior buildings.

### 4.4 Compose/orchestration (`compose.yaml`)

#### Fix: initdb entrypoint chaining
```yaml
initdb:
  …
  entrypoint:
    - bash
    - -c
    - |
      psql -h postgis -U iva_job -d impacted_values \
        -v app_pass="${APP_PGPASSWORD}" -v job_pass="${PGPASSWORD}" \
        -f /ddl/ddl.sql && \
      psql -h postgis -U iva_job -d impacted_values \
        -v app_pass="${APP_PGPASSWORD}" -v job_pass="${PGPASSWORD}" \
        -f /ddl/ddl_patch_views.sql
```
- **Why:** Original syntax was array-of-items which psql interpreted as separate commands, not a chain. Now both scripts run in sequence with AND operator.
- Added volume mount for `ddl_patch_views.sql`.

#### Enhancement: iva-job startup timing and environment
```yaml
iva-job:
  …
  environment:
    - FEATURE_SET_CODES=ecumene,first_nations,highways,rail,facilities,buildings
    - PGPASSWORD=${PGPASSWORD}
    - PGHOST=postgis
    - PGPORT=5432
    - PGUSER=iva_job
    - PGDATABASE=impacted_values
  command: ["bash","-c","sleep 10 && python -m iva_job.loaders /data && python -m iva_job.main"]
```
- **Why:** Explicit env vars + longer sleep (10s) allows initdb + pgbouncer to fully stabilize.
- Changed to `-c` (not `-lc`) to avoid unrelated shell profile sourcing.
- Loader runs **before** main job loop to ensure reference features are pre-loaded.

## 5) Status of Implementation

### ✅ Completed

- [x] risk.v_features_buildings_raw view created (ddl.sql + grants).
- [x] risk.v_latest_buildings_d3, risk.v_latest_buildings_d7 views created (ddl_patch_views.sql).
- [x] Logging and progress reporting added to loaders.py.
- [x] Connection retry logic implemented in loaders.py.
- [x] Compose entrypoint syntax fixed (initdb chaining).
- [x] Environment variable organization improved in compose.yaml.

### 🟡 Partially Complete

- [ ] **Building data population:** FDW approach abandoned due to read-only mount. Python loader with retry logic is in place and deployed, but **end-to-end loading has not yet been validated in the running containers.**
- [ ] **ECUMENE filtering:** Function skeleton exists but integration into the main loader loop not yet completed. Not tested.
- [ ] **Parallel building import:** Code added for `BUILDING_WORKERS` but not yet tested.

### ⏳ Not Started

- [ ] Validation of building counts in `risk.features`.
- [ ] FireSTARR data download verification (firestarr.py).
- [ ] Full pipeline zonal intersection testing.

## 6) Known Issues / Blockers

### 6.1 ogr_fdw + read-only mount incompatibility

**Issue:** GDAL layers in read-only mounts cannot be opened (WAL database lockfile issue).

**Workaround:** Use Python loader only. Accept that SQL-based import is not viable in this container setup.

**Inference:** If very-large-scale ingestion becomes a bottleneck, consider:
- Making `/data` read-write during init (security/liability trade-off).
- Pre-converting GPKGs to temporary staging tables via Python before DDL runs.
- Moving bulk-import to init hook in the PostGIS container itself (via Dockerfile COPY + SQL script).

### 6.2 psycopg connection timeout semantics

**Issue:** The psycopg parameter `timeout` does not exist; should be `connect_timeout`.

**Fix:** Applied in loaders.py (line ~25).

### 6.3 Missing multiprocessing dependencies

**Issue:** If `BUILDING_WORKERS > 1`, the multiprocessing module is used but may not be installed in container.

**Status:** Standard library so should work; not yet tested.

## 7) How to Resume / Verify (Next Session)

1. **Verify buildings are populated:**
   ```sql
   SELECT COUNT(*) FROM risk.features 
   WHERE feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='buildings');
   ```
   Should be > 0 (expect millions if all provinces loaded).

2. **Check per-province row counts:**
   ```sql
   SELECT name, COUNT(*) FROM risk.features f
   JOIN risk.feature_sets fs ON f.feature_set_id = fs.id
   WHERE fs.code = 'buildings'
   GROUP BY name;
   ```

3. **Verify buildings raw view works:**
   ```sql
   SELECT COUNT(*), ST_Extent(geom) FROM risk.v_features_buildings_raw LIMIT 1;
   ```

4. **Tail iva-job logs for loader progress:**
   ```bash
   podman compose logs iva-job --tail 100 -f
   ```
   Look for log lines like: `upserting 123456 features into set buildings` and `processed 10000/123456`.

5. **Implement ECUMENE filtering** (if filtering is desired):
   - Load ECUMENE into memory once.
   - Call filter function on each building GeoDataFrame before upsert.
   - Add environment flag `BUILDING_ECUMENE_FILTER=1` to enable/disable.

6. **Test parallel import** (optional optimization):
   - Set `BUILDING_WORKERS=2` or `4` in compose env.
   - Monitor CPU/memory during import.
   - Compare wall-clock time vs. sequential import.

## 8) Handoff for Next Session

### Files modified/created:
- `db/ddl.sql` — Added FDW logic + buildings raw view + grants.
- `db/ddl_patch_views.sql` — Added buildings d3/d7 views + grants.
- `job/iva_job/loaders.py` — Added retry logic, logging, parallel skeleton, ECUMENE filter skeleton.
- `compose.yaml` — Fixed initdb entrypoint, improved iva-job env + timing.

### Next priorities:
1. **Verify building population** end-to-end (run full stack, check counts).
2. **Finish ECUMENE filter** and test option.
3. **Prepare firestarr.py** for data download + parity testing.
4. **End-to-end validation** of zonal intersection output (building stats, evacuation flagging, etc.).

---

## Appendix A — Key Environment Variables for Testing

```bash
# Builder/DB
PGPASSWORD=changeme-job
APP_PGPASSWORD=changeme-app

# Data loading
FEATURE_SET_CODES=ecumene,first_nations,highways,rail,facilities,buildings
BUILDING_WORKERS=0              # set to 2-4 to enable parallel import
BUILDING_ECUMENE_FILTER=0       # set to 1 to enable ECUMENE filtering (when implemented)

# Future: FireSTARR discovery
FIRESTARR_BLOB_URL=https://<account>.blob.core.windows.net/firestarr
AZURE_SAS_TOKEN=<sas>
```
