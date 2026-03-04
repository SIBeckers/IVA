# IVA Next Session Checklist

> Date: 2026-02-24 (updated from 2026-01-28)  
> Priority: **Validate building population, complete firestarr.py integration, run full pipeline end-to-end.**

---

## Phase 1: Verify & Complete Building Population (🔴 CRITICAL PATH)

### 1.1 Confirm building data is loading
**Goal:** Verify `risk.features` table has millions of building records from provincial GPKGs.

- [ ] Start full stack: `podman compose up -d`
- [ ] Wait for `initdb` to complete (watch logs: `podman compose logs initdb --tail 50`)
- [ ] Run diagnostic query:
  ```sql
  SELECT COUNT(*) as total, 
         COUNT(CASE WHEN feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='buildings') THEN 1 END) as buildings
  FROM risk.features;
  ```
  **Expected:** buildings count > 1,000,000 (estimate 5–10M across Canada).

- [ ] If count is 0:
  - Tail iva-job logs: `podman compose logs iva-job --tail 200 | grep -i "loading\|error\|building\|processed"`
  - Check for: connection errors, file not found, multiprocessing issues
  - Re-read IVA_SESSION_LOG_2026-02-24.md section "6) Known Issues"

### 1.2 Verify buildings are accessible via views
**Goal:** Confirm pgfeatureserv and pg_tileserv can query buildings.

- [ ] Query raw view: `SELECT COUNT(*), ST_Extent(geom) FROM risk.v_features_buildings_raw;`
  **Expected:** Non-zero count, geometry extent over Canada
- [ ] Spot-check: `SELECT id, source_pk, name FROM risk.v_features_buildings_raw LIMIT 3;`
- [ ] Verify pg_tileserv: `curl -s http://localhost:7800/data/risk.v_features_buildings_raw | grep -i "geometry_type"`
  **Expected:** Layer metadata (no 404)

### 1.3 Complete ECUMENE filtering (optional)
**Goal:** If desired, load only buildings NOT inside ECUMENE polygons.

- [ ] Decide: **Is ECUMENE filtering needed?** If yes, continue. If no, skip to 1.4.
- [ ] Complete `job/iva_job/loaders.py`:
  - [ ] Load ECUMENE GeoDataFrame once before building loop
  - [ ] Add `BUILDING_ECUMENE_FILTER=1` env support
  - [ ] Integrate `_load_with_ecumene_filter()` into main flow
  - [ ] Test with AB buildings only first
  - [ ] Verify row count reduction
- [ ] Set env var and re-run: `BUILDING_ECUMENE_FILTER=1 python -m iva_job.loaders /data`

### 1.4 Parallel import stress test (optional)
**Goal:** If loading is slow, test parallel import performance.

- [ ] Set `BUILDING_WORKERS=2` in `.env`
- [ ] Rebuild and run: `podman compose build --no-cache iva-job && podman compose up -d iva-job`
- [ ] Monitor: `podman compose logs iva-job -f | grep -i "processed\|completed"`
- [ ] Compare wall-clock time vs. `BUILDING_WORKERS=0`
- [ ] **Decision:** Keep parallel if speedup > 20%; otherwise revert

---

## Phase 2: FireSTARR Data Pipeline (Prerequisite for Zonal Stats)

### 2.1 Validate firestarr.py blob discovery
**Goal:** Confirm we can list and select FireSTARR tiles from Azure Blob Storage.

- [ ] Set up `.env`:
  ```bash
  FIRESTARR_BLOB_URL=https://<YOUR_ACCOUNT>.blob.core.windows.net/firestarr
  AZURE_SAS_TOKEN=<YOUR_SAS_TOKEN>
  FIRESTARR_DISCOVERY=latest
  ```
- [ ] Test blob listing (if you have Python shell):
  ```bash
  python -c "from iva_job.firestarr import build_blob_urls; print(build_blob_urls(date(2026,2,24), 3, 'canada', [(0,0)]))"
  ```
  **Expected:** List of tile URLs or success message
- [ ] If Azure creds not available, **use mock/stub data:**
  - Add a pre-downloaded GeoTIFF to `/data` or blob
  - Modify `build_blob_urls()` to optionally return local paths
  - Proceed with testing using mock data

### 2.2 Validate firestarr.py tile download
**Goal:** Confirm downloading and reprojecting tiles works correctly.

- [ ] Test `download_tiles()` with one URL:
  ```bash
  python -c "from iva_job.firestarr import download_tiles; paths = download_tiles(['<URL>']); print(paths)"
  ```
  **Expected:** List of local `.tif` paths
- [ ] Verify raster: `gdalinfo <tile_path> | grep -i "size\|srid\|pixel"`
  **Expected:** Reasonable extent, values, projection hint

### 2.3 Validate mosaic and grid snapping
**Goal:** Confirm mosaic uses MAX aggregation and output is snapped to 100 m EPSG:3978 grid.

- [ ] Test `mosaic_to_grid()` with 1–2 tiles:
  ```bash
  python -c "
  from iva_job.firestarr import mosaic_to_grid
  mosaic_to_grid(['<TILE1>', '<TILE2>'], '/tmp/test_mosaic.tif', dst_epsg=3978, res_m=100, mosaic_method='max')
  "
  gdalinfo /tmp/test_mosaic.tif | grep -E "Pixel Size|EPSG"
  ```
  **Expected:**
  - Pixel size: 100 × 100 m (exactly)
  - EPSG code: 3978
  - Extent snapped to grid (origin + N×100 m)

### 2.4 Validate zonal extraction (main.py integration)
**Goal:** Confirm zonal stats are computed correctly for buildings and other features.

- [ ] Ensure `.env` includes:
  ```bash
  FEATURE_SET_CODES=ecumene,first_nations,highways,rail,facilities,buildings
  IVA_EXCLUDE_FEATURE_SETS=        # empty = include all, including buildings
  ```
- [ ] Run: `python -m iva_job.main` (or start iva-job container)
- [ ] **Check outcomes:**
  - [ ] New row in `risk.runs` (run_date, forecast_day, wmstime, blob_uris)
  - [ ] Rows in `risk.feature_stats` for all features
  - [ ] All fields (n, v_min, p05, p25, p50, v_mean, p75, p95, v_max) are non-null
  - [ ] Log output: `Upserted stats for 12345 features (run_id=..., horizon=3, ...)`
- [ ] Spot-check stats:
  ```sql
  SELECT fs.code, COUNT(*) as cnt, AVG(s.n) as avg_n, MIN(s.v_min), MAX(s.v_max)
  FROM risk.feature_stats s
  JOIN risk.features f ON s.feature_id = f.id
  JOIN risk.feature_sets fs ON f.feature_set_id = fs.id
  WHERE s.run_id = (SELECT id FROM risk.runs ORDER BY id DESC LIMIT 1)
  GROUP BY fs.code;
  ```
  **Expected:** Buildings row with large `n` values (if included), non-building sets with valid probability ranges

### 2.5 Validate evacuation flagging (optional)
**Goal:** Confirm buildings/features inside evacuation zones are flagged.

- [ ] Set `EVAC_WFS_URL` in `.env` (requires WFS endpoint)
- [ ] Re-run main.py
- [ ] Query:
  ```sql
  SELECT fs.code, COUNT(*) as total, COUNT(CASE WHEN evacuated THEN 1 END) as evac_count
  FROM risk.feature_stats s
  JOIN risk.features f ON s.feature_id = f.id
  JOIN risk.feature_sets fs ON f.feature_set_id = fs.id
  WHERE s.run_id = (SELECT id FROM risk.runs ORDER BY id DESC LIMIT 1)
  GROUP BY fs.code;
  ```
  **Expected:** Some features with `evacuated=true`
- [ ] If WFS not available, skip (it's nice-to-have)

---

## Phase 3: Web Map & UI Integration

### 3.1 Verify buildings layer in pg_tileserv
**Goal:** Ensure tiles are rendered at http://localhost:7800/data/risk.v_features_buildings_raw/

- [ ] Open browser: `http://localhost:7800/data/risk.v_features_buildings_raw/8/128/256.pbf`
- [ ] Try a few z/x/y combinations
- [ ] Check response size (should be non-trivial if buildings exist)
- [ ] If empty/404, verify view exists: `SELECT * FROM risk.v_features_buildings_raw LIMIT 1;`

### 3.2 Verify buildings layer in React frontend
**Goal:** Confirm React map includes buildings layer in layer control.

- [ ] Check `app/iva-map/src/layers.ts` has buildings entry
- [ ] Start web app: `cd app/iva-map && npm run dev`
- [ ] Open http://localhost:5173
- [ ] Verify "Buildings" checkbox in Overlays section
- [ ] Toggle on/off and confirm features appear/disappear
- [ ] Zoom/pan and verify tiles load at multiple zoom levels

### 3.3 Test per-horizon views (d3/d7)
**Goal:** Verify d3/d7 layers display and filter by `is_new` flag.

- [ ] Confirm views exist: `SELECT COUNT(*) FROM risk.v_latest_buildings_d3;`
- [ ] Check `is_new` distribution:
  ```sql
  SELECT forecast_day, COUNT(*) as total, COUNT(CASE WHEN is_new THEN 1 END) as new_count
  FROM risk.v_latest_feature_stats_with_prev
  WHERE feature_set_id = (SELECT id FROM risk.feature_sets WHERE code='buildings')
  GROUP BY forecast_day;
  ```
- [ ] (Optional) Add d3/d7 buildings layers to React frontend and test

---

## Phase 4: Checkpoint & Planning

### 4.1 Verify full pipeline end-to-end
**Success criteria:**
- [ ] Building count: > 100,000
- [ ] Building stats: Latest run has non-null n, quantiles, mean/max
- [ ] Buildings raw layer: Query works, pg_tileserv serves tiles, React shows features
- [ ] Forecasting: D3 and D7 both complete
- [ ] Evacuation flagging: Works (if enabled)

### 4.2 Document findings
- [ ] Note any discrepancies vs. justTheIntersection.R (if reference available)
- [ ] List any new blockers or performance concerns
- [ ] Update this checklist with outcomes

### 4.3 Commit & tag
- [ ] Commit changes: `git add -A && git commit -m "Complete buildings loading & firestarr validation"`
- [ ] Tag: `git tag v0.3-buildings-firestarr-complete`

---

## Quick Reference

| Item | Path / Command |
|------|----------------|
| Main loader | `job/iva_job/loaders.py` |
| FireSTARR module | `job/iva_job/firestarr.py` |
| Main job | `job/iva_job/main.py` |
| Buildings raw view | `risk.v_features_buildings_raw` |
| Buildings d3/d7 views | `risk.v_latest_buildings_d3/d7` |
| React frontend | `app/iva-map/src/` |
| Compose config | `compose.yaml` |
| Build & run | `podman compose up -d` |
| Monitor logs | `podman compose logs <service> -f` |
| DB query | `podman compose exec -T postgis psql -U iva_job -d impacted_values -c "..."` |

