# IVA Session Log — 2026-01-28

> Purpose: Preserve a detailed, shareable record of what we reviewed, what we found, what we changed, and how to resume next time.

## 1) Context & Goals

- **Primary goal:** Ensure the IVA stack reproduces the functionality of `justTheIntersection.R`, but in the IVA architecture (PostGIS + pg_tileserv/pg_featureserv + React/OpenLayers).  
- **User requirements confirmed:**
  - Toggle **per theme** (Ecumene / First Nations / Facilities / Highways / Rail, plus buildings via aggregates).  
  - **Strict raster parity** with the R workflow: **MAX mosaic**, **NEAREST resampling**, **EXACT snapped 100 m grid**, output in **EPSG:3978**.  
  - **Buildings are polygons** and **millions** of features: do *not* publish a per-building layer by default; instead support **drill-down from aggregation layers**.
  - FireSTARR “latest folder” discovery should mimic the **R blob listing logic**.
  - Layer control should be **grouped**; **basemap uses radio** selection; other layers use checkboxes.

## 2) Files Reviewed / Analyzed

### 2.1 Reference R workflow

- `justTheIntersection.R` (uploaded as `justTheIntersection.txt`) was treated as the **parity source of truth**:
  - Downloads FireSTARR tiles from Azure Blob.
  - Projects tiles to EPSG:3978 at 100 m and mosaics using `max`.
  - Computes zonal statistics using `terra::extract(... touches=TRUE, fun=interpolationStats)`.
  - Adds “new since yesterday” by comparing IDs to prior output.
  - Flags “evacuated” using a WFS evacuation feed buffered by 2500 m and intersected with features.
  - Writes CSV/GPKG outputs and Leaflet HTML maps.

### 2.2 IVA stack wiring

- `compose.yaml`:
  - PostGIS + `ogr_fdw` image build.
  - `initdb` one-shot applies `db/ddl.sql` with password variables.
  - `pg_tileserv` configured for **SRID 3978** and explicit extent via `TS_COORDINATESYSTEM_*` env vars.
  - `pg_featureserv` for feature inspection.
  - `iva-job` runs `python -m iva_job.main`.

### 2.3 Job pipeline

- `job/iva_job/main.py` (pre-change): orchestrated horizons (D3/D7) and inserted runs/stats but **wrote empty stats** (`summarize(np.array([]))`).
- `job/iva_job/firestarr.py` (pre-change):
  - Used deterministic URL template / fixed grid.
  - Downloaded tiles via HTTP.
  - Mosaicked without explicit `max` method.
  - Reprojected with **bilinear** resampling.
  - Did not enforce exact 100 m grid.
- `job/iva_job/stats.py`:
  - `summarize()` matches the R `interpolationStats()` output fields: n, min, p05/p25/p50/p75/p95, mean, max.
- `job/iva_job/db.py`:
  - Run upsert existed, but conflict update originally only updated `blob_uris` (metadata drift risk).

### 2.4 Frontend map app

- `app/iva-map/src/basemap.ts`:
  - CBMT ArcGIS VectorTileServer basemap uses **tileInfo-derived TileGrid** (origin + resolutions + tileSize), which is the correct fix for VTS misalignment.
- `app/iva-map/src/projection.ts`:
  - EPSG:3978 projection registered and a tile grid exists; alignment depends on matching pg_tileserv grid assumptions.
- `app/iva-map/src/App.tsx`, `LayerControl.tsx`, `layers.ts`:
  - Initially some uploads appeared HTML-escaped; later the real contents were pasted.
  - Original `layers.ts` used `risk.v_feature_stats_d3/d7` (not truly “latest”) and a geometry-inappropriate style for mixed geometry types.

## 3) Key Findings (Gaps & Root Causes)

### 3.1 Parity gaps vs `justTheIntersection.R`

1. **No real zonal stats stored:** job inserted placeholder/empty stats.
2. **Raster parity mismatch:** mosaic/resample/grid did not match R’s `max + near + 100 m` behavior.
3. **FireSTARR run discovery mismatch:** R discovers latest publish folder by listing blobs; Python assumed fixed layout.
4. **UI/data semantics mismatch:** D3/D7 “values” layers were not restricted to latest runs and mixed geometry types.

### 3.2 Basemap vs overlays misalignment hypothesis

- CBMT basemap uses ArcGIS `tileInfo` grid (correct).
- Overlays from pg_tileserv must use the same **extent + origin + resolution ladder** as pg_tileserv’s coordinate system.
- If the client tile grid differs from the tileserv grid definition, drift/misalignment appears across zoom levels.

## 4) Delivered Changes

Two update packs were produced during the session.

### 4.1 Update pack v1 (superseded)

- `IVA_update_pack_2026-01-28.zip` introduced strict mosaic parity, latest-per-theme views, and per-theme layers.

### 4.2 Update pack v2 (current recommended)

**Archive:** `IVA_update_pack_2026-01-28_v2.zip`

#### 4.2.1 Frontend updates

- **Grouped Layer Control:**
  - `app/iva-map/src/LayerControl.tsx` now groups overlay layers by group name and uses **radio buttons** for basemaps and **checkboxes** for overlays.
- **App wiring for basemap radio:**
  - `app/iva-map/src/App.tsx` supports basemap selection (radio behavior) and keeps OL layer visibility synchronized.
- **Per-theme layers & building strategy:**
  - `app/iva-map/src/layers.ts` publishes per-theme D3/D7 layers (Ecumene/FN/Facilities/Highways/Rail) and **buildings only via aggregate choropleths** (Buildings→CSD/Ecumene/FN). No per-building layer by default.
- **Tile grid alignment:**
  - `app/iva-map/src/projection.ts` builds a deterministic EPSG:3978 tile grid from `VITE_3978_XMIN/YMIN/XMAX/YMAX` (to match pg_tileserv extent).

#### 4.2.2 Backend/job updates

- **Strict raster parity implemented:**
  - `job/iva_job/firestarr.py` enforces:
    - **MAX** mosaic
    - **NEAREST** resampling
    - **EXACT snapped 100 m grid**
    - output CRS **EPSG:3978**
- **FireSTARR latest-folder discovery (R parity):**
  - `job/iva_job/firestarr.py` adds Azure Blob container listing via REST `comp=list` (XML), chooses latest prefix under `firestarr/`, selects the run folder containing target date `YYYYMMDD` (run_date + horizon - 1), and downloads `.tif` tiles.
- **Performance controls:**
  - `job/iva_job/main.py`:
    - Default excludes buildings with `IVA_EXCLUDE_FEATURE_SETS=buildings`.
    - Adds optional multiprocessing via `IVA_WORKERS` and chunking via `IVA_CHUNK_SIZE`.
    - Keeps `all_touched=True` behavior for zonal extraction equivalence.
    - Optional evac flagging when `EVAC_WFS_URL` is provided.

#### 4.2.3 Database patch

- `db/ddl_patch_views.sql` adds:
  - `risk.v_prev_runs`
  - `risk.v_latest_feature_stats_with_prev` (derives `is_new` from presence/absence in prior run)
  - per-theme/per-horizon views (`risk.v_latest_ecumene_d3`, etc.) with geometry cast/transform to EPSG:3978
  - grants to `iva_app`

## 5) How to Apply (Implementation Checklist)

1. **Apply v2 zip files** into repo paths.
2. **Apply SQL patch** after base schema is created:

```bash
psql -h localhost -U iva_job -d impacted_values -f db/ddl_patch_views.sql
```

3. **Set Vite env to match pg_tileserv extent** (must match `TS_COORDINATESYSTEM_*` used in `compose.yaml`):

```bash
VITE_3978_XMIN=...
VITE_3978_YMIN=...
VITE_3978_XMAX=...
VITE_3978_YMAX=...
VITE_TILESERV_BASE=http://localhost:7800
```

4. **Set job env vars** (key ones):

```bash
FIRESTARR_BLOB_URL=https://<account>.blob.core.windows.net/firestarr
AZURE_SAS_TOKEN=?sv=...&sig=...
FIRESTARR_ROOT_PREFIX=firestarr
FIRESTARR_DISCOVERY=latest
IVA_EXCLUDE_FEATURE_SETS=buildings
IVA_WORKERS=1
IVA_CHUNK_SIZE=250
# Optional
EVAC_WFS_URL=https://.../wfs?...&outputFormat=application/json
```

5. **Restart stack**:

```bash
docker compose down
docker compose up --build
```

## 6) Validation Plan

After applying v2, validate in this order:

1. **Basemap/overlay alignment:**
   - Verify CBMT basemap aligns with CSD and per-theme overlays at multiple zoom levels.
   - If misalignment persists, check that Vite extent matches pg_tileserv extent exactly.
2. **Job output correctness:**
   - Confirm `risk.runs` has new rows for D3 and D7.
   - Confirm `risk.feature_stats` has non-null stats (n, quantiles, mean/max) for non-building feature sets.
3. **Evacuation flagging (if enabled):**
   - Confirm some features have `evacuated=true` when evac feed intersects.
4. **Layer control UX:**
   - Basemap is radio-select.
   - Overlays are grouped checklists.

## 7) Next Steps (Planned)

### 7.1 Buildings drill-down

- Implement drill-down so per-building stats are computed only when user selects an aggregate polygon (CSD/Ecumene/FN) or a map ROI.
- Prefer a **cache-first** strategy:
  - first request computes + stores results keyed by (run_id, region key)
  - subsequent requests serve immediately

### 7.2 Performance hardening

- Tune `IVA_WORKERS` and chunk sizes for the non-building sets.
- Consider spatial indexing / raster window optimizations.
- Avoid ever generating full-Canada per-building stats unless required.

---

## Appendix A — Files included/changed by v2 pack

- `app/iva-map/src/LayerControl.tsx`
- `app/iva-map/src/App.tsx`
- `app/iva-map/src/layers.ts`
- `app/iva-map/src/projection.ts`
- `job/iva_job/firestarr.py`
- `job/iva_job/main.py`
- `db/ddl_patch_views.sql`
- `README_UPDATES_V2.md`

