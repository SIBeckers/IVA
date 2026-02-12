# Impacted Values Analysis (IVA)

A geospatial analytics platform for analyzing fire impact on populated areas using [FireSTARR](https://cwfis.cfs.nrcan.gc.ca/) satellite data and polygon-based zonal statistics.

## Overview

IVA combines a **Python backend** for large-scale geospatial processing with a **React/OpenLayers frontend** for interactive visualization. The system:

- Downloads latest FireSTARR fire probability tiles from Azure Blob Storage
- Mosaics and reprojects tiles to a standard grid (EPSG:3978, 100m resolution)
- Computes zonal statistics for impact assessment across geographic features
- Stores results in PostGIS and serves them via a web map

**Key Features:**
- Automated blob discovery and tile management
- Parallel feature extraction using process pools
- Configurable feature sets and aggregation levels
- Interactive map visualization with layer controls
- Multi-region support (Canada-focused, extensible)

---

## Architecture

```
IVA/
├── app/iva-map/          # React + Vite frontend
│   └── src/
│       ├── App.tsx       # Main component
│       ├── layers.ts     # WMS/tile layer definitions
│       ├── basemap.ts    # Base map configuration
│       └── LayerControl.tsx  # Layer selection UI
├── job/iva_job/          # Python geospatial processor
│   ├── main.py           # Job orchestration
│   ├── firestarr.py      # Tile discovery & download
│   ├── stats.py          # Zonal statistics computation
│   ├── db.py             # PostgreSQL interface
│   └── loaders.py        # Feature data loading
├── db/                   # Database initialization
│   ├── ddl.sql           # Schema & views
│   ├── ddl_patch_views.sql  # Additional views (apply manually)
│   └── Dockerfile.db     # Custom PostGIS image with ogr_fdw
├── data/                 # GeoPackage datasets (GPKG)
│   ├── ab_structures_en.gpkg
│   ├── facilities.gpkg
│   ├── FirstNations.gpkg
│   ├── highways_v2.gpkg
│   └── ...
├── compose.yaml          # Docker Compose stack
└── scripts/
    └── init-db.sh        # Database initialization script
```

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+ (for local development)
- Node.js 18+ (for frontend development)
- Environment variables: `PGPASSWORD`, `APP_PGPASSWORD` (PostgreSQL)

### 1. Start the Stack

```bash
# Set credentials in .env or export them
export PGPASSWORD=your_pg_password
export APP_PGPASSWORD=your_app_password

# Bring up PostGIS, initialize DB, and start services
docker compose up -d
```

**Services started:**
- **PostGIS** (port 5432) — Geospatial database with `ogr_fdw` extension
- **pg_tileserv** (port 7800) — Vector tile server for map layers

### 2. Configure Environment

Create a `.env` file in the project root:

```bash
# PostgreSQL
PGPASSWORD=your_password
APP_PGPASSWORD=your_app_password

# Frontend (Vite)
VITE_TILESERV_BASE=http://localhost:7800
VITE_3978_XMIN=-2674572
VITE_3978_YMIN=-1040881
VITE_3978_XMAX=1604568
VITE_3978_YMAX=3101387

# Job processor
FIRESTARR_BLOB_URL=https://your_account.blob.core.windows.net/container
AZURE_SAS_TOKEN=?sv=2021-06-08&...    # Optional; required if not using managed identity
FIRESTARR_ROOT_PREFIX=firestarr        # Default folder prefix in blob storage
FIRESTARR_DISCOVERY=latest             # 'latest' (auto-discover) or 'template' (manual grid)
IVA_WORKERS=4                          # Process pool size (1 = sequential)
IVA_CHUNK_SIZE=250                     # Feature batch size for processing
IVA_EXCLUDE_FEATURE_SETS=buildings     # Comma-separated; 'buildings' excluded by default
```

### 3. Run the Frontend

```bash
cd app/iva-map
npm install
npm run dev
```

Navigate to `http://localhost:5173` (Vite default).

### 4. Run the Job Processor

```bash
cd job
pip install -r requirements.txt

# Process FireSTARR data and compute statistics
python -m iva_job.main
```

---

## Configuration Reference

### Frontend Environment Variables

| Variable | Purpose | Example |
|----------|---------|---------|
| `VITE_TILESERV_BASE` | pg_tileserv base URL | `http://localhost:7800` |
| `VITE_3978_XMIN`, `VITE_3978_YMIN`, `VITE_3978_XMAX`, `VITE_3978_YMAX` | Map extent (EPSG:3978) | See `.env` |

**Note:** Vite extent must match pg_tileserv's available tiles for proper rendering.

### Job Processor Environment Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `FIRESTARR_BLOB_URL` | URL | (required) | Azure Blob Storage container URL |
| `AZURE_SAS_TOKEN` | string | (optional) | SAS token starting with `?` (required if not using managed identity) |
| `FIRESTARR_ROOT_PREFIX` | string | `firestarr` | Root folder path in blob storage |
| `FIRESTARR_DISCOVERY` | enum | `latest` | `latest` = auto-discover tiles; `template` = use `FIRESTARR_GRID` |
| `FIRESTARR_GRID` | grid | `0,0;0,1;1,0;1,1` | Grid indices when `FIRESTARR_DISCOVERY=template` (format: `ix,iy;ix,iy;...`) |
| `IVA_WORKERS` | int | `1` | Number of worker processes (>1 enables parallel extraction) |
| `IVA_CHUNK_SIZE` | int | `250` | Number of features per processing batch |
| `IVA_EXCLUDE_FEATURE_SETS` | string | `buildings` | Comma-separated feature set names to skip |
| `PGPASSWORD` | string | (required) | PostgreSQL job user password |
| `APP_PGPASSWORD` | string | (required) | PostgreSQL app user password |

---

## Data Flow

1. **Tile Discovery** (`firestarr.py`)
   - Query Azure blob storage for latest FireSTARR run
   - Identify tiles matching target date + horizon
   - Download tiles to temporary directory

2. **Mosaic & Reproject** (`firestarr.py`)
   - Merge overlapping tiles (method: `max`)
   - Reproject to EPSG:3978 (Canada Albers Equal Area)
   - Resample to 100m regular grid
   - Output single GeoTIFF

3. **Zonal Statistics** (`stats.py`)
   - Load features from PostGIS or local GeoPackages
   - Extract raster values using zonal mask (`all_touched=True`)
   - Compute summaries (min, max, mean, percentiles)
   - Chunk processing for memory efficiency

4. **Storage** (`db.py`)
   - Insert run metadata
   - Upsert feature statistics by run and feature ID
   - Query via PostgreSQL views

5. **Visualization** (Frontend)
   - Fetch vector tiles from pg_tileserv
   - Display layer controls for feature sets
   - Render fire probability mosaic overlay
   - Show computed statistics on click

---

## Database Setup

### Initial Setup

```bash
# Compose stack automatically initializes on first run
docker compose up -d postgis initdb

# Wait for health checks (~30s)
docker compose logs postgis
```

### Apply Additional Views

After the initial DDL, apply optional views:

```bash
psql -h localhost -U iva_job -d impacted_values -f db/ddl_patch_views.sql
```

### Reset Database

```bash
# Stop and remove volume
docker compose down -v

# Restart (will reinitialize)
docker compose up -d
```

---

## Development Workflow

### Frontend Development

```bash
cd app/iva-map

# Install dependencies
npm install

# Start dev server with HMR
npm run dev

# Build for production
npm run build
```

**Technologies:** React 18, OpenLayers 10, Mapbox GL styles, Vite

### Job Processor Development

```bash
cd job

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run processor
python -m iva_job.main

# Run specific module
python -m iva_job.firestarr
```

**Key Modules:**
- `main.py` — Orchestration & logging
- `firestarr.py` — Tile management (discovery, download, mosaic)
- `stats.py` — Zonal statistics & aggregation
- `db.py` — PostgreSQL operations
- `loaders.py` — Feature data loading

### Database Development

Edit `db/ddl.sql` for schema changes, then:

```bash
# Rebuild database with new schema
docker compose down -v
docker compose up -d postgis initdb
```

---

## API & Integration Points

### PostGIS Queries

```sql
-- List computed statistics
SELECT * FROM iva_stats 
WHERE run_id = 'latest' 
ORDER BY feature_id;

-- Aggregated impacts by feature set
SELECT feature_set, COUNT(*), AVG(fire_prob_mean)
FROM iva_stats
GROUP BY feature_set;
```

### pg_tileserv Layers

Available as OGC vector tiles at:

```
http://localhost:7800/data/{layer}/{z}/{x}/{y}.pbf
```

**Example layers:** `buildings`, `facilities`, `highways`, etc. (depends on DDL views)

---

## Performance Considerations

- **Parallel Processing:** Set `IVA_WORKERS > 1` to process features in parallel (multiprocessing).
- **Exclusions:** Default excludes `buildings` (millions of objects). Use aggregates for large datasets.
- **Chunk Size:** Adjust `IVA_CHUNK_SIZE` based on memory; smaller chunks = more I/O, larger chunks = more memory.
- **Tile Download:** `discover_latest_tile_urls()` streams downloads in 1MB chunks.
- **Zonal Extraction:** Uses `rasterio.mask` with `all_touched=True` for accurate boundary pixel inclusion.

---

## Troubleshooting

### Database Connection Issues

```bash
# Test database connectivity
docker compose exec postgis psql -U iva_job -d impacted_values -c "SELECT version();"

# Check PostGIS extension
docker compose exec postgis psql -U iva_job -d impacted_values -c "SELECT postgis_version();"
```

### Tile Discovery Failures

- Verify `FIRESTARR_BLOB_URL` is correct
- Confirm `AZURE_SAS_TOKEN` is valid (if not using managed identity)
- Check blob storage for expected folder structure
- Review logs: `docker compose logs job`

### Map Rendering Issues

- Ensure `VITE_TILESERV_BASE` is reachable from browser
- Verify extent variables match pg_tileserv layer bounds
- Check browser console for tile loading errors

### Performance Degradation

- Monitor worker processes: `ps aux | grep python`
- Check disk space for temporary files: `/tmp/firestarr_*`
- Adjust `IVA_WORKERS` and `IVA_CHUNK_SIZE`

---

## Contributing

1. Create a feature branch
2. Make changes (follow docstring style in existing code)
3. Test locally with `docker compose`
4. Submit a pull request

---

## References

- [FireSTARR Data](https://cwfis.cfs.nrcan.gc.ca/)
- [PostGIS Documentation](https://postgis.net/documentation/)
- [Rasterio](https://rasterio.readthedocs.io/)
- [OpenLayers](https://openlayers.org/)
- [pg_tileserv](https://access.crunchydata.com/documentation/pg_tileserv/latest/)

---

## License

(Add your license here)

