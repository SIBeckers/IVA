
# job/iva_job/firestarr.py
import os
import tempfile
from datetime import date, timedelta
from typing import List
import logging
import requests
from pathlib import Path

import rasterio
from rasterio.merge import merge
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.enums import Resampling as ResamplingEnum

log = logging.getLogger("iva.firestarr")
logging.basicConfig(level=logging.INFO)

# Expected env:
# FIRESTARR_BLOB_URL = "https://<account>.blob.core.windows.net/firestarr"
# AZURE_SAS_TOKEN    = "?sv=...&sig=..."  (or AZURE_BLOB_KEY if you proxy via signed URLs)
# TILE_TEMPLATE      = "{prefix}/{ymd}/D{horizon}/tile_{ix}_{iy}.tif"
# where {ymd} is YYYYMMDD, {horizon} in {3,7}.
# You can adapt to your actual layout.

def _ymd(d: date) -> str:
    return d.strftime("%Y%m%d")

def build_blob_urls(run_date: date, horizon: int, prefix: str, grid: List[tuple]) -> List[str]:
    """Build blob URIs for a given date+horizon and a list of (ix,iy) tiles."""
    base = os.getenv("FIRESTARR_BLOB_URL", "").rstrip("/")
    sas  = os.getenv("AZURE_SAS_TOKEN", "")
    tmpl = os.getenv("TILE_TEMPLATE", "{prefix}/{ymd}/D{horizon}/tile_{ix}_{iy}.tif")
    urls = []
    for ix, iy in grid:
        path = tmpl.format(prefix=prefix, ymd=_ymd(run_date), horizon=horizon, ix=ix, iy=iy)
        url  = f"{base}/{path}{sas}"
        urls.append(url)
    return urls

def download_tiles(urls: List[str]) -> List[Path]:
    """Download a list of tile URLs to a temporary folder and return local paths."""
    tmpdir = Path(tempfile.mkdtemp(prefix="firestarr_"))
    local_paths = []
    for u in urls:
        fn = tmpdir / Path(u).name
        log.info(f"Downloading {u} -> {fn}")
        with requests.get(u, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(fn, "wb") as f:
                for chunk in r.iter_content(chunk_size=1<<20):
                    if chunk:
                        f.write(chunk)
        local_paths.append(fn)
    return local_paths

def mosaic_to_3979(tile_paths: List[Path], out_path: Path, dst_crs: str = "EPSG:3979", res_m: float = 100.0):
    """Mosaic GeoTIFF tiles and reproject to EPSG:3979 at ~100 m resolution."""
    if not tile_paths:
        raise ValueError("No tiles provided for mosaic")
    src_files = [rasterio.open(str(p)) for p in tile_paths]

    # Mosaic in source CRS
    mosaic, mosaic_transform = merge(src_files)
    src_meta = src_files[0].meta.copy()
    src_crs  = src_meta["crs"]
    for ds in src_files:
        ds.close()

    # Compute target transform / shape
    dst_transform, width, height = calculate_default_transform(
        src_crs, dst_crs, mosaic.shape[2], mosaic.shape[1], *rasterio.transform.array_bounds(
            mosaic.shape[1], mosaic.shape[2], mosaic_transform
        )
    )

    # Adjust resolution approximately (optional)
    # Note: calculate_default_transform chooses a default resolution; you can override if needed.

    dst_meta = src_meta.copy()
    dst_meta.update({
        "driver": "GTiff",
        "height": height,
        "width": width,
        "transform": dst_transform,
        "crs": dst_crs
    })

    log.info(f"Writing mosaic to {out_path} (CRS={dst_crs}, size={width}x{height})")
    with rasterio.open(out_path, "w", **dst_meta) as dst:
        for i in range(mosaic.shape[0]):  # bands
            reproject(
                source=mosaic[i],
                destination=rasterio.band(dst, i + 1),
                src_transform=mosaic_transform,
                src_crs=src_crs,
                dst_transform=dst_transform,
                dst_crs=dst_crs,
                resampling=Resampling.bilinear
            )

def get_run_day(run_date: date, horizon: int) -> date:
    """Compute the TIME date used by the horizon (e.g., D3 -> run_date + 2)."""
    return run_date + timedelta(days=horizon - 1)
