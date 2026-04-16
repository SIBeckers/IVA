"""
Raster-first aggregation: multiply FireSTARR × building-count rasters,
then compute zonal stats over polygon features.

Replaces per-building vector → raster masking with pure raster operations.
"""

from pathlib import Path
import logging
import numpy as np
import rasterio
from rasterio.mask import mask
import json
from .db import upsert_feature_stats


log = logging.getLogger("iva.stats_raster")


def _weighted_raster_path(base_dir: Path, run_date, horizon: int) -> Path:
    """
    Intermediate: FireSTARR probability × building-count raster.
    Stored in tmp; used only during this run's zonal stats.
    """
    from datetime import date
    ymd = run_date.strftime("%Y%m%d") if isinstance(run_date, date) else str(run_date)
    return base_dir / f"firestarr_{ymd}_day_{int(horizon):02d}_weighted.tif"


def create_weighted_raster(
    firestarr_path: Path,
    building_count_path: Path,
    out_path: Path,
) -> Path:
    """
    Multiply FireSTARR probability × building count per cell.
    
    Output: same grid/CRS as firestarr_path, values = probability × count.
    Used for zonal stats aggregation.
    
    Robust to:
      - Misaligned grids: handled by masking both to common bounds
      - Nodata/NaN: propagates as nodata in output
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(firestarr_path) as fs_ds:
        if fs_ds.count != 1:
            raise ValueError(f"FireSTARR must be single-band; got {fs_ds.count}")
        if str(fs_ds.crs) not in ("EPSG:3978", "EPSG:3978:"):
            raise ValueError(f"FireSTARR must be EPSG:3978; got {fs_ds.crs}")

        fs_profile = fs_ds.profile.copy()
        fs_data = fs_ds.read(1)
        fs_nodata = fs_ds.nodata

    with rasterio.open(building_count_path) as bc_ds:
        if bc_ds.count != 1:
            raise ValueError(f"Building count must be single-band; got {bc_ds.count}")

        # Reproject/resample building_count to match FireSTARR if needed
        if (str(bc_ds.crs) != str(fs_ds.crs) or
            bc_ds.bounds != fs_ds.bounds or
            bc_ds.transform != fs_ds.transform):
            from rasterio.vrt import WarpedVRT
            from rasterio.warp import Resampling
            
            with WarpedVRT(
                bc_ds,
                crs=fs_ds.crs,
                resampling=Resampling.nearest,  # preserve count integers
                transform=fs_ds.transform,
                width=fs_ds.width,
                height=fs_ds.height,
                nodata=0,
            ) as vrt:
                bc_data = vrt.read(1)
        else:
            bc_data = bc_ds.read(1)
        
        bc_nodata = bc_ds.nodata or 0

    # Multiply: probability × count
    # Where either is nodata, result is nodata
    weighted = np.zeros_like(fs_data, dtype="float32")
    
    valid_mask = ((fs_data != fs_nodata) & (fs_data != np.nan) &
                  (bc_data != bc_nodata) & (bc_data != np.nan))
    
    weighted[valid_mask] = fs_data[valid_mask].astype("float32") * bc_data[valid_mask].astype("float32")
    weighted[~valid_mask] = fs_nodata if fs_nodata is not None else np.nan

    # Write output
    profile = fs_profile.copy()
    profile.update(dtype="float32", nodata=fs_nodata)
    
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(weighted, 1)

    log.info("Created weighted raster: %s", out_path)
    return out_path


def zonal_stats_raster(
    weighted_raster_path: Path,
    geom_obj,
    all_touched: bool = True,
) -> dict:
    """
    Extract weighted building-probability values under geometry,
    compute stats over all valid pixels.
    
    Returns: dict with keys n, v_min, p05, p25, p50, v_mean, p75, p95, v_max.
    """
    from .stats import summarize

    try:
        with rasterio.open(weighted_raster_path) as ds:
            out, _ = mask(
                ds,
                [geom_obj],
                crop=True,
                all_touched=all_touched,
                filled=False,
                pad=True,
                pad_width=0.5,
            )
    except ValueError:
        # No overlap with raster
        return summarize(np.array([], dtype="float64"))

    band = out[0]
    vals = band.compressed() if hasattr(band, "compressed") else np.asarray(band).ravel()
    
    if vals.size == 0:
        return summarize(np.array([], dtype="float64"))

    vals = vals.astype("float64", copy=False)
    nodata = ds.nodata
    if nodata is not None:
        vals = vals[vals != nodata]
    vals = vals[~np.isnan(vals)]

    return summarize(vals)


def compute_building_counts(cur, building_count_raster_path, zones, run_id):
    """
    Compute the total number of buildings intersected by zones using the building count raster.

    Args:
        cur: Database cursor for inserting stats.
        building_count_raster_path: Path to the building count raster.
        zones: List of zones (geometry in GeoJSON format).
        run_id: ID of the current run.

    Returns:
        None
    """
    with rasterio.open(building_count_raster_path) as ds:
        for zone in zones:
            feature_id = zone['feature_id']
            geom = zone['geometry']

            try:
                out_image, _ = mask(ds, [geom], crop=True, all_touched=True, filled=False)
                building_counts = out_image[0]

                # Filter out nodata and NaN values
                valid_counts = building_counts[(building_counts != ds.nodata) & ~np.isnan(building_counts)]

                # Compute the total building count
                total_buildings = valid_counts.sum()

                if total_buildings > 0:
                    stats = {
                        'n': len(valid_counts),
                        'sum': total_buildings
                    }
                    upsert_feature_stats(cur, run_id, feature_id, stats, evacuated=False)

            except ValueError:
                # Handle cases where the zone does not intersect the raster
                continue