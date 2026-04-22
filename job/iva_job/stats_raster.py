from __future__ import annotations

import numpy as np
import rasterio
from rasterio.features import geometry_mask, geometry_window
from rasterio.windows import bounds as window_bounds
from rasterio.windows import from_bounds, transform as window_transform


def building_zone_metrics(
    fire_ds: rasterio.io.DatasetReader,
    bld_ds: rasterio.io.DatasetReader,
    geom_obj: dict,
) -> tuple[int, float]:
    """
    Compute building metrics for a single zone geometry.

    Returns:
      (building_count, expected_buildings)

    building_count:
      Sum of building-count pixels intersecting the geometry
      within the FireSTARR raster extent.

    expected_buildings:
      Sum(building_count * fire_probability) over intersecting pixels.
    """
    try:
        # Define the working window from the FireSTARR raster only.
        fire_window = geometry_window(fire_ds, [geom_obj], pad_x=0, pad_y=0)
    except ValueError:
        # Geometry does not intersect FireSTARR extent.
        return 0, 0.0

    fire = np.ma.asarray(
        fire_ds.read(1, window=fire_window, masked=True),
        dtype="float64",
    )

    if fire.size == 0:
        return 0, 0.0

    fire_transform = window_transform(fire_window, fire_ds.transform)

    # Build one common geometry mask on the FireSTARR grid/window.
    inside_geom = geometry_mask(
        [geom_obj],
        transform=fire_transform,
        invert=True,
        out_shape=fire.shape,
        all_touched=True,
    )

    # Read the building raster over the exact same geographic bounds.
    left, bottom, right, top = window_bounds(fire_window, fire_ds.transform)
    bld_window = from_bounds(left, bottom, right, top, transform=bld_ds.transform)
    bld_window = bld_window.round_offsets().round_lengths()

    bld = np.ma.asarray(
        bld_ds.read(1, window=bld_window, masked=True),
        dtype="float64",
    )

    # Defensive check: if shapes still differ, fail loudly with detail.
    if fire.shape != bld.shape:
        raise RuntimeError(
            f"Fire/building window shape mismatch: "
            f"fire.shape={fire.shape}, bld.shape={bld.shape}, "
            f"fire_window={fire_window}, bld_window={bld_window}"
        )

    fire_mask = np.ma.getmaskarray(fire)
    bld_mask = np.ma.getmaskarray(bld)

    fire_filled = fire.filled(np.nan)
    bld_filled = bld.filled(np.nan)

    valid = (
        inside_geom
        & (~fire_mask)
        & (~bld_mask)
        & ~np.isnan(fire_filled)
        & ~np.isnan(bld_filled)
    )

    if not np.any(valid):
        return 0, 0.0

    building_count = int(np.sum(bld_filled[valid]))
    expected_buildings = float(np.sum(bld_filled[valid] * fire_filled[valid]))

    return building_count, expected_buildings
