
import proj4 from 'proj4';
import { register } from 'ol/proj/proj4';
import { get as getProjection } from 'ol/proj';
import TileGrid from 'ol/tilegrid/TileGrid';
import type Projection from 'ol/proj/Projection';

// EPSG:3978 (NAD83 / Canada Atlas Lambert)
proj4.defs(
  'EPSG:3978',
  '+proj=lcc +lat_0=49 +lon_0=-95 +lat_1=49 +lat_2=77 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +type=crs'
);
register(proj4);

function envNum(key: string): number | null {
  const v = (import.meta as any).env?.[key];
  if (v === undefined || v === null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

// Prefer VITE-provided extents so client tile grid matches pg_tileserv TS_COORDINATESYSTEM_*.
export const EXTENT_3978: [number, number, number, number] = (() => {
  const xmin = envNum('VITE_3978_XMIN');
  const ymin = envNum('VITE_3978_YMIN');
  const xmax = envNum('VITE_3978_XMAX');
  const ymax = envNum('VITE_3978_YMAX');
  if (xmin !== null && ymin !== null && xmax !== null && ymax !== null) {
    return [xmin, ymin, xmax, ymax];
  }
  // Fallback Canada-wide extent
  return [-2421618.74911166, -774687.715795296, 3062991.079527088, 4704011.872167985];
})();

const p = getProjection('EPSG:3978');
if (!p) {
  throw new Error('EPSG:3978 projection not registered (getProjection returned null).');
}
export const proj3978: Projection = p;
proj3978.setExtent(EXTENT_3978);

function buildResolutions(extent: [number, number, number, number], tileSize = 256, maxZoom = 22) {
  const width = extent[2] - extent[0];
  const res0 = width / tileSize; // zoom 0: 1 tile covers full extent width
  const resolutions: number[] = [];
  for (let z = 0; z <= maxZoom; z++) {
    resolutions.push(res0 / Math.pow(2, z));
  }
  return resolutions;
}

// Explicit tile grid to match pg_tileserv coordinate system extent and an XYZ-like pyramid.
export const tileGrid3978 = (() => {
  const tileSize = 256;
  const maxZoom = 22;
  const origin: [number, number] = [EXTENT_3978[0], EXTENT_3978[3]]; // top-left
  const resolutions = buildResolutions(EXTENT_3978, tileSize, maxZoom);
  return new TileGrid({ extent: EXTENT_3978, origin, resolutions, tileSize });
})();
