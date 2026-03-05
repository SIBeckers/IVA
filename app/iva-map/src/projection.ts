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

// --- View extent (UI pan/zoom constraint) ---
export const EXTENT_3978: [number, number, number, number] = (() => {
  const xmin = envNum('VITE_3978_XMIN');
  const ymin = envNum('VITE_3978_YMIN');
  const xmax = envNum('VITE_3978_XMAX');
  const ymax = envNum('VITE_3978_YMAX');
  if (xmin !== null && ymin !== null && xmax !== null && ymax !== null) {
    return [xmin, ymin, xmax, ymax];
  }
  // Canada-wide fallback
  return [-2421618.74911166, -774687.715795296, 3062991.079527088, 4704011.872167985];
})();


export const TILE_SIZE_3978 = 512;


export const GRID_ORIGIN_3978: [number, number] = [-34655613.47869982, 38474944.64475933];


export const GRID_RESOLUTIONS_3978: number[] = [
  135373.49015117117,
  67686.74507558558,
  33843.37253779279,
  16921.686268896396,
  8460.843134448198,
  4230.421567224099,
  2115.2107836120495,
  1057.6053918060247,
  528.8026959030124,
  264.4013479515062,
  132.2006739757531,
  66.10033698787655,
  33.05016849393827,
  16.525084246969136,
  8.262542123484568,
  4.131271061742284,
  2.065635530871142,
  1.032817765435571,
  0.5164088827177855,
  0.25820444135889276,
  0.12910222067944638,
  0.06455111033972319,
  0.032275555169861594,
  0.016137777584930797,
];

// Square zoom=0 tile extent implied by origin + tileSize*res0
const z0Width = TILE_SIZE_3978 * GRID_RESOLUTIONS_3978[0];
export const GRID_EXTENT_3978: [number, number, number, number] = [
  GRID_ORIGIN_3978[0],
  GRID_ORIGIN_3978[1] - z0Width,
  GRID_ORIGIN_3978[0] + z0Width,
  GRID_ORIGIN_3978[1],
];

const p = getProjection('EPSG:3978');
if (!p) throw new Error('EPSG:3978 projection not registered.');
export const proj3978: Projection = p;

// Use the grid extent as projection extent so OL has a defined “world”
proj3978.setExtent(GRID_EXTENT_3978);

export const tileGrid3978 = new TileGrid({
  extent: GRID_EXTENT_3978,
  origin: GRID_ORIGIN_3978,
  resolutions: GRID_RESOLUTIONS_3978,
  tileSize: TILE_SIZE_3978,
});

export const proj3857 = getProjection('EPSG:3857')!;