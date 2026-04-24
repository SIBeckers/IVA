// app/iva-map/src/layers.ts
import VectorTileLayer from 'ol/layer/VectorTile';
import VectorTileSource from 'ol/source/VectorTile';
import MVT from 'ol/format/MVT';
import type BaseLayer from 'ol/layer/Base';
import { Style, Stroke, Fill } from 'ol/style';
import { proj3857, proj3978, tileGrid3978 } from './projection';

export type LayerKind = 'base' | 'overlay';

export type LayerEntry = {
  id: string;
  layer: BaseLayer;
  visible: boolean;
  label: string;
  group: string;
  kind: LayerKind;
};

const TILESERV = import.meta.env.VITE_TILESERV_BASE ?? 'http://localhost:7800';

// Toggle this to true temporarily to see feature counts in console
const DEBUG_TILE_COUNTS = true;

const strokeThin = (color: string, w = 1) => new Stroke({ color, width: w });
const stylePoly = (fillColor: string, strokeColor = '#333', w = 0.8) =>
  new Style({ fill: new Fill({ color: fillColor }), stroke: strokeThin(strokeColor, w) });
const styleLine = (color: string, w = 1.8) => new Style({ stroke: strokeThin(color, w) });

const csdStyle = stylePoly('rgba(34,139,34,0.22)', '#2e8b57', 2.0); // slightly stronger for visibility
const ecumeneBase = stylePoly('rgba(50,205,50,0.15)', '#2e8b57', 0.8);
const fnBase = stylePoly('rgba(148,0,211,0.15)', '#6a0dad', 0.8);
const facilitiesBase = stylePoly('rgba(31,120,180,0.12)', '#0c3a66', 1.0);

const highwaysBase = styleLine('#f1b814', 2.2);
const railBase = styleLine('#666', 2.2);

function rampFill(v: number) {
  if (v >= 0.6) return 'rgba(228,26,28,0.45)';
  if (v >= 0.2) return 'rgba(255,127,0,0.35)';
  return 'rgba(77,175,74,0.25)';
}

function polyRiskStyle(feature: any) {
  const vmax = Number(feature.get('v_max') ?? 0);
  const isNew = !!feature.get('is_new');
  const evacuated = !!feature.get('evacuated');
  const fill = rampFill(vmax);
  const stroke = evacuated ? '#111' : isNew ? '#ff7f00' : '#333';
  const w = evacuated ? 2.0 : isNew ? 1.6 : 0.9;
  return stylePoly(fill, stroke, w);
}

function lineRiskStyle(feature: any) {
  const vmax = Number(feature.get('v_max') ?? 0);
  const isNew = !!feature.get('is_new');
  const evacuated = !!feature.get('evacuated');

  const color =
    evacuated ? '#111' :
    isNew ? '#ff7f00' :
    vmax >= 0.6 ? '#e41a1c' :
    vmax >= 0.2 ? '#f1b814' :
    '#4daf4a';

  const w = evacuated ? 3.0 : isNew ? 2.6 : 2.0;
  return styleLine(color, w);
}

function polygonChoroplethStyle(feature: any) {
  const maxProb = Number(feature.get('max_prob') ?? 0);
  return stylePoly(rampFill(maxProb), '#333', 0.8);
}

function mvt(id: string, url: string, style?: (f: any) => Style) {
  const source = new VectorTileSource({
    format: new MVT(),
    url,
    projection: proj3857,
  });

  if (DEBUG_TILE_COUNTS) {
    source.on('tileloadend', (evt: any) => {
      const tile = evt.tile;
      const n = tile?.getFeatures?.()?.length ?? 0;
      console.debug(`[tileloadend] ${id} features=${n}`);
    });
  }

  const layer = new VectorTileLayer({
    properties: { url },
    source,
    style: style ? (f) => style(f) : undefined,
    declutter: true,
    visible: true,
    renderBuffer: 256,
    renderMode: 'hybrid',
  });

  layer.set('id', id);
  layer.setZIndex(10); // ensure overlays above basemap
  return layer;
}

export function buildLayerEntries(): LayerEntry[] {
  const csd = mvt(
    'csd',
    `${TILESERV}/public.census_subdivisions/{z}/{x}/{y}.pbf?properties=csduid,name,prname`,
    () => csdStyle
  );

  const ecumeneD3 = mvt(
    'ecumene_d3',
    `${TILESERV}/risk.v_latest_ecumene_d3/{z}/{x}/{y}.pbf?properties=run_date,forecast_day,feature_id,n,v_max,evacuated,is_new,name`,
    polyRiskStyle
  );

  const ecumeneD7 = mvt(
    'ecumene_d7',
    `${TILESERV}/risk.v_latest_ecumene_d7/{z}/{x}/{y}.pbf?properties=run_date,forecast_day,feature_id,n,v_max,evacuated,is_new,name`,
    polyRiskStyle
  );

  const fnD3 = mvt(
    'fn_d3',
    `${TILESERV}/risk.v_latest_first_nations_d3/{z}/{x}/{y}.pbf?properties=run_date,forecast_day,feature_id,n,v_max,evacuated,is_new,name`,
    polyRiskStyle
  );

  const fnD7 = mvt(
    'fn_d7',
    `${TILESERV}/risk.v_latest_first_nations_d7/{z}/{x}/{y}.pbf?properties=run_date,forecast_day,feature_id,n,v_max,evacuated,is_new,name`,
    polyRiskStyle
  );

  const facilitiesD3 = mvt(
    'fac_d3',
    `${TILESERV}/risk.v_latest_facilities_d3/{z}/{x}/{y}.pbf?properties=run_date,forecast_day,feature_id,n,v_max,evacuated,is_new,name`,
    polyRiskStyle
  );

  const facilitiesD7 = mvt(
    'fac_d7',
    `${TILESERV}/risk.v_latest_facilities_d7/{z}/{x}/{y}.pbf?properties=run_date,forecast_day,feature_id,n,v_max,evacuated,is_new,name`,
    polyRiskStyle
  );

  const highwaysD3 = mvt(
    'hw_d3',
    `${TILESERV}/risk.v_latest_highways_d3/{z}/{x}/{y}.pbf?properties=run_date,forecast_day,feature_id,n,v_max,evacuated,is_new,name`,
    lineRiskStyle
  );

  const highwaysD7 = mvt(
    'hw_d7',
    `${TILESERV}/risk.v_latest_highways_d7/{z}/{x}/{y}.pbf?properties=run_date,forecast_day,feature_id,n,v_max,evacuated,is_new,name`,
    lineRiskStyle
  );

  const railD3 = mvt(
    'rail_d3',
    `${TILESERV}/risk.v_latest_rail_d3/{z}/{x}/{y}.pbf?properties=run_date,forecast_day,feature_id,n,v_max,evacuated,is_new,name`,
    lineRiskStyle
  );

  const railD7 = mvt(
    'rail_d7',
    `${TILESERV}/risk.v_latest_rail_d7/{z}/{x}/{y}.pbf?properties=run_date,forecast_day,feature_id,n,v_max,evacuated,is_new,name`,
    lineRiskStyle
  );

  const ecumeneRaw = mvt(
    'ecumene_raw',
    `${TILESERV}/risk.v_features_ecumene_raw/{z}/{x}/{y}.pbf?properties=name`,
    () => ecumeneBase
  );

  const fnRaw = mvt(
    'first_nations_raw',
    `${TILESERV}/risk.v_features_first_nations_raw/{z}/{x}/{y}.pbf?properties=name`,
    () => fnBase
  );

  const highwaysRaw = mvt(
    'highways_raw',
    `${TILESERV}/risk.v_features_highways_raw/{z}/{x}/{y}.pbf?properties=name`,
    () => highwaysBase
  );

  const railRaw = mvt(
    'rail_raw',
    `${TILESERV}/risk.v_features_rail_raw/{z}/{x}/{y}.pbf?properties=name`,
    () => railBase
  );

  const facilitiesRaw = mvt(
    'facilities_raw',
    `${TILESERV}/risk.v_features_facilities_raw/{z}/{x}/{y}.pbf?properties=name`,
    () => facilitiesBase
  );

  return [
    // Basemap placeholder entry (actual layer inserted in App.tsx)
    { id: 'cbmt', layer: new VectorTileLayer(), visible: true, label: 'Base (CBMT 3978)', group: 'Basemap', kind: 'base' },

    { id: 'csd', layer: csd, visible: true, label: 'Census Subdivisions (2025)', group: 'Reference', kind: 'overlay' },

    { id: 'ecumene_d3', layer: ecumeneD3, visible: true, label: 'Ecumene (D3)', group: 'D3 Risk', kind: 'overlay' },
    { id: 'fn_d3', layer: fnD3, visible: false, label: 'First Nations (D3)', group: 'D3 Risk', kind: 'overlay' },
    { id: 'fac_d3', layer: facilitiesD3, visible: false, label: 'Facilities (D3)', group: 'D3 Risk', kind: 'overlay' },
    { id: 'hw_d3', layer: highwaysD3, visible: false, label: 'Highways (D3)', group: 'D3 Risk', kind: 'overlay' },
    { id: 'rail_d3', layer: railD3, visible: false, label: 'Rail (D3)', group: 'D3 Risk', kind: 'overlay' },

    { id: 'ecumene_d7', layer: ecumeneD7, visible: false, label: 'Ecumene (D7)', group: 'D7 Risk', kind: 'overlay' },
    { id: 'fn_d7', layer: fnD7, visible: false, label: 'First Nations (D7)', group: 'D7 Risk', kind: 'overlay' },
    { id: 'fac_d7', layer: facilitiesD7, visible: false, label: 'Facilities (D7)', group: 'D7 Risk', kind: 'overlay' },
    { id: 'hw_d7', layer: highwaysD7, visible: false, label: 'Highways (D7)', group: 'D7 Risk', kind: 'overlay' },
    { id: 'rail_d7', layer: railD7, visible: false, label: 'Rail (D7)', group: 'D7 Risk', kind: 'overlay' },

    { id: 'ecumene_raw', layer: ecumeneRaw, visible: false, label: 'Ecumene (raw)', group: 'Raw Reference', kind: 'overlay' },
    { id: 'first_nations_raw', layer: fnRaw, visible: false, label: 'First Nations (raw)', group: 'Raw Reference', kind: 'overlay' },
    { id: 'highways_raw', layer: highwaysRaw, visible: false, label: 'Highways (raw)', group: 'Raw Reference', kind: 'overlay' },
    { id: 'rail_raw', layer: railRaw, visible: false, label: 'Rail (raw)', group: 'Raw Reference', kind: 'overlay' },
    { id: 'facilities_raw', layer: facilitiesRaw, visible: false, label: 'Facilities (raw)', group: 'Raw Reference', kind: 'overlay' },
  ];
}