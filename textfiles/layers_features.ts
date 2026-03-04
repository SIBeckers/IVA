// app/iva-map/src/layers.ts
import VectorLayer from 'ol/layer/Vector';
import type BaseLayer from 'ol/layer/Base';
import VectorSource from 'ol/source/Vector';

import { Style, Stroke, Fill } from 'ol/style';

import { makeFeatureServSource } from './featureserv';

export type LayerKind = 'base' | 'overlay';

export type LayerEntry = {
  id: string;
  layer: BaseLayer;
  visible: boolean;
  label: string;
  group: string;
  kind: LayerKind;
};

// ---------------------------
// Styling helpers (same logic as your MVT version)
// ---------------------------
const strokeThin = (color: string, w = 1) => new Stroke({ color, width: w });
const stylePoly = (fillColor: string, strokeColor = '#333', w = 0.8) =>
  new Style({ fill: new Fill({ color: fillColor }), stroke: strokeThin(strokeColor, w) });
const styleLine = (color: string, w = 1.8) => new Style({ stroke: strokeThin(color, w) });

const csdStyle = stylePoly('rgba(34,139,34,0.10)', '#2e8b57', 1);
const ecumeneBase = stylePoly('rgba(50,205,50,0.15)', '#2e8b57', 0.8);
const fnBase = stylePoly('rgba(148,0,211,0.15)', '#6a0dad', 0.8);
const facilitiesBase = stylePoly('rgba(31,120,180,0.12)', '#0c3a66', 1.0);

const highwaysBase = styleLine('#f1b814', 1.8);
const railBase = styleLine('#666', 1.8);

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
  const color = evacuated
    ? '#111'
    : isNew
      ? '#ff7f00'
      : vmax >= 0.6
        ? '#e41a1c'
        : vmax >= 0.2
          ? '#f1b814'
          : '#4daf4a';
  const w = evacuated ? 3.0 : isNew ? 2.6 : 2.0;
  return styleLine(color, w);
}

function polygonChoroplethStyle(feature: any) {
  const maxProb = Number(feature.get('max_prob') ?? 0);
  return stylePoly(rampFill(maxProb), '#333', 0.8);
}

// ---------------------------
// Helper to create a pg_featureserv-backed VectorLayer
// ---------------------------
function fsLayer(
  id: string,
  collectionId: string,
  properties: string[],
  style: Style | ((f: any) => Style)
) {
  const layer = new VectorLayer({
    source: makeFeatureServSource(collectionId, {
      properties,
      bboxCrs: 3978,
      outCrs: 3978,
      limit: 5000,
      quantizeMeters: 250,
    }),
    style: typeof style === 'function' ? (f) => style(f) : style,
    declutter: true,
    visible: true,
  });

  layer.set('id', id);
  return layer;
}

export function buildLayerEntries(): LayerEntry[] {
  // ✅ Collection IDs confirmed by your /collections.json dump
  // e.g., "public.census_subdivisions_2025", "risk.v_latest_ecumene", etc.

  // Reference geography
  const csd = fsLayer(
    'csd',
    'public.census_subdivisions_2025',
    ['csduid', 'csdname', 'prname'], // adjust if your column names differ
    csdStyle
  );

  // ----------------------------
  // IMPORTANT NOTE about D3/D7 layers:
  // Your collections.json excerpt shows risk.v_latest_ecumene / ..._highways / ..._rail etc,
  // but not the specific *_d3 / *_d7 views (they may exist further down, or may not be present).
  //
  // If you DO have the *_d3/*_d7 collections, replace the collectionId strings below accordingly.
  // If you do NOT, use the generic latest collections as a starting point.
  // ----------------------------

  // Latest-per-theme risk layers (starting with non-horizon-specific collections)
  const ecumene = fsLayer(
    'ecumene_latest',
    'risk.v_latest_ecumene',
    ['run_date', 'forecast_day', 'feature_id', 'n', 'v_max', 'evacuated', 'is_new', 'name'],
    polyRiskStyle
  );

  const firstNations = fsLayer(
    'fn_latest',
    'risk.v_latest_first_nations',
    ['run_date', 'forecast_day', 'feature_id', 'n', 'v_max', 'evacuated', 'is_new', 'name'],
    polyRiskStyle
  );

  const facilities = fsLayer(
    'fac_latest',
    'risk.v_latest_facilities',
    ['run_date', 'forecast_day', 'feature_id', 'n', 'v_max', 'evacuated', 'is_new', 'name'],
    polyRiskStyle
  );

  const highways = fsLayer(
    'hw_latest',
    'risk.v_latest_highways',
    ['run_date', 'forecast_day', 'feature_id', 'n', 'v_max', 'evacuated', 'is_new', 'name'],
    lineRiskStyle
  );

  const rail = fsLayer(
    'rail_latest',
    'risk.v_latest_rail',
    ['run_date', 'forecast_day', 'feature_id', 'n', 'v_max', 'evacuated', 'is_new', 'name'],
    lineRiskStyle
  );

  // Building aggregates
  const bldCsd = fsLayer(
    'bld_csd',
    'risk.v_buildings_csd_agg_latest',
    ['forecast_day', 'bld_count', 'v_mean_p50', 'max_prob'],
    polygonChoroplethStyle
  );

  const bldEcu = fsLayer(
    'bld_ecu',
    'risk.v_buildings_ecumene_agg_latest',
    ['forecast_day', 'bld_count', 'v_mean_p50', 'max_prob'],
    polygonChoroplethStyle
  );

  const bldFn = fsLayer(
    'bld_fn',
    'risk.v_buildings_fn_agg_latest',
    ['forecast_day', 'bld_count', 'v_mean_p50', 'max_prob'],
    polygonChoroplethStyle
  );

  // Raw reference layers
  const ecumeneRaw = fsLayer('ecumene_raw', 'risk.v_features_ecumene_raw', ['name'], ecumeneBase);
  const fnRaw = fsLayer('first_nations_raw', 'risk.v_features_first_nations_raw', ['name'], fnBase);
  const highwaysRaw = fsLayer('highways_raw', 'risk.v_features_highways_raw', ['name'], highwaysBase);
  const railRaw = fsLayer('rail_raw', 'risk.v_features_rail_raw', ['name'], railBase);
  const facilitiesRaw = fsLayer('facilities_raw', 'risk.v_features_facilities_raw', ['name'], facilitiesBase);

  return [
    // Basemap placeholder entry (actual layer inserted in App.tsx)
    { id: 'cbmt', layer: new VectorLayer({ source: new VectorSource() }), visible: true, label: 'Base (CBMT 3978)', group: 'Basemap', kind: 'base' },

    { id: 'csd', layer: csd, visible: true, label: 'Census Subdivisions (2025)', group: 'Reference', kind: 'overlay' },

    { id: 'ecumene_latest', layer: ecumene, visible: true, label: 'Ecumene (latest)', group: 'Risk', kind: 'overlay' },
    { id: 'fn_latest', layer: firstNations, visible: false, label: 'First Nations (latest)', group: 'Risk', kind: 'overlay' },
    { id: 'fac_latest', layer: facilities, visible: false, label: 'Facilities (latest)', group: 'Risk', kind: 'overlay' },
    { id: 'hw_latest', layer: highways, visible: false, label: 'Highways (latest)', group: 'Risk', kind: 'overlay' },
    { id: 'rail_latest', layer: rail, visible: false, label: 'Rail (latest)', group: 'Risk', kind: 'overlay' },

    { id: 'bld_csd', layer: bldCsd, visible: false, label: 'Buildings → CSD (latest)', group: 'Buildings Aggregates', kind: 'overlay' },
    { id: 'bld_ecu', layer: bldEcu, visible: false, label: 'Buildings → Ecumene (latest)', group: 'Buildings Aggregates', kind: 'overlay' },
    { id: 'bld_fn', layer: bldFn, visible: false, label: 'Buildings → First Nations (latest)', group: 'Buildings Aggregates', kind: 'overlay' },

    { id: 'ecumene_raw', layer: ecumeneRaw, visible: false, label: 'Ecumene (raw)', group: 'Raw Reference', kind: 'overlay' },
    { id: 'first_nations_raw', layer: fnRaw, visible: false, label: 'First Nations (raw)', group: 'Raw Reference', kind: 'overlay' },
    { id: 'highways_raw', layer: highwaysRaw, visible: false, label: 'Highways (raw)', group: 'Raw Reference', kind: 'overlay' },
    { id: 'rail_raw', layer: railRaw, visible: false, label: 'Rail (raw)', group: 'Raw Reference', kind: 'overlay' },
    { id: 'facilities_raw', layer: facilitiesRaw, visible: false, label: 'Facilities (raw)', group: 'Raw Reference', kind: 'overlay' },
  ];
}
