
// app/iva-map/src/layers.ts
import VectorTileLayer from 'ol/layer/VectorTile';
import VectorTileSource from 'ol/source/VectorTile';
import MVT from 'ol/format/MVT';
import TileLayer from 'ol/layer/Tile';
import XYZ from 'ol/source/XYZ';
import { Style, Stroke, Fill, Circle as CircleStyle } from 'ol/style';
import { proj3978, tileGrid3978 } from './projection';

export type LayerEntry = {
  id: string;
  layer: VectorTileLayer<any> | TileLayer<any>;
  visible: boolean;
  label: string;
};

const TILESERV = import.meta.env.VITE_TILESERV_BASE ?? 'http://localhost:7800';

// ---- Styles
const strokeThin = (color: string, w = 1) => new Stroke({ color, width: w });

const stylePoly = (fillColor: string, strokeColor = '#333', w = 0.6) =>
  new Style({ fill: new Fill({ color: fillColor }), stroke: strokeThin(strokeColor, w) });

const styleCircle = (fillColor: string, r = 4, strokeColor = '#113' ) =>
  new Style({ image: new CircleStyle({ radius: r, fill: new Fill({ color: fillColor }), stroke: strokeThin(strokeColor, 1) }) });

const csdStyle = stylePoly('rgba(34,139,34,0.10)', '#2e8b57', 1);
const ecumeneStyle = stylePoly('rgba(50,205,50,0.25)', '#2e8b57', 0.8);
const fnStyle = stylePoly('rgba(148,0,211,0.25)', '#6a0dad', 0.8);

const highwaysLine = new Style({ stroke: strokeThin('#f1b814', 1.5) });
const railLine = new Style({ stroke: strokeThin('#666', 1.5) });

const facilityPoint = styleCircle('#1f78b4', 4, '#0c3a66');

const circleLow = styleCircle('#4daf4a', 4, '#2b7a2b');
const circleMed = styleCircle('#ff7f00', 5, '#b35a00');
const circleHigh = styleCircle('#e41a1c', 6, '#a41315');

function valuesStyle(feature: any) {
  const vmax = feature.get('v_max');
  const img = vmax >= 0.6 ? circleHigh : vmax >= 0.2 ? circleMed : circleLow;
  return img;
}

function polygonChoroplethStyle(feature: any) {
  const maxProb = feature.get('max_prob');
  let c = 'rgba(0,0,0,0.05)';
  if (maxProb >= 0.6) c = 'rgba(228,26,28,0.55)';
  else if (maxProb >= 0.2) c = 'rgba(255,127,0,0.45)';
  return stylePoly(c, '#333', 0.5);
}


function mvt(url: string, style?: (f: any) => Style) {
  return new VectorTileLayer({
    properties: { url },
    source: new VectorTileSource({
      format: new MVT(),
      url,
      projection: proj3978,
      tileGrid: tileGrid3978,
    }),
    style: style ? (f) => style(f) : undefined,
    declutter: true,
    visible: true,
  });
}


export function buildLayerEntries(): LayerEntry[] {

  // Reference geography
  const csd = mvt(
    `${TILESERV}/public.census_subdivisions/{z}/{x}/{y}.pbf?properties=csduid,name,prname`,
    () => csdStyle
  ); csd.set('id','csd');

  // Values layers (latest D3/D7)
  const d3 = mvt(
    `${TILESERV}/risk.v_feature_stats_d3/{z}/{x}/{y}.pbf?properties=feature_set_code,v_mean,p50,v_max,feature_name`,
    valuesStyle
  ); d3.set('id','d3');

  const d7 = mvt(
    `${TILESERV}/risk.v_feature_stats_d7/{z}/{x}/{y}.pbf?properties=feature_set_code,v_mean,p50,v_max,feature_name`,
    valuesStyle
  ); d7.set('id','d7');

  // Intersection (latest) polygon choropleths
  const bldCsd = mvt(
    `${TILESERV}/risk.v_buildings_csd_agg_latest/{z}/{x}/{y}.pbf?properties=forecast_day,bld_count,v_mean_p50,max_prob`,
    polygonChoroplethStyle
  ); bldCsd.set('id','bld_csd');

  const bldEcu = mvt(
    `${TILESERV}/risk.v_buildings_ecumene_agg_latest/{z}/{x}/{y}.pbf?properties=forecast_day,bld_count,v_mean_p50,max_prob`,
    polygonChoroplethStyle
  ); bldEcu.set('id','bld_ecu');

  const bldFn = mvt(
    `${TILESERV}/risk.v_buildings_fn_agg_latest/{z}/{x}/{y}.pbf?properties=forecast_day,bld_count,v_mean_p50,max_prob`,
    polygonChoroplethStyle
  ); bldFn.set('id','bld_fn');

  // Raw reference layers
  const ecumeneRaw = mvt(
    `${TILESERV}/risk.v_features_ecumene_raw/{z}/{x}/{y}.pbf?properties=name`,
    () => ecumeneStyle
  ); ecumeneRaw.set('id','ecumene_raw');

  const fnRaw = mvt(
    `${TILESERV}/risk.v_features_first_nations_raw/{z}/{x}/{y}.pbf?properties=name`,
    () => fnStyle
  ); fnRaw.set('id','first_nations_raw');

  const highwaysRaw = mvt(
    `${TILESERV}/risk.v_features_highways_raw/{z}/{x}/{y}.pbf?properties=name`,
    () => highwaysLine
  ); highwaysRaw.set('id','highways_raw');

  const railRaw = mvt(
    `${TILESERV}/risk.v_features_rail_raw/{z}/{x}/{y}.pbf?properties=name`,
    () => railLine
  ); railRaw.set('id','rail_raw');

  const facilitiesRaw = mvt(
    `${TILESERV}/risk.v_features_facilities_raw/{z}/{x}/{y}.pbf?properties=name`,
    () => facilityPoint
  ); facilitiesRaw.set('id','facilities_raw');

  return [
    { id: 'csd',      layer: csd,           visible: true,  label: 'Census Subdivisions (2025)' },

    { id: 'd3',       layer: d3,            visible: true,  label: 'Values (D3)' },
    { id: 'd7',       layer: d7,            visible: false, label: 'Values (D7)' },

    { id: 'bld_csd',  layer: bldCsd,        visible: false, label: 'Buildings → CSD (latest)' },
    { id: 'bld_ecu',  layer: bldEcu,        visible: false, label: 'Buildings → Ecumene (latest)' },
    { id: 'bld_fn',   layer: bldFn,         visible: false, label: 'Buildings → First Nations (latest)' },

    { id: 'ecumene_raw',      layer: ecumeneRaw,    visible: false, label: 'Ecumene (raw)' },
    { id: 'first_nations_raw',layer: fnRaw,         visible: false, label: 'First Nations (raw)' },
    { id: 'highways_raw',     layer: highwaysRaw,   visible: false, label: 'Highways (raw)' },
    { id: 'rail_raw',         layer: railRaw,       visible: false, label: 'Railways (raw)' },
    { id: 'facilities_raw',   layer: facilitiesRaw, visible: false, label: 'Facilities (raw)' },
  ];
}
