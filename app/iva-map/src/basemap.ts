
// app/iva-map/src/basemap.ts
import VectorTileLayer from 'ol/layer/VectorTile';
import VectorTileSource from 'ol/source/VectorTile';
import MVT from 'ol/format/MVT';
import TileGrid from 'ol/tilegrid/TileGrid';
import { applyStyle } from 'ol-mapbox-style';
import { proj3978 } from './projection';

const CBMT_VTS =
  'https://tiles.arcgis.com/tiles/HsjBaDykC1mjhXz9/arcgis/rest/services/CBMT_CBCT_3978_V_OSM/VectorTileServer';


const CBMT_STYLE = `${CBMT_VTS}/resources/styles/root.json`; 

type ArcGisVtsInfo = {
  fullExtent: { xmin: number; ymin: number; xmax: number; ymax: number };
  tileInfo: {
    rows: number;
    cols: number;
    origin: { x: number; y: number };
    lods: { level: number; resolution: number }[];
  };
};

function tileGridFromArcGis(info: ArcGisVtsInfo) {
  const { xmin, ymin, xmax, ymax } = info.fullExtent;
  const extent: [number, number, number, number] = [xmin, ymin, xmax, ymax];

  const tileSize = [info.tileInfo.cols, info.tileInfo.rows] as [number, number];
  const origin: [number, number] = [info.tileInfo.origin.x, info.tileInfo.origin.y];
  const resolutions = info.tileInfo.lods.map((l) => l.resolution);

  return new TileGrid({ extent, origin, resolutions, tileSize });
}

export async function buildCbmtBasemapLayer() {

  const vtsInfo = (await fetch(`${CBMT_VTS}?f=pjson`).then((r) => r.json())) as ArcGisVtsInfo;
  const tileGrid = tileGridFromArcGis(vtsInfo);

  const layer = new VectorTileLayer({
    declutter: true,
    overlaps: false,
    source: new VectorTileSource({
      format: new MVT(),
      projection: proj3978,
      tileGrid,
      url: `${CBMT_VTS}/tile/{z}/{y}/{x}.pbf`,
    }),
  });


  await applyStyle(layer, CBMT_STYLE, 'esri', { updateSource: false }); 

  return layer;
}
