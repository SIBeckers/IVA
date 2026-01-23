// app/iva-map/src/basemap.ts
import VectorTileLayer from 'ol/layer/VectorTile';
import VectorTileSource from 'ol/source/VectorTile';
import MVT from 'ol/format/MVT';
import { applyStyle } from 'ol-mapbox-style';
import { proj3978, tileGrid3978 } from './projection';

const CBMT_VTS = 'https://tiles.arcgis.com/tiles/HsjBaDykC1mjhXz9/arcgis/rest/services/CBMT_CBCT_3978_V_OSM/VectorTileServer';
const CBMT_STYLE = 'https://arcgis.com/sharing/rest/content/items/708e92c1f00941e3af3dd3c092ae4a0a/resources/styles/root.json';

export async function buildCbmtBasemapLayer() {
    const layer = new VectorTileLayer({
        declutter: true,
        source: new VectorTileSource({
            format: new MVT(),
            projection: proj3978,
            tileGrid: tileGrid3978,
            url: `${CBMT_VTS}/tile/{z}/{y}/{x}.pbf`, // Esri tile pattern [5](https://developers.arcgis.com/documentation/portal-and-data-services/data-services/vector-tile-services/display-vector-tiles/)
        }),
    });

  // Apply style but do NOT let it replace your source/grid
    await applyStyle(layer, CBMT_STYLE, {
        updateSource: false,
    });

    return layer;
}
