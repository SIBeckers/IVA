import proj4 from 'proj4'
import { register } from 'ol/proj/proj4'
import { get as getProjection } from 'ol/proj'
import { createXYZ } from 'ol/tilegrid';
import type Projection from 'ol/proj/Projection';
proj4.defs('EPSG:3978','+proj=lcc +lat_0=49 +lon_0=-95 +lat_1=49 +lat_2=77 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +type=crs');
register(proj4);

export const EXTENT_3978: [number, number, number, number] = [
    -2421618.74911166,
    -774687.715795296,
    3062991.079527088,
    4704011.872167985,
];

const p = getProjection('EPSG:3978');
if (!p) {
    throw new Error('EPSG:3978 projection not registered (getProjection returned null).');
}
export const proj3978: Projection = p;

proj3978.setExtent(EXTENT_3978); // extent is crucial for correct tile grid / reprojection [1](https://github.com/CrunchyData/pg_featureserv/issues/97)[3](https://github.com/CrunchyData/pg_featureserv/issues/6)

export const tileGrid3978 = createXYZ({
    extent: EXTENT_3978,
    tileSize: 256,
    maxZoom: 22,
});

