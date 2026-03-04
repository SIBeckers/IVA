// app/iva-map/src/featureserv.ts
import VectorSource from 'ol/source/Vector';
import GeoJSON from 'ol/format/GeoJSON';
import { bbox as bboxStrategy } from 'ol/loadingstrategy';
import type { Extent } from 'ol/extent';

const FEATURESERV_BASE =
  (import.meta as any).env?.VITE_FEATURESERV_BASE ?? 'http://localhost:9000';

export type FeatureServOptions = {
  /** Request bbox is in this CRS (SRID). IMPORTANT for EPSG:3978 maps. */
  bboxCrs?: number; // default 3978

  /** Request output geometry CRS. pg_featureserv supports `crs` parameter. */
  outCrs?: number; // default 3978

  /** Reduce payload size */
  properties?: string[]; // maps to `properties=...`

  /** Page size for paging through results */
  limit?: number; // default 5000

  /** Quantize bbox keys (meters) to reduce refetch spam */
  quantizeMeters?: number; // default 250
};

function collectionItemsUrl(
  collectionId: string,
  extent: Extent,
  limit: number,
  offset: number,
  opts: Required<Pick<FeatureServOptions, 'bboxCrs' | 'outCrs'>> & Pick<FeatureServOptions, 'properties'>
) {
  const u = new URL(
    `${FEATURESERV_BASE.replace(/\/+$/, '')}/collections/${collectionId}/items`
  );

  // bbox always included
  u.searchParams.set('bbox', extent.join(','));

  // IMPORTANT:
  // pg_featureserv treats bbox as lon/lat (SRID=4326) by default.
  // Provide bbox-crs to indicate the bbox is in EPSG:3978. [2](https://access.crunchydata.com/documentation/pg_featureserv/latest/usage/query_data/)
  u.searchParams.set('bbox-crs', String(opts.bboxCrs));

  // Ask for output CRS = 3978 so GeoJSON coordinates match the map projection.
  // pg_featureserv supports `crs` query parameter. [1](https://github.com/CrunchyData/pg_featureserv)[2](https://access.crunchydata.com/documentation/pg_featureserv/latest/usage/query_data/)
  u.searchParams.set('crs', String(opts.outCrs));

  // paging
  u.searchParams.set('limit', String(limit));
  u.searchParams.set('offset', String(offset));

  // reduce payload
  if (opts.properties && opts.properties.length) {
    u.searchParams.set('properties', opts.properties.join(','));
  }

  return u.toString();
}

export function makeFeatureServSource(collectionId: string, options: FeatureServOptions = {}) {
  const format = new GeoJSON();

  const bboxCrs = options.bboxCrs ?? 3978;
  const outCrs = options.outCrs ?? 3978;
  const limit = options.limit ?? 5000;
  const quant = options.quantizeMeters ?? 250;

  const seen = new Set<string>();

  const source = new VectorSource({
    format,
    strategy: bboxStrategy,
    wrapX: false,
    loader: async (extent, _resolution, projection) => {
      // Quantize extent to reduce repeat fetches for tiny pans.
      const q = (x: number) => Math.round(x / quant) * quant;
      const key = [q(extent[0]), q(extent[1]), q(extent[2]), q(extent[3])].join(',');

      if (seen.has(key)) return;
      seen.add(key);

      try {
        let offset = 0;

        while (true) {
          const url = collectionItemsUrl(collectionId, extent, limit, offset, {
            bboxCrs,
            outCrs,
            properties: options.properties,
          });

          const resp = await fetch(url);
          if (!resp.ok) {
            throw new Error(`pg_featureserv ${resp.status} ${resp.statusText}: ${url}`);
          }

          const geojson = await resp.json();

          // Because we requested crs=3978, the server response geometry is in EPSG:3978.
          // Use the map projection for featureProjection so OL stores geometries correctly.
          const feats = format.readFeatures(geojson, {
            dataProjection: projection,
            featureProjection: projection,
          });

          if (feats.length) source.addFeatures(feats);

          if (feats.length < limit) break;
          offset += limit;
        }
      } catch (err) {
        console.error(`FeatureServ loader error (${collectionId}):`, err);
      }
    },
  });

  return source;
}
