
import os, sys
import geopandas as gpd
import psycopg
EPSG = 3979

def upsert_features(gdf: gpd.GeoDataFrame, conn: psycopg.Connection, set_code: str, pk_col: str, name_col: str|None=None):
    gdf = gdf.copy()
    if gdf.crs is None or gdf.crs.to_epsg() != EPSG:
        gdf = gdf.to_crs(EPSG)
    with conn.cursor() as cur:
        cur.execute('SELECT id FROM risk.feature_sets WHERE code=%s', (set_code,))
        set_id = cur.fetchone()[0]
        for _, row in gdf.iterrows():
            attrs = row.drop(labels=[pk_col, name_col, gdf.geometry.name], errors='ignore').to_dict()
            cur.execute(
                """INSERT INTO risk.features(feature_set_id, source_pk, name, attrs, geom)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (feature_set_id, source_pk)
                    DO UPDATE SET name=EXCLUDED.name, attrs=EXCLUDED.attrs, geom=EXCLUDED.geom""",
                (set_id, str(row[pk_col]), row[name_col] if name_col else None, attrs, row[gdf.geometry.name].wkb)
            )
        conn.commit()

if __name__ == '__main__':
    data_dir = sys.argv[1] if len(sys.argv) > 1 else './data'
    conn = psycopg.connect(host=os.getenv('PGHOST','postgis'), port=os.getenv('PGPORT','5432'),
                           dbname=os.getenv('PGDATABASE','impacted_values'), user=os.getenv('PGUSER','iva_job'),
                           password=os.getenv('PGPASSWORD','changeme-job'))
    # Adjust filenames/columns to your local data
    # Ecumene
    ec = gpd.read_file(os.path.join(data_dir, 'ECUMENE_V3.gpkg'))
    upsert_features(ec, conn, 'ecumene', pk_col='OBJECTID_1', name_col='EcuName')
    # First Nations
    fn = gpd.read_file(os.path.join(data_dir, 'FirstNations.gpkg'))
    upsert_features(fn, conn, 'first_nations', pk_col='ID', name_col='BAND_NAME')
    # Highways
    hw = gpd.read_file(os.path.join(data_dir, 'highways_v2.gpkg'))
    upsert_features(hw, conn, 'highways', pk_col='ID', name_col='rtenum1')
    # Rail
    rl = gpd.read_file(os.path.join(data_dir, 'railways_v2.gpkg'))
    upsert_features(rl, conn, 'rail', pk_col='ID', name_col='subnam1_en')
    # Facilities
    fc = gpd.read_file(os.path.join(data_dir, 'facilities.gpkg'))
    upsert_features(fc, conn, 'facilities', pk_col='ID', name_col='Name')
    # Buildings (AB)
    bld = gpd.read_file(os.path.join(data_dir, 'ab_structures_en.gpkg'))
    pk = 'id' if 'id' in bld.columns else bld.columns[0]
    nm = 'name' if 'name' in bld.columns else None
    upsert_features(bld, conn, 'buildings', pk_col=pk, name_col=nm)
    conn.close()
