
// app/iva-map/src/App.tsx
import { useEffect, useRef, useState } from 'react';
import Map from 'ol/Map';
import View from 'ol/View';
import { LayerControl } from './LayerControl';
import { proj3978, EXTENT_3978 } from './projection';
import { buildLayerEntries, LayerEntry } from './layers';
import { buildCbmtBasemapLayer } from './basemap';
export default function App() {
  const mapDivRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<Map | null>(null);
  const [entries, setEntries] = useState<LayerEntry[]>(() => buildLayerEntries());

  useEffect(() => {
    if (!mapDivRef.current) return;
    const center3978: [number, number] = [(EXTENT_3978[0] + EXTENT_3978[2]) / 2,(EXTENT_3978[1] + EXTENT_3978[3]) / 2,];
    const map = new Map({
      target: mapDivRef.current,
      view: new View({
        projection: proj3978,
        center: center3978,
        zoom: 3,
        extent: EXTENT_3978,
      }),
      layers: entries.map(e => {
        e.layer.setVisible(e.visible);
        e.layer.set('id', e.id);
        return e.layer;
      }),
    });

    mapRef.current = map;
    
    
    buildCbmtBasemapLayer()
      .then((baseLayer) => {
        baseLayer.set('id', 'cbmt');
        baseLayer.setVisible(true);
        map.getLayers().insertAt(0, baseLayer);

        // Turn off OSM if present (it will otherwise hide CBMT)
        const osm = map.getLayers().getArray().find(l => l.get('id') === 'osm');
        if (osm) osm.setVisible(false);

        // Keep LayerControl state in sync
        setEntries((prev) => {
          const next = prev.map(e => e.id === 'osm' ? { ...e, visible: false } : e);
          return [
            { id: 'cbmt', layer: baseLayer as any, visible: true, label: 'Base (CBMT 3978)' },
            ...next,
          ];
        });
      })
      .catch((err) => console.error('CBMT basemap failed to load:', err));

    return () => {
      map.setTarget(undefined);
      mapRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    for (const e of entries) {
      const match = map.getLayers().getArray().find(l => l.get('id') === e.id);
      if (match) match.setVisible(e.visible);
    }
  }, [entries]);

  const onToggle = (id: string, visible: boolean) => {
    setEntries(prev => prev.map(e => (e.id === id ? { ...e, visible } : e)));
  };

  return (
    <>
      <div ref={mapDivRef} style={{ width: '100%', height: '100vh' }} />
      <LayerControl
        layers={entries.map(e => ({ id: e.id, label: e.label, visible: e.visible }))}
        onToggle={onToggle}
      />
    </>
  );
}
