
// app/iva-map/src/App.tsx
import { useEffect, useRef, useState } from 'react';
import Map from 'ol/Map';
import View from 'ol/View';
import { LayerControl } from './LayerControl';
import { CANADA_3979 } from './projection';
import { buildLayerEntries, LayerEntry } from './layers';

export default function App() {
  const mapDivRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<Map | null>(null);
  const [entries, setEntries] = useState<LayerEntry[]>(() => buildLayerEntries());

  useEffect(() => {
    if (!mapDivRef.current) return;

    const map = new Map({
      target: mapDivRef.current,
      view: new View({
        projection: CANADA_3979,
        center: [0, 0],
        zoom: 3,
      }),
      layers: entries.map(e => {
        e.layer.setVisible(e.visible);
        e.layer.set('id', e.id);
        return e.layer;
      }),
    });

    mapRef.current = map;
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
