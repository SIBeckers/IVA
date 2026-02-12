
// app/iva-map/src/App.tsx
import { useEffect, useMemo, useRef, useState } from 'react';
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

  // Build map once
  useEffect(() => {
    if (!mapDivRef.current) return;

    const center3978: [number, number] = [
      (EXTENT_3978[0] + EXTENT_3978[2]) / 2,
      (EXTENT_3978[1] + EXTENT_3978[3]) / 2,
    ];

    const map = new Map({
      target: mapDivRef.current,
      view: new View({
        projection: proj3978,
        center: center3978,
        zoom: 3,
        extent: EXTENT_3978,
      }),
      layers: entries.map((e) => {
        e.layer.setVisible(e.visible);
        e.layer.set('id', e.id);
        return e.layer;
      }),
    });

    mapRef.current = map;

    // Add CBMT basemap at index 0 so overlays draw on top.
    buildCbmtBasemapLayer()
      .then((baseLayer) => {
        baseLayer.set('id', 'cbmt');
        baseLayer.setVisible(true);
        map.getLayers().insertAt(0, baseLayer);

        setEntries((prev) => {
          const filtered = prev.filter((e) => e.id !== 'cbmt');
          return [
            { id: 'cbmt', kind: 'base', group: 'Basemap', layer: baseLayer as any, visible: true, label: 'CBMT (3978)' },
            ...filtered,
          ];
        });
      })
      .catch((err) => console.error('CBMT basemap failed to load:', err));

    return () => {
      map.setTarget(undefined);
      mapRef.current = null;
    };
  }, []);

  // Sync visibility state -> OL layers
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    for (const e of entries) {
      const match = map.getLayers().getArray().find((l) => l.get('id') === e.id);
      if (match) match.setVisible(e.visible);
    }
  }, [entries]);

  const baseLayers = useMemo(
    () => entries.filter((e) => e.kind === 'base').map((e) => ({ id: e.id, label: e.label, visible: e.visible })),
    [entries]
  );
  const selectedBaseId = useMemo(() => {
    const active = entries.find((e) => e.kind === 'base' && e.visible);
    return active?.id ?? null;
  }, [entries]);

  const overlayGroups = useMemo(() => {
    const groups = new Map<string, { title: string; layers: { id: string; label: string; visible: boolean }[] }>();
    for (const e of entries) {
      if (e.kind !== 'overlay') continue;
      const key = e.group ?? 'Other';
      if (!groups.has(key)) groups.set(key, { title: key, layers: [] });
      groups.get(key)!.layers.push({ id: e.id, label: e.label, visible: e.visible });
    }
    // Sort groups and layers for stable UI
    return Array.from(groups.values())
      .map((g) => ({
        title: g.title,
        layers: g.layers.sort((a, b) => a.label.localeCompare(b.label)),
      }))
      .sort((a, b) => a.title.localeCompare(b.title));
  }, [entries]);

  const onSelectBase = (id: string) => {
    setEntries((prev) =>
      prev.map((e) =>
        e.kind === 'base'
          ? { ...e, visible: e.id === id }
          : e
      )
    );
  };

  const onToggleOverlay = (id: string, visible: boolean) => {
    setEntries((prev) => prev.map((e) => (e.id === id ? { ...e, visible } : e)));
  };

  return (
    <>
      <div ref={mapDivRef} style={{ width: '100%', height: '100vh' }} />
      <LayerControl
        baseLayers={baseLayers}
        selectedBaseId={selectedBaseId}
        overlayGroups={overlayGroups}
        onSelectBase={onSelectBase}
        onToggleOverlay={onToggleOverlay}
      />
    </>
  );
}
