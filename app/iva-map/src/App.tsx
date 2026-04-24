// app/iva-map/src/App.tsx
import { useEffect, useMemo, useRef, useState } from 'react';
import OlMap from 'ol/Map';
import View from 'ol/View';

import { LayerControl } from './LayerControl';
import { proj3978, EXTENT_3978, GRID_RESOLUTIONS_3978 } from './projection';
import { buildLayerEntries, type LayerEntry } from './layers';
import { buildCbmtBasemapLayer } from './basemap';

export default function App() {
  const mapDivRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<OlMap | null>(null);

  const [entries, setEntries] = useState<LayerEntry[]>(() => buildLayerEntries());

  useEffect(() => {
    if (!mapDivRef.current) return;

    const center3978: [number, number] = [
      (EXTENT_3978[0] + EXTENT_3978[2]) / 2,
      (EXTENT_3978[1] + EXTENT_3978[3]) / 2,
    ];

    const map = new OlMap({
      target: mapDivRef.current,
      view: new View({
        projection: proj3978,
        center: center3978,
        extent: EXTENT_3978,
        resolutions: GRID_RESOLUTIONS_3978,
        constrainResolution: true,

        zoom: 4, // slightly closer than 3 to make outlines more apparent
        minZoom: 0,
        maxZoom: GRID_RESOLUTIONS_3978.length - 1,
      }),
      layers: entries.map((e) => {
        e.layer.setVisible(e.visible);
        e.layer.set('id', e.id);

        // Ensure overlays are above basemap by default.
        // (Basemap will be forced to zIndex 0 when inserted.)
        if (e.kind === 'overlay') {
          (e.layer as any).setZIndex?.(10);
        } else {
          (e.layer as any).setZIndex?.(0);
        }
        return e.layer;
      }),
    });

    mapRef.current = map;

    // Handy debug handle
    (window as any).__iva_map = map;

    // Add CBMT basemap at index 0 so overlays draw on top.
    buildCbmtBasemapLayer()
      .then((baseLayer) => {
        baseLayer.set('id', 'cbmt');
        baseLayer.setVisible(true);
        baseLayer.setZIndex?.(0);

        map.getLayers().insertAt(0, baseLayer);

        // Ensure existing overlays stay above
        map.getLayers()
          .getArray()
          .forEach((l) => {
            if (l.get('id') !== 'cbmt') (l as any).setZIndex?.(10);
          });

        // Keep LayerControl entries list in sync
        setEntries((prev) => {
          const filtered = prev.filter((e) => e.id !== 'cbmt');
          return [
            {
              id: 'cbmt',
              kind: 'base',
              group: 'Basemap',
              layer: baseLayer as any,
              visible: true,
              label: 'CBMT (3978)',
            },
            ...filtered,
          ];
        });
      })
      .catch((err) => console.error('CBMT basemap failed to load:', err));

    return () => {
      map.setTarget(undefined);
      mapRef.current = null;
      delete (window as any).__iva_map;
    };
  }, []);

  // Sync visibility state -> OL layers
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;

    for (const e of entries) {
      const match = map
        .getLayers()
        .getArray()
        .find((l) => l.get('id') === e.id);

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

    return Array.from(groups.values())
      .map((g) => ({
        title: g.title,
        layers: g.layers.sort((a, b) => a.label.localeCompare(b.label)),
      }))
      .sort((a, b) => a.title.localeCompare(b.title));
  }, [entries]);

  const onSelectBase = (id: string) => {
    setEntries((prev) => prev.map((e) => (e.kind === 'base' ? { ...e, visible: e.id === id } : e)));
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