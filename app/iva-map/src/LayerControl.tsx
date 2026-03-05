
import React from 'react';

type LayerDef = {
  id: string;
  label: string;
  visible: boolean;
};

type Group = {
  title: string;
  layers: LayerDef[];
};

type Props = {
  baseLayers: LayerDef[];
  selectedBaseId: string | null;
  overlayGroups: Group[];
  onSelectBase: (id: string) => void;
  onToggleOverlay: (id: string, visible: boolean) => void;
};

export function LayerControl({
  baseLayers,
  selectedBaseId,
  overlayGroups,
  onSelectBase,
  onToggleOverlay,
}: Props) {
  return (
    <div className="layer-control">
      <h3>Layers</h3>

      {baseLayers.length > 0 && (
        <>
          <h4>Basemap</h4>
          <ul>
            {baseLayers.map((l) => (
              <li key={l.id}>
                <label>
                  <input
                    type="radio"
                    name="basemap"
                    checked={selectedBaseId === l.id}
                    onChange={() => onSelectBase(l.id)}
                  />
                  {l.label}
                </label>
              </li>
            ))}
          </ul>
        </>
      )}

      <h4>Overlays</h4>
      {overlayGroups.map((g) => (
        <div key={g.title} className="group">
          <div className="group-title">{g.title}</div>
          <ul>
            {g.layers.map((l) => (
              <li key={l.id}>
                <label>
                  <input
                    type="checkbox"
                    checked={l.visible}
                    onChange={(e) => onToggleOverlay(l.id, e.target.checked)}
                  />
                  {l.label}
                </label>
              </li>
            ))}
          </ul>
        </div>
      ))}

      <style>{`
        .layer-control {
          position: absolute;
          top: 50px;
          left: 8px;
          z-index: 1000;
          background: rgba(255,255,255,.92);
          padding: 10px 12px;
          border-radius: 8px;
          font-family: system-ui,-apple-system,Segoe UI,Roboto,"Helvetica Neue",Arial;
          max-height: calc(100vh - 16px);
          overflow: auto;
          box-shadow: 0 1px 6px rgba(0,0,0,.18);
          min-width: 260px;
        }
        .layer-control h3 { margin: 0 0 8px; font-size: 14px; }
        .layer-control h4 { margin: 10px 0 6px; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; color: #334; }
        .layer-control ul { list-style: none; margin: 0; padding: 0; }
        .layer-control li { margin: 4px 0; }
        .layer-control input { margin-right: 6px; }
        .group { margin-bottom: 8px; }
        .group-title { font-size: 12px; font-weight: 600; margin: 6px 0 4px; color: #223; }
      `}</style>
    </div>
  );
}
