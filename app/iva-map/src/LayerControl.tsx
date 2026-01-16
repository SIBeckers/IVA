
import React from 'react';

type LayerDef = {
  id: string;
  label: string;
  visible: boolean;
};

type Props = {
  layers: LayerDef[];
  onToggle: (id: string, visible: boolean) => void;
};

export function LayerControl({ layers, onToggle }: Props) {
  return (
    <div className="layer-control">
      <h3>Layers</h3>
      <ul>
        {layers.map(l => (
          <li key={l.id}>
            <label>
              <input
                type="checkbox"
                checked={l.visible}
                onChange={e => onToggle(l.id, e.target.checked)}
              />
              {l.label}
            </label>
          </li>
        ))}
      </ul>
      <style>{`
        .layer-control {
          position: absolute; top: 8px; left: 8px; z-index: 1000;
          background: rgba(255,255,255,.9); padding: 8px 10px; border-radius: 6px;
          font-family: system-ui,-apple-system,Segoe UI,Roboto,"Helvetica Neue",Arial;
        }
        .layer-control h3 { margin: 0 0 6px; font-size: 14px; }
        .layer-control ul { list-style: none; margin: 0; padding: 0; }
        .layer-control li { margin: 4px 0; }
      `}</style>
    </div>
  );
}
