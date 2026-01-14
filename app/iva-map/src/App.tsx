
import { useEffect, useRef } from 'react'
import Map from 'ol/Map'
import View from 'ol/View'
import TileLayer from 'ol/layer/Tile'
import XYZ from 'ol/source/XYZ'
import { CANADA_3979 } from './projection'
import { buildingsLayer, ecumeneLayer } from './layers'
export default function App() {
  const mapRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    if (!mapRef.current) return
    const base = new TileLayer({ source: new XYZ({ url: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png' }) })
    const map = new Map({ target: mapRef.current, view: new View({ projection: CANADA_3979, center: [0,0], zoom: 3 }), layers: [base, ecumeneLayer, buildingsLayer] })
    return () => { map.setTarget(undefined) }
  }, [])
  return <div id="map" ref={mapRef} />
}
