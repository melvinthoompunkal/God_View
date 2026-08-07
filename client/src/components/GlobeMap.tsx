"use client";

import React, { useMemo } from "react";
import Map, { NavigationControl, Source, Layer, CircleLayer } from "react-map-gl";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

interface GlobeMapProps {
  events?: any[];
}

const eventsLayer: CircleLayer = {
  id: 'events-point',
  type: 'circle',
  paint: {
    'circle-radius': [
      'interpolate', ['linear'], ['zoom'],
      2, 4,
      10, 12
    ],
    'circle-color': [
      'match',
      ['get', 'event_type'],
      'wildfire', '#ef4444',
      'severe_storm', '#3b82f6',
      'earthquake', '#f97316',
      'volcano', '#b91c1c',
      '#10b981' // default
    ],
    'circle-opacity': 0.8,
    'circle-stroke-width': 1,
    'circle-stroke-color': '#ffffff'
  }
};

export default function GlobeMap({ events = [] }: GlobeMapProps) {
  const geojsonData = useMemo(() => {
    return {
      type: "FeatureCollection",
      features: events
    };
  }, [events]);

  return (
    <div className="absolute inset-0 w-full h-full z-0">
      <Map
        mapLib={maplibregl}
        initialViewState={{
          longitude: -74.006,
          latitude: 40.7128,
          zoom: 2,
        }}
        mapStyle="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
        style={{ width: "100%", height: "100%" }}
        interactive={true}
      >
        <NavigationControl position="bottom-right" />
        
        <Source id="events-source" type="geojson" data={geojsonData as any}>
          <Layer {...eventsLayer} />
        </Source>
      </Map>
    </div>
  );
}
