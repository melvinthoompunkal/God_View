"use client";

import { useState } from "react";
import { Switch } from "@headlessui/react";
import { ShieldExclamationIcon, GlobeAltIcon, ChartBarIcon } from "@heroicons/react/24/outline";

interface SidebarProps {
  events: any[];
}

export default function Sidebar({ events }: SidebarProps) {
  const [filters, setFilters] = useState({
    geopolitics: true,
    market: true,
    climate: true,
  });

  const toggleFilter = (key: keyof typeof filters) => {
    setFilters((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  return (
    <div className="absolute top-0 right-0 h-full w-full max-w-sm p-6 z-20 pointer-events-none flex flex-col justify-center">
      <div className="bg-surface/60 border border-white/5 rounded-3xl shadow-[0_20px_40px_rgba(0,0,0,0.5)] backdrop-blur-2xl pointer-events-auto flex flex-col max-h-[85vh] overflow-hidden">
        
        {/* Header & Controls */}
        <div className="p-6 border-b border-white/5 flex-shrink-0">
          <h2 className="text-xl font-light tracking-tight text-white mb-6 flex items-center gap-3">
            <GlobeAltIcon className="w-6 h-6 text-indigo-400" />
            Global Streams
          </h2>
          
          <div className="space-y-4">
            <Switch.Group as="div" className="flex items-center justify-between">
              <span className="flex flex-grow flex-col">
                <Switch.Label as="span" className="text-sm font-medium leading-6 text-white" passive>
                  Geopolitics
                </Switch.Label>
              </span>
              <Switch
                checked={filters.geopolitics}
                onChange={() => toggleFilter('geopolitics')}
                className={`${filters.geopolitics ? 'bg-indigo-500' : 'bg-white/10'}
                  relative inline-flex h-5 w-9 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 focus:ring-offset-surface`}
              >
                <span
                  aria-hidden="true"
                  className={`${filters.geopolitics ? 'translate-x-4' : 'translate-x-0'}
                    pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out`}
                />
              </Switch>
            </Switch.Group>

            <Switch.Group as="div" className="flex items-center justify-between">
              <span className="flex flex-grow flex-col">
                <Switch.Label as="span" className="text-sm font-medium leading-6 text-white" passive>
                  Market Anomalies
                </Switch.Label>
              </span>
              <Switch
                checked={filters.market}
                onChange={() => toggleFilter('market')}
                className={`${filters.market ? 'bg-fuchsia-500' : 'bg-white/10'}
                  relative inline-flex h-5 w-9 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-fuchsia-500 focus:ring-offset-2 focus:ring-offset-surface`}
              >
                <span
                  aria-hidden="true"
                  className={`${filters.market ? 'translate-x-4' : 'translate-x-0'}
                    pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out`}
                />
              </Switch>
            </Switch.Group>

            <Switch.Group as="div" className="flex items-center justify-between">
              <span className="flex flex-grow flex-col">
                <Switch.Label as="span" className="text-sm font-medium leading-6 text-white" passive>
                  Climate
                </Switch.Label>
              </span>
              <Switch
                checked={filters.climate}
                onChange={() => toggleFilter('climate')}
                className={`${filters.climate ? 'bg-emerald-500' : 'bg-white/10'}
                  relative inline-flex h-5 w-9 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-emerald-500 focus:ring-offset-2 focus:ring-offset-surface`}
              >
                <span
                  aria-hidden="true"
                  className={`${filters.climate ? 'translate-x-4' : 'translate-x-0'}
                    pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out`}
                />
              </Switch>
            </Switch.Group>
          </div>
        </div>

        {/* Scrolling Notification Feed */}
        <div className="flex-1 overflow-y-auto p-4 space-y-3 relative no-scrollbar">
          {events.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-32 text-center text-muted space-y-3">
              <ShieldExclamationIcon className="w-8 h-8 opacity-50" />
              <p className="text-sm">No live anomalies detected.</p>
            </div>
          ) : (
            events.map((event, idx) => {
              const properties = event.properties || event; // fallback if not a strict feature
              const eventType = properties.event_type || 'system_alert';
              const title = properties.title || 'A new spatial anomaly was detected in this sector.';
              
              // Map types to styling
              let badgeColor = 'bg-white/10 text-white';
              if (eventType === 'wildfire') badgeColor = 'bg-red-500/20 text-red-400 border-red-500/20';
              if (eventType === 'severe_storm') badgeColor = 'bg-blue-500/20 text-blue-400 border-blue-500/20';
              if (eventType === 'earthquake') badgeColor = 'bg-orange-500/20 text-orange-400 border-orange-500/20';

              return (
                <div key={properties.id || idx} className="p-4 bg-white/[0.02] border border-white/5 rounded-2xl hover:bg-white/[0.04] transition-all duration-300">
                  <div className="flex items-center justify-between mb-2">
                    <span className={`text-[10px] font-semibold uppercase tracking-wider px-2 py-0.5 rounded-full border ${badgeColor}`}>
                      {eventType.replace('_', ' ')}
                    </span>
                    <span className="text-[10px] text-muted">LIVE</span>
                  </div>
                  <p className="text-sm text-white/90 leading-relaxed font-light">
                    {title}
                  </p>
                </div>
              );
            })
          )}
        </div>
        
      </div>
    </div>
  );
}
