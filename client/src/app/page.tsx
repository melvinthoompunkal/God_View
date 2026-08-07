"use client";

import GlobeMap from "@/components/GlobeMap";
import Sidebar from "@/components/Sidebar";
import { useLiveStream } from "@/hooks/useLiveStream";

export default function Home() {
  const { events, isConnected } = useLiveStream("ws://localhost:8000/api/events/ws");

  return (
    <main className="min-h-screen flex items-center p-8 selection:bg-white selection:text-black relative overflow-hidden">
      <GlobeMap events={events} />
      
      {/* Foreground Antigravity UI */}
      <div className="relative z-10 group pointer-events-none w-full max-w-lg">
        <div className="absolute -inset-1 bg-gradient-to-r from-zinc-800 to-zinc-900 rounded-3xl blur-md opacity-25 group-hover:opacity-100 transition duration-1000 group-hover:duration-200"></div>
        <div className="relative p-12 bg-surface/60 border border-white/5 rounded-3xl shadow-[0_40px_80px_rgba(0,0,0,0.8)] flex flex-col items-start backdrop-blur-2xl pointer-events-auto transition-transform duration-500 hover:scale-[1.01]">
          <h1 className="text-5xl md:text-7xl font-light tracking-tighter text-white mb-2">
            God View
          </h1>
          <div className="flex items-center gap-3 mb-6">
            <span className={`relative flex h-3 w-3`}>
              {isConnected && <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>}
              <span className={`relative inline-flex rounded-full h-3 w-3 ${isConnected ? 'bg-green-500' : 'bg-red-500'}`}></span>
            </span>
            <span className="text-xs text-muted uppercase tracking-widest">{isConnected ? "System Online" : "Establishing Link..."}</span>
          </div>
          <p className="text-muted text-sm md:text-base leading-relaxed mb-6 font-light">
            A real-time spatial intelligence platform monitoring global geopolitics, market anomalies, and climate instability.
          </p>
          <div className="px-4 py-2 bg-white/5 rounded-full border border-white/5 text-xs text-white/80 font-medium">
            {events.length} Live Artifacts Tracked
          </div>
        </div>
      </div>

      <Sidebar events={events} />
    </main>
  );
}
