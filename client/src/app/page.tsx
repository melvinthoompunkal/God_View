export default function Home() {
  return (
    <main className="min-h-screen flex flex-col items-center justify-center p-8 selection:bg-white selection:text-black">
      <div className="relative group">
        <div className="absolute -inset-1 bg-gradient-to-r from-zinc-800 to-zinc-900 rounded-xl blur opacity-25 group-hover:opacity-100 transition duration-1000 group-hover:duration-200"></div>
        <div className="relative p-12 bg-surface border border-white/5 rounded-xl shadow-[0_20px_40px_rgba(0,0,0,0.5)] flex flex-col items-center text-center backdrop-blur-xl">
          <h1 className="text-4xl md:text-6xl font-light tracking-tight text-white mb-4">
            Antigravity
          </h1>
          <p className="text-muted max-w-md mx-auto text-sm md:text-base leading-relaxed">
            A spatial, stark dark-mode interface. Built with precision and weightless design principles.
          </p>
        </div>
      </div>
    </main>
  );
}
