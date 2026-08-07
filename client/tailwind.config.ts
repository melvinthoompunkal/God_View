import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./src/pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        background: "#030303",
        foreground: "#FAFAFA",
        surface: "#0A0A0A",
        "surface-glass": "rgba(255, 255, 255, 0.03)",
        muted: "#666666",
      },
      backgroundImage: {
        "glass-gradient": "linear-gradient(145deg, rgba(255,255,255,0.05) 0%, rgba(255,255,255,0.01) 100%)",
      }
    },
  },
  plugins: [],
};
export default config;
