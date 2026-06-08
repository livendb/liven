import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import fs from "fs";
import path from "path";

// Dynamically extract the webui_port from the LIVEN config file (liven.toml)
let webuiPort = 43120; // Standard fallback default
try {
  const tomlPath = path.resolve(__dirname, "../liven.toml");
  if (fs.existsSync(tomlPath)) {
    const tomlContent = fs.readFileSync(tomlPath, "utf-8");
    const match = tomlContent.match(/webui_port\s*=\s*(\d+)/);
    if (match) {
      webuiPort = parseInt(match[1], 10);
    }
  }
} catch (err) {
  console.warn("Could not read webui_port from liven.toml, defaulting to 43120:", err);
}

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      "/api": `http://localhost:${webuiPort}`,
      "/ws": {
        target: `ws://localhost:${webuiPort}`,
        ws: true,
      },
    },
  },
});
