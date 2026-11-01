import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// In development the console talks to the API through this proxy; in Compose, nginx
// does the same job (see nginx.conf). Either way the browser only sees one origin.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: process.env.DJOS_API ?? "http://localhost:58000",
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
    },
  },
});
