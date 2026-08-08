import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    // shadcn/ui generates components that import from '@/...'. This alias must
    // stay in sync with the "paths" entry in tsconfig.app.json — Vite resolves
    // at build time, TypeScript only type-checks, so both are required.
    // import.meta.dirname, not __dirname — Vite 8 warns on the CJS global.
    alias: {
      '@': new URL('./src', import.meta.url).pathname,
    },
  },
  server: {
    // Pinned rather than left to Vite's default so the port stays in sync with
    // the API's CORS allow-list (app/core/config.py). A drifting dev port shows
    // up as a CORS failure that reads like an API bug.
    port: 5173,
    strictPort: true,
  },
})
