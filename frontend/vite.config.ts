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
    // 5174, NOT Vite's default 5173. Every Vite project takes 5173, so two
    // running at once collide — and Vite's usual response is to quietly pick
    // the next free port, which then fails the API's CORS allow-list and reads
    // as an API bug rather than a port conflict. strictPort makes that failure
    // loud instead.
    //
    // Must stay in sync with QUANT_VITE_PORT in run_pipeline.sh and with the
    // allow-list in app/core/config.py.
    port: Number(process.env.QUANT_VITE_PORT ?? 5174),
    strictPort: true,
  },
})
