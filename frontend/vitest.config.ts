import { defineConfig, mergeConfig } from 'vitest/config'

import viteConfig from './vite.config.ts'

// Kept separate from vite.config.ts, and merged rather than duplicated, so the
// `@/` alias can never drift between how the app builds and how it is tested.
export default mergeConfig(
  viteConfig,
  defineConfig({
    test: {
      environment: 'jsdom',
      setupFiles: ['./src/test/setup.ts'],
      // No `globals: true` on purpose. `npm run build` runs `tsc -b`, which
      // type-checks everything under src/ — including tests. Global describe/it
      // would need vitest's types wired into tsconfig.app.json, and getting it
      // wrong breaks the production build. Explicit imports cost one line per
      // file and keep the build config untouched.
      include: ['src/**/*.test.{ts,tsx}'],
      restoreMocks: true,
    },
  }),
)
