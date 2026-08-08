import js from '@eslint/js'
import globals from 'globals'
import reactHooks from 'eslint-plugin-react-hooks'
import reactRefresh from 'eslint-plugin-react-refresh'
import tseslint from 'typescript-eslint'
import { defineConfig, globalIgnores } from 'eslint/config'

export default defineConfig([
  // src/components/ui and src/api/schema.d.ts are GENERATED, not authored:
  // shadcn writes the former (`npx shadcn add ...`), openapi-typescript the
  // latter (`npm run gen:api`). Linting them just flags upstream's style — and
  // any fix is overwritten on the next regeneration.
  globalIgnores(['dist', 'src/components/ui/**', 'src/api/schema.d.ts']),
  {
    // Test files: allow the leading-underscore convention for values that exist
    // only to be discarded by destructuring (e.g. omitting a key before a deep
    // equality assertion).
    files: ['**/*.test.{ts,tsx}', 'src/test/**'],
    rules: {
      '@typescript-eslint/no-unused-vars': [
        'error',
        { varsIgnorePattern: '^_', argsIgnorePattern: '^_' },
      ],
    },
  },
  {
    files: ['**/*.{ts,tsx}'],
    extends: [
      js.configs.recommended,
      tseslint.configs.recommended,
      reactHooks.configs.flat.recommended,
      reactRefresh.configs.vite,
    ],
    languageOptions: {
      ecmaVersion: 2020,
      globals: globals.browser,
    },
  },
])
