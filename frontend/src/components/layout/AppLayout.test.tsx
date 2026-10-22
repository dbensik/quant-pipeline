/**
 * components/layout/AppLayout.test.tsx
 *
 * The navigation lists every page once, inside its section, with the sections
 * in flow order.
 */

import { screen, within } from '@testing-library/react'
import { describe, expect, it } from 'vitest'

import { AppLayout } from '@/components/layout/AppLayout'
import { routeGroups, routes } from '@/routes'
import { renderPage } from '@/test/renderPage'

describe('Navigation', () => {
  it('groups every page under its section, sections in flow order', () => {
    renderPage(<AppLayout>content</AppLayout>)
    const nav = screen.getByRole('navigation')
    const sections = within(nav).getAllByRole('group')
    expect(sections.map((s) => s.getAttribute('aria-label'))).toEqual([...routeGroups])

    for (const section of sections) {
      const links = within(section).getAllByRole('link').map((a) => a.textContent)
      const expected = routes
        .filter((r) => r.group === section.getAttribute('aria-label'))
        .map((r) => r.label)
      expect(links).toEqual(expected)
    }
    expect(within(nav).getAllByRole('link')).toHaveLength(routes.length)
  })

  it('lists routes in the same order as their sections', () => {
    const order = routes.map((r) => routeGroups.indexOf(r.group))
    expect(order).toEqual([...order].sort((a, b) => a - b))
  })
})
