import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import { BenchmarkReport } from './BenchmarkReport'
import { REPORT } from './benchmarkReportFixture'

const chartProps = vi.fn()
vi.mock('@/components/charts/ComparisonChart', () => ({
  ComparisonChart: (props: unknown) => {
    chartProps(props)
    return <div data-testid="comparison-chart" />
  },
}))

describe('BenchmarkReport', () => {
  it('shows exposure, the rolling Sharpe as numbers, and the regimes with errors', () => {
    render(<BenchmarkReport report={REPORT} strategyName="Trend" />)
    expect(screen.getByText(/Beta 0\.30 to SPY/)).toBeInTheDocument()
    expect(screen.getByText('Most volatile third')).toBeInTheDocument()
    expect(screen.getByText('0.61 ± 0.46')).toBeInTheDocument()
    expect(screen.getByText(/26 early days had too little history/)).toBeInTheDocument()
    const props = chartProps.mock.calls.at(-1)![0] as { format: string; series: unknown[] }
    expect(props.format).toBe('number')
    expect(props.series).toHaveLength(2)
  })

  it('says why there is no report', () => {
    render(
      <BenchmarkReport
        report={null}
        unavailable="The default benchmark SPY is not stored."
        strategyName="X"
      />,
    )
    expect(screen.getByText(/No benchmark report: The default benchmark SPY/)).toBeInTheDocument()
  })

  it('renders nothing when the report was not asked for', () => {
    const { container } = render(<BenchmarkReport report={null} strategyName="X" />)
    expect(container).toBeEmptyDOMElement()
  })
})
