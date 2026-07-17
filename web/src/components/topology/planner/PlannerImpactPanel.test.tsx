import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import type { PlanImpactReport, MetroLatencyDelta } from '@/lib/api'
import { PlannerImpactPanel } from './PlannerImpactPanel'

const fullReport: PlanImpactReport = {
  partition_issues: [
    {
      severity: 'high',
      entity_type: 'device',
      entity_pk: 'devpk',
      entity_code: 'lax-dz1',
      description: 'lax-dz1 is cut off from the network',
      caused_by: [{ seq: 10, op_type: 'remove_link', label: 'change 10' }],
      type: 'device_isolated',
    },
  ],
  latency_deltas: [
    {
      severity: 'medium',
      metro_a: 'nyc',
      metro_z: 'lon',
      before_us: 30000,
      after_us: 33000,
      delta_us: 3000,
      caused_by: [{ seq: 20, op_type: 'remove_link', label: 'change 20' }],
    },
    {
      severity: 'high',
      metro_a: 'sea',
      metro_z: 'sin',
      before_us: 100000,
      after_us: -1,
      delta_us: 0,
      caused_by: [{ seq: 10, op_type: 'remove_link', label: 'change 10' }],
    },
  ],
  redundancy_changes: [
    {
      severity: 'high',
      metro_a: 'fra',
      metro_z: 'ams',
      before_paths: 3,
      after_paths: 1,
      caused_by: [{ seq: 20, op_type: 'remove_link', label: 'change 20' }],
    },
  ],
  capacity_risks: [
    {
      severity: 'medium',
      link_pk: 'lpk',
      description: 'chi-nyc may run hot at 90% after reroute',
      estimated: true,
      reroute_from_link_pk: 'other-lpk',
      current_bps: 8_000_000_000,
      displaced_bps: 1_000_000_000,
      projected_bps: 9_000_000_000,
      bandwidth_bps: 10_000_000_000,
      utilization_pct: 90,
      caused_by: [{ seq: 20, op_type: 'remove_link', label: 'change 20' }],
      note: '',
    },
  ],
  overlap_warnings: [
    {
      severity: 'medium',
      other_plan_id: 'p2',
      other_plan_name: 'Q3 decom',
      other_plan_status: 'approved',
      entity_type: 'link',
      entity_pk: 'lpk',
      entity_code: 'chi-nyc',
      description: 'Q3 decom also removes link chi-nyc',
    },
  ],
  data_issues: [{ message: 'traffic metrics unavailable for 2 links' }],
  estimated: true,
  generated_at: '2026-07-16T00:00:00Z',
}

const labels = new Map<number, string>([
  [10, '#10 Remove link chi-nyc'],
  [20, '#20 Remove link fra-ams'],
])

const emptyReport: PlanImpactReport = {
  partition_issues: [],
  latency_deltas: [],
  redundancy_changes: [],
  capacity_risks: [],
  overlap_warnings: [],
  data_issues: [],
  estimated: false,
  generated_at: 'x',
}

describe('PlannerImpactPanel', () => {
  it('shows the prompt when there is no report yet', () => {
    render(
      <PlannerImpactPanel report={null} isLoading={false} error={null} changeLabels={new Map()} />,
    )
    expect(screen.getByText(/add changes to see impact/i)).toBeInTheDocument()
  })

  it('shows a loading state while computing the first report', () => {
    render(
      <PlannerImpactPanel report={null} isLoading={true} error={null} changeLabels={new Map()} />,
    )
    expect(screen.getByText(/computing impact/i)).toBeInTheDocument()
  })

  it('shows the empty state when the draft has no impact', () => {
    render(
      <PlannerImpactPanel report={emptyReport} isLoading={false} error={null} changeLabels={new Map()} />,
    )
    expect(screen.getByText(/no impact detected/i)).toBeInTheDocument()
  })

  it('renders all four checks plus overlap warnings with severity counts', () => {
    render(
      <PlannerImpactPanel report={fullReport} isLoading={false} error={null} changeLabels={labels} />,
    )

    // severity summary tallies (partition + latency + redundancy high = 3)
    expect(screen.getByText(/3 high/i)).toBeInTheDocument()
    expect(screen.getByText(/3 medium/i)).toBeInTheDocument()

    // 1. connectivity / partitions
    expect(screen.getByText('lax-dz1')).toBeInTheDocument()

    // 2. latency deltas, worst-first (unreachable sea->sin above nyc->lon)
    const latRows = screen.getAllByTestId('impact-latency-row')
    expect(latRows[0]).toHaveTextContent('sea')
    expect(latRows[0]).toHaveTextContent('sin')
    expect(latRows[1]).toHaveTextContent('nyc')
    expect(latRows[0]).toHaveTextContent(/no path/i)
    expect(latRows[1]).toHaveTextContent('+3.0ms')

    // 3. redundancy before/after
    expect(screen.getByText(/3 → 1/)).toBeInTheDocument()

    // 4. capacity fallback risk, labeled as an estimate
    expect(screen.getByText(/run hot at 90%/i)).toBeInTheDocument()
    expect(screen.getAllByText(/estimate/i).length).toBeGreaterThan(0)

    // cross-plan overlap warning (the plan name renders both as a styled
    // badge and inside the description sentence, so it appears twice)
    expect(screen.getAllByText(/Q3 decom/).length).toBeGreaterThan(0)

    // data issues surfaced to the operator
    expect(screen.getByText(/traffic metrics unavailable/i)).toBeInTheDocument()

    // plan-wide estimate flag
    expect(screen.getByText(/results include estimates/i)).toBeInTheDocument()

    // causing-change attribution (client label override by seq wins over the finding's own label)
    expect(screen.getAllByText('#10 Remove link chi-nyc').length).toBeGreaterThan(0)
    expect(screen.getAllByText('#20 Remove link fra-ams').length).toBeGreaterThan(0)
  })

  it('shows an error banner', () => {
    render(
      <PlannerImpactPanel report={null} isLoading={false} error="boom" changeLabels={new Map()} />,
    )
    expect(screen.getByText('boom')).toBeInTheDocument()
  })

  it('collapses metros sharing the same added latency into one row, revealed on hover', () => {
    const others = [
      'ams', 'lon', 'par', 'mad', 'mil', 'zrh', 'vie', 'waw', 'osl', 'sto', 'hel', 'dub', 'lax',
    ]
    const latency_deltas: MetroLatencyDelta[] = others.map((m) => ({
      severity: 'medium',
      metro_a: m,
      metro_z: 'nyc',
      before_us: 50000,
      after_us: 60000,
      delta_us: 10000,
      caused_by: [{ seq: 1, op_type: 'remove_link', label: 'change 1' }],
    }))
    const report: PlanImpactReport = { ...emptyReport, latency_deltas }

    render(
      <PlannerImpactPanel report={report} isLoading={false} error={null} changeLabels={new Map()} />,
    )

    // 13 near-identical pairs collapse into a single summary row.
    const rows = screen.getAllByTestId('impact-latency-row')
    expect(rows).toHaveLength(1)
    expect(rows[0]).toHaveTextContent('13 metros')
    expect(rows[0]).toHaveTextContent('nyc')
    expect(rows[0]).toHaveTextContent('+10.0ms')

    // members stay hidden until hover
    expect(screen.queryByTestId('latency-group-members')).not.toBeInTheDocument()

    const trigger = screen.getByText('13 metros')
    fireEvent.mouseOver(trigger)
    const membersPanel = screen.getByTestId('latency-group-members')
    for (const m of others) {
      expect(membersPanel).toHaveTextContent(m)
    }

    fireEvent.mouseOut(trigger)
    expect(screen.queryByTestId('latency-group-members')).not.toBeInTheDocument()
  })

  it('renders a lone latency pair as a normal single row (no forced grouping)', () => {
    const latency_deltas: MetroLatencyDelta[] = [
      {
        severity: 'medium',
        metro_a: 'nyc',
        metro_z: 'lon',
        before_us: 30000,
        after_us: 33000,
        delta_us: 3000,
        caused_by: [],
      },
    ]
    const report: PlanImpactReport = { ...emptyReport, latency_deltas }

    render(
      <PlannerImpactPanel report={report} isLoading={false} error={null} changeLabels={new Map()} />,
    )

    const rows = screen.getAllByTestId('impact-latency-row')
    expect(rows).toHaveLength(1)
    expect(rows[0]).toHaveTextContent('nyc')
    expect(rows[0]).toHaveTextContent('lon')
    expect(rows[0]).toHaveTextContent('+3.0ms')
    expect(screen.queryByTestId('latency-group-members')).not.toBeInTheDocument()
  })
})
