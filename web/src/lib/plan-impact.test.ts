import { describe, it, expect, vi, afterEach } from 'vitest'
import { fetchPlanImpact } from './api'
import type { PlanImpactReport } from './api'

const emptyReport: PlanImpactReport = {
  partition_issues: [],
  latency_deltas: [],
  redundancy_changes: [],
  capacity_risks: [],
  overlap_warnings: [],
  data_issues: [],
  estimated: false,
  generated_at: '2026-07-16T00:00:00Z',
}

// The Go handler can emit a finding with caused_by: null (nil slice when a
// change does not directly touch the isolated entity's footprint). This typed
// fixture proves PlanImpactReport accepts that shape.
const reportWithNullCause: PlanImpactReport = {
  ...emptyReport,
  partition_issues: [
    {
      severity: 'high',
      entity_type: 'metro',
      entity_pk: '',
      entity_code: 'ams',
      description: 'Metro loses all external connectivity',
      caused_by: null,
      type: 'metro_isolated',
      metro_code: 'ams',
    },
  ],
}

afterEach(() => {
  vi.restoreAllMocks()
})

describe('fetchPlanImpact', () => {
  it('POSTs the draft changes to the plan impact endpoint', async () => {
    const spy = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response(JSON.stringify(emptyReport), { status: 200 }))

    const report = await fetchPlanImpact('plan-1', [])

    expect(report).toEqual(emptyReport)
    expect(spy).toHaveBeenCalledTimes(1)
    const [url, init] = spy.mock.calls[0]
    expect(url).toBe('/api/topology/plans/plan-1/impact')
    expect(init?.method).toBe('POST')
    expect(JSON.parse(init?.body as string)).toEqual({ changes: [] })
  })

  it('forwards the draft changes in the request body', async () => {
    const spy = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response(JSON.stringify(emptyReport), { status: 200 }))

    const changes = [
      { id: 'c1', seq: 10, op_type: 'remove_link' },
    ] as unknown as Parameters<typeof fetchPlanImpact>[1]

    await fetchPlanImpact('plan-1', changes)

    const body = JSON.parse(spy.mock.calls[0][1]?.body as string)
    expect(body.changes[0].id).toBe('c1')
  })

  it('throws when the response is not ok', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('nope', { status: 500 }))
    await expect(fetchPlanImpact('plan-1', [])).rejects.toThrow('Failed to compute plan impact')
  })

  it('returns findings whose caused_by is null (nil Go slice)', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify(reportWithNullCause), { status: 200 })
    )

    const report = await fetchPlanImpact('plan-1', [])

    expect(report.partition_issues[0].caused_by).toBeNull()
  })
})
