import { describe, it, expect, vi, afterEach } from 'vitest'
import { fetchPlanActionList } from './api'
import type { ActionList } from './api'

afterEach(() => {
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
})

describe('fetchPlanActionList', () => {
  it('requests the action-list endpoint and returns parsed JSON', async () => {
    // The typed literal proves the nullable server fields (target_date,
    // involved_contributors) accept null — Go emits them without omitempty.
    const payload: ActionList = {
      plan_id: 'p1',
      environment: 'mainnet-beta',
      markdown: '# Topology plan: Q3\n',
      groups: [
        {
          contributor_pk: 'c-jump',
          contributor_code: 'jump_',
          slack_channel: '#ext-doublezero-jump_',
          markdown: '## jump_ (#ext-doublezero-jump_)\n',
          tasks: [
            {
              seq: 10,
              op_type: 'remove_device',
              title: 'Decommission device lax001-dz001',
              state: 'pending',
              target_date: null,
              involved_contributors: null,
              current_users: 5,
              stake_sol: 1000,
              stake_share: 2.5,
            },
          ],
        },
      ],
    }
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify(payload), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    )
    vi.stubGlobal('fetch', fetchMock)

    const result = await fetchPlanActionList('p1')
    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(fetchMock.mock.calls[0][0]).toBe('/api/topology/plans/p1/action-list')
    expect(result).toEqual(payload)
    // Nullable fields round-trip as null, not undefined/absent.
    expect(result.groups[0].tasks[0].target_date).toBeNull()
    expect(result.groups[0].tasks[0].involved_contributors).toBeNull()
  })

  it('throws when the response is not ok', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('nope', { status: 500 })))
    await expect(fetchPlanActionList('p1')).rejects.toThrow('Failed to fetch action list')
  })
})
