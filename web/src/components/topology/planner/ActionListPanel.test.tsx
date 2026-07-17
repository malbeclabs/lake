import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ActionListPanel } from './ActionListPanel'
import type { ActionList } from '@/lib/api'

vi.mock('@/lib/api', () => ({
  fetchPlanActionList: vi.fn(),
}))
import { fetchPlanActionList } from '@/lib/api'

const coordinationTitle =
  'jump_ ↔ latitude: coordinate moving DZX link L1 to device nyc002-dz001'

const actionList: ActionList = {
  plan_id: 'plan-1',
  environment: 'mainnet-beta',
  markdown: '# Topology plan: Q3 cleanup\n\n(full body)',
  groups: [
    {
      contributor_pk: 'c-jump',
      contributor_code: 'jump_',
      slack_channel: '#ext-doublezero-jump_',
      markdown: '## jump_ (#ext-doublezero-jump_)\n',
      tasks: [
        {
          seq: 10,
          op_type: 'move_link_end',
          title: coordinationTitle,
          state: 'pending',
          target_date: null,
          involved_contributors: ['jump_', 'latitude', 'teleport'],
        },
      ],
    },
    {
      contributor_pk: 'c-lat',
      contributor_code: 'latitude',
      slack_channel: '#ext-doublezero-latitude',
      markdown: '## latitude (#ext-doublezero-latitude)\n',
      tasks: [
        {
          seq: 10,
          op_type: 'move_link_end',
          title: coordinationTitle,
          state: 'pending',
          target_date: null,
          involved_contributors: ['jump_', 'latitude', 'teleport'],
        },
      ],
    },
  ],
}

const writeText = vi.fn().mockResolvedValue(undefined)
Object.defineProperty(navigator, 'clipboard', {
  value: { writeText },
  configurable: true,
  writable: true,
})

function renderPanel() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={client}>
      <ActionListPanel planId="plan-1" />
    </QueryClientProvider>
  )
}

describe('ActionListPanel', () => {
  beforeEach(() => {
    writeText.mockClear()
    ;(fetchPlanActionList as unknown as ReturnType<typeof vi.fn>).mockResolvedValue(actionList)
  })

  it('renders one group per contributor with slack hints and the coordination task', async () => {
    renderPanel()
    expect(await screen.findByText('jump_')).toBeInTheDocument()
    expect(screen.getByText('latitude')).toBeInTheDocument()
    expect(screen.getByText('#ext-doublezero-jump_')).toBeInTheDocument()
    expect(screen.getByText('#ext-doublezero-latitude')).toBeInTheDocument()

    // The DZX coordination task shows up in both involved groups.
    expect(screen.getAllByText(coordinationTitle)).toHaveLength(2)
    expect(screen.getAllByText('Coordinate with: jump_, latitude, teleport')).toHaveLength(2)
  })

  it('copies the full plan markdown to the clipboard', async () => {
    renderPanel()
    const copyAll = await screen.findByRole('button', { name: /copy all as markdown/i })
    fireEvent.click(copyAll)
    await waitFor(() => expect(writeText).toHaveBeenCalledWith(actionList.markdown))
  })

  it('copies a single contributor group markdown', async () => {
    renderPanel()
    const groupButtons = await screen.findAllByRole('button', { name: /^copy markdown$/i })
    fireEvent.click(groupButtons[0])
    await waitFor(() => expect(writeText).toHaveBeenCalledWith(actionList.groups[0].markdown))
  })
})
