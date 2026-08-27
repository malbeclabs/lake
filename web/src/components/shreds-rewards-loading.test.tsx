import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ShredsRewardsDetail, ShredsRewardsResponse } from '@/lib/api'

// The pages call these through the api module, so the module is what gets
// stubbed — the components themselves are rendered for real.
const fetchShredsRewards = vi.fn()
const fetchShredsRewardsDetail = vi.fn()
vi.mock('@/lib/api', () => ({
  fetchShredsRewards: (...a: unknown[]) => fetchShredsRewards(...a),
  fetchShredsRewardsDetail: (...a: unknown[]) => fetchShredsRewardsDetail(...a),
}))

import { ShredsRewardsPage } from './shreds-rewards-page'
import { ShredsRewardsDetailPage } from './shreds-rewards-detail-page'

// useDelayedLoading holds the skeletons back by 150ms so a cache hit never
// flashes them. Every assertion about a skeleton has to step past that first.
const LOADING_DELAY_MS = 150

function deferred<T>() {
  let resolve!: (v: T) => void
  const promise = new Promise<T>((r) => {
    resolve = r
  })
  return { promise, resolve }
}

function renderWithProviders(ui: React.ReactNode, route = '/') {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  })
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[route]}>{ui}</MemoryRouter>
    </QueryClientProvider>,
  )
}

/** Advance past the delay and let React flush the resulting state update. */
async function advancePastDelay() {
  await act(async () => {
    vi.advanceTimersByTime(LOADING_DELAY_MS + 10)
  })
}

function listPayload(): ShredsRewardsResponse {
  return {
    current_solana_epoch: 1023,
    latest_finalized_epoch: 1022,
    epoch_columns: [1022],
    validators: [
      {
        node_id: 'node-A',
        vote_pubkey: 'vote-A',
        validator_name: 'Alpha Validator',
        activated_stake: 5_000_000_000,
        dz_user_ip: '203.0.113.10',
        total_earned_2z: 10530,
        immediately_claimable_2z: 3510,
        epoch_earnings: { 1022: 3510 },
        epoch_tokens: { 1022: '2Z' },
      },
    ],
    clients: [],
    total: 1,
  }
}

function detailPayload(): ShredsRewardsDetail {
  return {
    node_id: 'node-A',
    vote_pubkey: 'vote-A',
    validator_name: 'Alpha Validator',
    activated_stake: 5_000_000_000,
    dz_user_ip: '203.0.113.10',
    epochs: [
      {
        solana_epoch: 1022,
        subscription_epoch: 1022,
        leader_slots: 60,
        client_id: 1,
        client_name: 'Jito Labs',
        earned: 3510,
        token_symbol: '2Z',
        state: 'claimable',
      },
    ],
  }
}

beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true })
  fetchShredsRewards.mockReset()
  fetchShredsRewardsDetail.mockReset()
})

afterEach(() => {
  vi.useRealTimers()
})

describe('Edge Rewards list loading state', () => {
  it('shows the page chrome and skeleton rows while the first page loads', async () => {
    const d = deferred<ShredsRewardsResponse>()
    fetchShredsRewards.mockReturnValue(d.promise)

    const { container } = renderWithProviders(<ShredsRewardsPage />, '/dz/shreds/rewards')
    await advancePastDelay()

    // The chrome is real content and must be on screen while the rows are not:
    // a bare centred spinner used to replace all of it.
    expect(screen.getByRole('heading', { name: 'Edge Rewards' })).toBeInTheDocument()
    expect(screen.getByText('Validator')).toBeInTheDocument()
    expect(screen.getByText('Claimable')).toBeInTheDocument()
    expect(container.querySelectorAll('.animate-pulse').length).toBeGreaterThan(0)

    // And the empty state must NOT be showing — an arriving page is not an
    // answer of "no validators".
    expect(screen.queryByText(/No validators have earned rewards yet/)).not.toBeInTheDocument()

    d.resolve(listPayload())
    expect(await screen.findByText('Alpha Validator')).toBeInTheDocument()
    expect(container.querySelectorAll('.animate-pulse')).toHaveLength(0)
  })

  // Regression: keepPreviousData used to carry the validator payload across a
  // grouping switch. Its `clients` is an empty array, and groupByClient flips
  // the instant the URL does, so the client table rendered "No client teams
  // have earned rewards yet" as a finished answer — at full opacity, because
  // the placeholder dimming is delayed — until the real response arrived.
  it('does not answer "no client teams" while the grouping switch is in flight', async () => {
    const list = deferred<ShredsRewardsResponse>()
    fetchShredsRewards.mockReturnValueOnce(list.promise)

    const { container } = renderWithProviders(<ShredsRewardsPage />, '/dz/shreds/rewards')
    list.resolve(listPayload())
    expect(await screen.findByText('Alpha Validator')).toBeInTheDocument()

    // Switch to Client teams; hold the response open.
    const clients = deferred<ShredsRewardsResponse>()
    fetchShredsRewards.mockReturnValueOnce(clients.promise)
    await act(async () => {
      screen.getByRole('button', { name: 'Client teams' }).click()
    })

    expect(screen.queryByText(/No client teams have earned rewards yet/)).not.toBeInTheDocument()
    // The validator rows belong to the mode being left, so they must be gone too.
    expect(screen.queryByText('Alpha Validator')).not.toBeInTheDocument()

    await advancePastDelay()
    expect(container.querySelectorAll('.animate-pulse').length).toBeGreaterThan(0)

    clients.resolve({
      ...listPayload(),
      validators: [],
      clients: [{ client_id: 1, client_name: 'Jito Labs', validators: 68, total_earned_2z: 1234 }],
      total: 1,
    })
    expect(await screen.findByText('Jito Labs')).toBeInTheDocument()
  })

  it('keeps the previous page readable while the next one loads', async () => {
    const first = deferred<ShredsRewardsResponse>()
    fetchShredsRewards.mockReturnValueOnce(first.promise)

    renderWithProviders(<ShredsRewardsPage />, '/dz/shreds/rewards')
    first.resolve({ ...listPayload(), total: 250 })
    expect(await screen.findByText('Alpha Validator')).toBeInTheDocument()

    // A page turn stays inside one grouping, so the old rows are a stale view of
    // the same table and should stay on screen under the shimmer.
    fetchShredsRewards.mockReturnValueOnce(deferred<ShredsRewardsResponse>().promise)
    await act(async () => {
      screen.getByRole('button', { name: 'Next page' }).click()
    })
    expect(screen.getByText('Alpha Validator')).toBeInTheDocument()
  })

  it('does not flash skeletons for a response that arrives inside the delay', async () => {
    fetchShredsRewards.mockResolvedValue(listPayload())

    const { container } = renderWithProviders(<ShredsRewardsPage />, '/dz/shreds/rewards')
    // Rows arrive, and the delay is never reached — a page-cache hit is this
    // fast, and a skeleton that appeared and vanished would read as a glitch.
    expect(await screen.findByText('Alpha Validator')).toBeInTheDocument()
    await act(async () => {
      vi.advanceTimersByTime(LOADING_DELAY_MS - 50)
    })

    expect(container.querySelectorAll('.animate-pulse')).toHaveLength(0)
  })
})

describe('Edge Rewards detail loading state', () => {
  function renderDetail() {
    return renderWithProviders(
      <Routes>
        <Route path="/dz/shreds/rewards/:nodeId" element={<ShredsRewardsDetailPage />} />
      </Routes>,
      '/dz/shreds/rewards/node-A',
    )
  }

  it('names the validator from the URL and skeletons only the figures', async () => {
    const d = deferred<ShredsRewardsDetail>()
    fetchShredsRewardsDetail.mockReturnValue(d.promise)

    const { container } = renderDetail()
    await advancePastDelay()

    // The node id is in the URL, so identity is real content from frame one
    // rather than something to wait for.
    expect(screen.getAllByText('node-A').length).toBeGreaterThan(0)
    expect(screen.getByText('Validator Identity')).toBeInTheDocument()
    expect(screen.getByText('Immediately Claimable')).toBeInTheDocument()
    expect(container.querySelectorAll('.animate-pulse').length).toBeGreaterThan(0)
    expect(screen.queryByText(/No rewards recorded/)).not.toBeInTheDocument()

    d.resolve(detailPayload())
    expect(await screen.findByText('Jito Labs')).toBeInTheDocument()
    expect(container.querySelectorAll('.animate-pulse')).toHaveLength(0)
  })

  it('shows the error state rather than skeletons when the request fails', async () => {
    fetchShredsRewardsDetail.mockRejectedValue(new Error('boom'))

    const { container } = renderDetail()
    await advancePastDelay()

    expect(await screen.findByText('Unable to load validator rewards')).toBeInTheDocument()
    expect(container.querySelectorAll('.animate-pulse')).toHaveLength(0)
  })
})
