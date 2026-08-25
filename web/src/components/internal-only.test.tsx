import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { InternalOnly } from './internal-only'
import type { Account } from '@/lib/api'

const auth = vi.hoisted(() => ({ state: { user: null as Account | null, isLoading: false } }))
vi.mock('@/contexts/AuthContext', () => ({ useAuth: () => auth.state }))

function renderGuarded(state: { user: Account | null; isLoading: boolean }) {
  auth.state = state
  return render(
    <MemoryRouter initialEntries={['/dz/kalshi/l2']}>
      <Routes>
        <Route path="/" element={<div>home</div>} />
        <Route
          path="/dz/kalshi/l2"
          element={
            <InternalOnly>
              <div>Kalshi Sports L2</div>
            </InternalOnly>
          }
        />
      </Routes>
    </MemoryRouter>,
  )
}

const internal = { is_internal_user: true } as Account
const outside = { is_internal_user: false } as Account

describe('InternalOnly', () => {
  it('renders the page for an internal user', () => {
    renderGuarded({ user: internal, isLoading: false })
    expect(screen.getByText('Kalshi Sports L2')).toBeInTheDocument()
  })

  // The venue name and the column vocabulary are the disclosure, not the data — the API returns
  // 403 either way. So the page must not reach the DOM at all, which is what the sidebar's
  // `is_internal_user` check was always for.
  it('redirects an anonymous visitor home without rendering the page', () => {
    renderGuarded({ user: null, isLoading: false })
    expect(screen.queryByText('Kalshi Sports L2')).not.toBeInTheDocument()
    expect(screen.getByText('home')).toBeInTheDocument()
  })

  it('redirects a signed-in user from outside the allowed domain', () => {
    renderGuarded({ user: outside, isLoading: false })
    expect(screen.queryByText('Kalshi Sports L2')).not.toBeInTheDocument()
    expect(screen.getByText('home')).toBeInTheDocument()
  })

  // The case this test exists for. `user` is null while the session resolves, so a guard that
  // decides before the load finishes bounces an internal user who hard-refreshes the URL or opens
  // it from a bookmark — the page would work only when reached from inside the app, which is the
  // bug nobody reproduces on the first try. It must neither render nor redirect yet.
  it('waits for the session rather than redirecting while auth loads', () => {
    renderGuarded({ user: null, isLoading: true })
    expect(screen.queryByText('Kalshi Sports L2')).not.toBeInTheDocument()
    expect(screen.queryByText('home')).not.toBeInTheDocument()
  })
})
