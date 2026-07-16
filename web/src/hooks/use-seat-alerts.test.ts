import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import React from 'react'
import { useCreateSeatAlert } from './use-seat-alerts'
import * as api from '../lib/api'

function wrapper({ children }: { children: React.ReactNode }) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return React.createElement(QueryClientProvider, { client: qc }, children)
}

describe('useCreateSeatAlert', () => {
  beforeEach(() => vi.restoreAllMocks())
  it('calls createSeatAlert and returns the deep link', async () => {
    vi.spyOn(api, 'createSeatAlert').mockResolvedValue({
      id: 'x', activation_token: 't', telegram_deep_link: 'https://t.me/b?start=t', status: 'pending_activation',
    })
    const { result } = renderHook(() => useCreateSeatAlert(), { wrapper })
    result.current.mutate({ seat_pk: 's', trigger_type: 'epochs_left', threshold_value: 2, announcements_opt_in: true })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(result.current.data?.telegram_deep_link).toContain('start=t')
  })
})
