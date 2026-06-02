import { describe, it, expect } from 'vitest'
import { transformTickets, applyFilters, extractMetros } from './use-maintenance-events'
import type { OpsTicket } from '@/lib/ops-api'

const makeTicket = (overrides: Partial<OpsTicket> = {}): OpsTicket => ({
  id: 'test-id',
  human_readable_id: 'M20260529-0001',
  type: 'maintenance',
  title: 'Arista EOS upgrade sea-rox1',
  description: '',
  status: 'planned',
  affected_link_pubkey: [],
  device_pubkey: [],
  reporter_name: 'Test User',
  reporter_email: 'test@test.com',
  contributor_name: 'RockawayX',
  start_at: '2026-06-15T01:00:00Z',
  end_at: '2026-06-15T05:00:00Z',
  created_at: '2026-06-01T00:00:00Z',
  updated_at: '2026-06-01T00:00:00Z',
  ...overrides,
})

describe('extractMetros', () => {
  it('returns metro_codes from ticket', () => {
    const t = makeTicket({ metro_codes: ['nyc', 'lon'] })
    expect(extractMetros(t)).toEqual(['nyc', 'lon'])
  })

  it('returns empty array when metro_codes is absent', () => {
    const t = makeTicket({ metro_codes: undefined })
    expect(extractMetros(t)).toEqual([])
  })
})

describe('transformTickets', () => {
  it('filters out non-maintenance tickets', () => {
    const incident = makeTicket({ type: 'incident' })
    expect(transformTickets([incident])).toHaveLength(0)
  })

  it('filters out tickets missing start_at', () => {
    const t = makeTicket({ start_at: undefined })
    expect(transformTickets([t])).toHaveLength(0)
  })

  it('includes tickets missing end_at, using updated_at for closed status', () => {
    const t = makeTicket({ end_at: undefined, status: 'closed', updated_at: '2026-06-20T00:00:00Z' })
    const [ev] = transformTickets([t])
    expect(ev).toBeDefined()
    expect(ev.endAt).toEqual(new Date('2026-06-20T00:00:00Z'))
  })

  it('includes planned tickets missing end_at, falling back to start_at', () => {
    const t = makeTicket({ end_at: undefined, status: 'planned' })
    const [ev] = transformTickets([t])
    expect(ev).toBeDefined()
    expect(ev.endAt).toEqual(new Date(t.start_at!))
  })

  it('falls back to start_at when closed ticket has empty updated_at', () => {
    const t = makeTicket({ end_at: undefined, status: 'closed', updated_at: '' })
    const [ev] = transformTickets([t])
    expect(ev).toBeDefined()
    expect(ev.endAt).toEqual(new Date(t.start_at!))
  })

  it('converts ISO timestamps to Dates', () => {
    const t = makeTicket()
    const [ev] = transformTickets([t])
    expect(ev.startAt).toBeInstanceOf(Date)
    expect(ev.endAt).toBeInstanceOf(Date)
  })

  it('falls back to reporter_name when contributor_name is absent', () => {
    const t = makeTicket({ contributor_name: undefined })
    const [ev] = transformTickets([t])
    expect(ev.contributorName).toBe('Test User')
  })
})

describe('applyFilters', () => {
  const events = transformTickets([
    makeTicket({ id: '1', title: 'Arista EOS upgrade sea-rox1', contributor_name: 'RockawayX', metro_codes: ['sea'], affected_links: [{ code: 'sea-rox1', pubkey: 'pk1' }] }),
    makeTicket({ id: '2', title: 'Device reboot chi-mlb1', contributor_name: 'Malbec Labs', metro_codes: ['chi'], affected_links: [{ code: 'chi-mlb1', pubkey: 'pk2' }], status: 'in-progress' }),
  ])

  it('returns all events when filters are empty', () => {
    expect(applyFilters(events, { search: '', contributors: new Set(), metros: new Set(), devices: new Set(), links: new Set(), status: '' })).toHaveLength(2)
  })

  it('filters by contributor', () => {
    const result = applyFilters(events, { search: '', contributors: new Set(['RockawayX']), metros: new Set(), devices: new Set(), links: new Set(), status: '' })
    expect(result).toHaveLength(1)
    expect(result[0].contributorName).toBe('RockawayX')
  })

  it('filters by status', () => {
    const result = applyFilters(events, { search: '', contributors: new Set(), metros: new Set(), devices: new Set(), links: new Set(), status: 'in-progress' })
    expect(result).toHaveLength(1)
    expect(result[0].id).toBe('2')
  })

  it('filters by metro', () => {
    const result = applyFilters(events, { search: '', contributors: new Set(), metros: new Set(['chi']), devices: new Set(), links: new Set(), status: '' })
    expect(result).toHaveLength(1)
    expect(result[0].id).toBe('2')
  })

  it('filters by search text (title)', () => {
    const result = applyFilters(events, { search: 'arista', contributors: new Set(), metros: new Set(), devices: new Set(), links: new Set(), status: '' })
    expect(result).toHaveLength(1)
    expect(result[0].id).toBe('1')
  })

  it('filters by search text (contributor name)', () => {
    const result = applyFilters(events, { search: 'malbec', contributors: new Set(), metros: new Set(), devices: new Set(), links: new Set(), status: '' })
    expect(result).toHaveLength(1)
    expect(result[0].id).toBe('2')
  })
})
