import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fetchMaintenanceTickets, isTicketClosed } from '@/lib/ops-api'
import { contributorHue } from './colors'
import type { OpsTicket, OpsTicketStatus } from '@/lib/ops-api'

export interface MaintenanceEvent {
  id: string
  title: string
  contributorName: string
  hue: number
  startAt: Date
  endAt: Date
  status: OpsTicketStatus
  affectedLinks: Array<{ code: string; pubkey: string }>
  affectedDevices: Array<{ code: string; pubkey: string }>
  enrichedDeviceCodes: string[]  // includes link-side devices from ClickHouse
  metros: string[]
  slackUrl?: string
}

export interface CalendarFilters {
  search: string
  contributors: Set<string>
  metros: Set<string>
  devices: Set<string>
  links: Set<string>
  status: string
}

// Extract metro codes enriched by the API from ClickHouse.
export function extractMetros(ticket: OpsTicket): string[] {
  return ticket.metro_codes ?? []
}

function resolveEndAt(t: OpsTicket): Date {
  if (t.end_at) return new Date(t.end_at)
  // Closed tickets with no end_at: use updated_at as a proxy for when it closed.
  if (isTicketClosed(t.status)) {
    const d = new Date(t.updated_at)
    return isNaN(d.getTime()) ? new Date(t.start_at!) : d
  }
  // Active/planned with no end_at: treat as same-day.
  return new Date(t.start_at!)
}

export function transformTickets(tickets: OpsTicket[]): MaintenanceEvent[] {
  return tickets
    .filter((t) => t.type === 'maintenance' && t.start_at)
    .map((t) => {
      const name = t.contributor_name ?? t.reporter_name
      return {
        id: t.id,
        title: t.title,
        contributorName: name,
        hue: contributorHue(name),
        startAt: new Date(t.start_at!),
        endAt: resolveEndAt(t),
        status: t.status,
        affectedLinks: t.affected_links ?? [],
        affectedDevices: t.affected_devices ?? [],
        enrichedDeviceCodes: t.enriched_device_codes ?? [],
        metros: extractMetros(t),
        slackUrl: t.slack_message_url,
      }
    })
}

export function applyFilters(
  events: MaintenanceEvent[],
  filters: CalendarFilters
): MaintenanceEvent[] {
  const q = filters.search.toLowerCase()
  return events.filter((ev) => {
    if (filters.contributors.size > 0 && !filters.contributors.has(ev.contributorName)) return false
    if (filters.status && ev.status !== filters.status) return false
    if (filters.metros.size > 0 && !ev.metros.some((m) => filters.metros.has(m))) return false
    if (filters.devices.size > 0 &&
        !ev.affectedDevices.some((d) => filters.devices.has(d.code)) &&
        !ev.enrichedDeviceCodes.some((c) => filters.devices.has(c))) return false
    if (filters.links.size > 0 && !ev.affectedLinks.some((l) => filters.links.has(l.code))) return false
    if (q && !ev.title.toLowerCase().includes(q) && !ev.contributorName.toLowerCase().includes(q)) return false
    return true
  })
}

export function useMaintenanceEvents() {
  const { data: activeData, isLoading: activeLoading, error: activeError } = useQuery({
    queryKey: ['maintenance-tickets', 'active'],
    queryFn: () => fetchMaintenanceTickets('active'),
    staleTime: 5 * 60 * 1000,
    retry: false,
    throwOnError: false,
  })
  const { data: completedData, isLoading: completedLoading, error: completedError } = useQuery({
    queryKey: ['maintenance-tickets', 'not_active'],
    queryFn: () => fetchMaintenanceTickets('not_active'),
    staleTime: 5 * 60 * 1000,
    retry: false,
    throwOnError: false,
  })

  const allEvents = useMemo(() => {
    const raw = [
      ...(activeData?.tickets ?? []),
      ...(completedData?.tickets ?? []),
    ]
    return transformTickets(raw)
  }, [activeData, completedData])

  const rawCount = (activeData?.tickets.length ?? 0) + (completedData?.tickets.length ?? 0)

  const isLoading = activeLoading || completedLoading
  const error = activeError ?? completedError

  const [filters, setFilters] = useState<CalendarFilters>({
    search: '',
    contributors: new Set(),
    metros: new Set(),
    devices: new Set(),
    links: new Set(),
    status: '',
  })

  const filteredEvents = useMemo(
    () => applyFilters(allEvents, filters),
    [allEvents, filters]
  )

  const allContributors = useMemo(
    () => [...new Set(allEvents.map((e) => e.contributorName))].sort(),
    [allEvents]
  )

  const allMetros = useMemo(
    () => [...new Set(allEvents.flatMap((e) => e.metros))].sort(),
    [allEvents]
  )

  const allDevices = useMemo(
    () => [...new Set(allEvents.flatMap((e) => [
      ...e.affectedDevices.map((d) => d.code),
      ...e.enrichedDeviceCodes,
    ]))].sort(),
    [allEvents]
  )

  const allLinks = useMemo(
    () => [...new Set(allEvents.flatMap((e) => e.affectedLinks.map((l) => l.code)))].sort(),
    [allEvents]
  )

  const hasActiveFilters =
    filters.search !== '' ||
    filters.contributors.size > 0 ||
    filters.metros.size > 0 ||
    filters.devices.size > 0 ||
    filters.links.size > 0 ||
    filters.status !== ''

  return {
    events: filteredEvents,
    allContributors,
    allMetros,
    allDevices,
    allLinks,
    rawCount,
    filters,
    setFilters,
    hasActiveFilters,
    isLoading,
    error,
  }
}
