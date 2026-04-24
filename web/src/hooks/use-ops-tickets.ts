// web/src/hooks/use-ops-tickets.ts
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchActiveOpsTickets,
  fetchOpsTicketHistory,
  fetchOpsAssignees,
  createOpsTicket,
  type OpsTicket,
  type OpsTicketType,
  type CreateOpsTicketInput,
} from '@/lib/ops-api'

const STALE_TIME = 5 * 60 * 1000 // 5 minutes

// Fetch all active tickets once; filter client-side per entity.
// Returns undefined data (empty state) for unauthenticated users — no error shown.
export function useActiveOpsTickets() {
  return useQuery({
    queryKey: ['ops-tickets', 'active'],
    queryFn: fetchActiveOpsTickets,
    staleTime: STALE_TIME,
    retry: false,
    throwOnError: false,
  })
}

// Returns active tickets that affect a specific link or device pubkey.
// Checks both the flat pubkey arrays and the enriched object arrays returned by
// tickets created via the Ops Management web UI.
export function useTicketsForEntity(entityPk: string): OpsTicket[] {
  const { data } = useActiveOpsTickets()
  if (!data) return []
  return data.tickets.filter(
    (t) =>
      t.affected_link_pubkey.includes(entityPk) ||
      t.device_pubkey.includes(entityPk) ||
      (t.affected_links?.some(l => l.pubkey === entityPk) ?? false) ||
      (t.affected_devices?.some(d => d.pubkey === entityPk) ?? false)
  )
}

// Fetch 5 most recent closed tickets for a specific entity (detail page only).
export function useOpsTicketHistory(entityPk: string, ticketType?: OpsTicketType, entityType?: 'link' | 'device') {
  return useQuery({
    queryKey: ['ops-tickets', 'history', entityPk, ticketType, entityType],
    queryFn: () => fetchOpsTicketHistory(entityPk, ticketType, entityType),
    staleTime: STALE_TIME,
    enabled: !!entityPk,
  })
}

// Create a new incident ticket and invalidate the active tickets cache on success.
export function useCreateOpsTicket() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: CreateOpsTicketInput) => createOpsTicket(input),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['ops-tickets', 'active'] })
    },
  })
}

// Fetch the list of valid assignees. Cached for 5 minutes.
export function useOpsAssignees() {
  return useQuery({
    queryKey: ['ops-assignees'],
    queryFn: fetchOpsAssignees,
    staleTime: STALE_TIME,
    retry: false,
    throwOnError: false,
  })
}
