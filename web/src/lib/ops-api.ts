// web/src/lib/ops-api.ts
import { apiFetch } from './api'

export type OpsTicketType = 'incident' | 'maintenance'

export type OpsTicketStatus =
  | 'open' | 'acknowledged' | 'investigating' | 'mitigating'
  | 'monitoring' | 'planned' | 'in-progress'
  | 'resolved' | 'closed' | 'completed'

export type OpsTicketSeverity = 'sev1' | 'sev2' | 'sev3'

export interface OpsTicket {
  id: string                         // UUID
  human_readable_id: string          // e.g. "I20250413-a7b2"
  type: OpsTicketType
  title: string
  description: string
  severity?: OpsTicketSeverity
  status: OpsTicketStatus
  affected_link_pubkey: string[]
  device_pubkey: string[]
  // Enriched link/device objects (code + pubkey) returned by upstream API
  affected_links?: Array<{ code: string; pubkey: string }>
  affected_devices?: Array<{ code: string; pubkey: string }>
  reporter_name: string
  reporter_email: string
  contributor_name?: string
  start_at?: string                  // ISO timestamp
  end_at?: string                    // ISO timestamp
  slack_message_url?: string
  created_at: string
  updated_at: string
}

export interface OpsTicketsListResponse {
  tickets: OpsTicket[]
  total: number
}

export interface CreateOpsTicketInput {
  type: OpsTicketType
  title: string
  description: string
  severity?: OpsTicketSeverity
  status?: OpsTicketStatus
  affected_link_pubkey?: string[]
  device_pubkey?: string[]
  start_at?: string
  end_at?: string
  contributor_pubkey?: string | null
}

// Fetch all active tickets (maintenance + incidents across all entities).
// Called once per status-page load; filtered client-side per entity.
export async function fetchActiveOpsTickets(): Promise<OpsTicketsListResponse> {
  const res = await apiFetch('/api/ops-tickets?status=active')
  if (!res.ok) throw new Error(`Failed to fetch ops tickets: ${res.status}`)
  const json = await res.json()
  return json.data ?? json
}

// Fetch the 5 most recent closed tickets for a specific link or device.
export async function fetchOpsTicketHistory(
  entityPk: string,
  ticketType?: OpsTicketType,
  entityType?: 'link' | 'device'
): Promise<OpsTicketsListResponse> {
  const params = new URLSearchParams({ entity_pk: entityPk, limit: '5' })
  if (ticketType) params.set('type', ticketType)
  if (entityType) params.set('entity_type', entityType)
  const res = await apiFetch(`/api/ops-tickets/history?${params}`)
  if (!res.ok) throw new Error(`Failed to fetch ops ticket history: ${res.status}`)
  const json = await res.json()
  return json.data ?? json
}

// Create a new incident ticket. Reporter fields are set server-side.
export async function createOpsTicket(input: CreateOpsTicketInput): Promise<OpsTicket> {
  const res = await apiFetch('/api/ops-tickets', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(input),
  })
  if (!res.ok) throw new Error(`Failed to create ops ticket: ${res.status}`)
  const json = await res.json()
  return json.data ?? json
}

// opsTicketUrl returns the Ops Management deep-link for a ticket.
export function opsTicketUrl(ticketId: string): string {
  return `https://doublezero.xyz/ops-management/tickets/${ticketId}`
}

export interface OpsAssignee {
  value: string   // contributor code, e.g. "rox" or "dz_malbeclabs"
  label: string   // display name, e.g. "RockawayX"
  type?: string   // "admin" | "contributor"
  pubkey?: string // Solana pubkey, joined server-side from ClickHouse
}

export interface OpsAssigneesResponse {
  assignees: OpsAssignee[]
}

// Fetch the list of valid assignees from the Ops Management API.
export async function fetchOpsAssignees(): Promise<OpsAssigneesResponse> {
  const res = await apiFetch('/api/ops-tickets/assignees')
  if (!res.ok) throw new Error(`Failed to fetch assignees: ${res.status}`)
  const json = await res.json()
  // Handle plain array, {data: [...]}, and {assignees: [...]} shapes
  if (Array.isArray(json)) return { assignees: json }
  if (Array.isArray(json.data)) return { assignees: json.data }
  return json
}
