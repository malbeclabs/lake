// web/src/components/ops/IncidentBadge.tsx
import { type OpsTicket, opsTicketUrl } from '@/lib/ops-api'

interface IncidentBadgeProps {
  ticket: OpsTicket
}

const severityClass: Record<string, string> = {
  sev1: 'bg-red-500/20 text-red-300',
  sev2: 'bg-orange-500/20 text-orange-300',
  sev3: 'bg-gray-400/20 text-gray-300',
}

const statusClass = 'bg-blue-500/20 text-blue-300'

function timeAgo(isoStr: string): string {
  const diffMs = Date.now() - new Date(isoStr).getTime()
  const mins = Math.floor(diffMs / 60_000)
  if (mins < 60) return `${mins}m ago`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}h ${mins % 60}m ago`
  return `${Math.floor(hrs / 24)}d ago`
}

/**
 * Badge shown on link/device rows when there is an active incident.
 * Hover reveals a sticky tooltip with full details (ops users only —
 * the parent is responsible for only rendering this component for ops users).
 */
export function IncidentBadge({ ticket }: IncidentBadgeProps) {
  return (
    <span
      className="relative inline-block text-[10px] px-1.5 py-0.5 font-medium cursor-default bg-red-500/15 text-red-300 [&:hover_.incident-tip]:block"
      style={{ zIndex: 1 }}
    >
      Incident
      {/* Tooltip — stays open because it is a DOM child of the badge */}
      <div
        className="incident-tip hidden absolute top-full left-0 z-50 min-w-[250px] bg-popover border border-border p-2.5 shadow-xl text-xs"
        style={{ pointerEvents: 'auto' }}
      >
        <a
          href={opsTicketUrl(ticket.id)}
          target="_blank"
          rel="noreferrer"
          className="font-mono text-[11px] text-blue-300 hover:underline block mb-1"
          onClick={(e) => e.stopPropagation()}
        >
          {ticket.human_readable_id} ↗
        </a>
        <div className="font-medium mb-2">{ticket.title}</div>
        <div className="flex gap-1.5 mb-2">
          {ticket.severity && (
            <span className={`text-[10px] px-1.5 py-0.5 font-medium ${severityClass[ticket.severity] ?? ''}`}>
              {ticket.severity}
            </span>
          )}
          <span className={`text-[10px] px-1.5 py-0.5 font-medium ${statusClass}`}>
            {ticket.status}
          </span>
        </div>
        <div className="border-t border-border pt-1.5 text-muted-foreground">
          Started {ticket.created_at ? timeAgo(ticket.created_at) : '—'}
          {ticket.slack_message_url && (
            <a
              href={ticket.slack_message_url}
              target="_blank"
              rel="noreferrer"
              className="text-blue-300 hover:underline block mt-1"
              onClick={(e) => e.stopPropagation()}
            >
              Slack ↗
            </a>
          )}
        </div>
      </div>
    </span>
  )
}
