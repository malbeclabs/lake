// web/src/components/ops/OpsPanel.tsx
import { useState, useMemo } from 'react'
import { useTicketsForEntity, useOpsTicketHistory } from '@/hooks/use-ops-tickets'
import { useIsOpsUser } from '@/hooks/use-is-ops-user'
import { opsTicketUrl, type OpsTicket, type OpsTicketType } from '@/lib/ops-api'

interface OpsPanelProps {
  entityPk: string
  entityCode: string
  entityType: 'link' | 'device'
  contributorCode: string
  isDown: boolean
  downSince?: string   // ISO timestamp or human string
  onCreateIncident: () => void
}

const severityClass: Record<string, string> = {
  sev1: 'bg-red-500/20 text-red-300',
  sev2: 'bg-orange-500/20 text-orange-300',
  sev3: 'bg-gray-400/20 text-gray-300',
}

function HistoryRow({ ticket }: { ticket: OpsTicket }) {
  const now = useMemo(() => Date.now(), [])
  const age = ticket.updated_at
    ? Math.floor((now - new Date(ticket.updated_at).getTime()) / 86_400_000) + 'd ago'
    : '—'

  const borderColor =
    ticket.type === 'maintenance'
      ? 'border-l-blue-500/50'
      : ticket.severity === 'sev1'
      ? 'border-l-red-500/50'
      : ticket.severity === 'sev2'
      ? 'border-l-orange-500/50'
      : 'border-l-gray-400/40'

  return (
    <div className={`flex items-baseline gap-2.5 px-3.5 py-1.5 border-b border-border last:border-b-0 border-l-2 ${borderColor} text-xs`}>
      <a
        href={opsTicketUrl(ticket.id)}
        target="_blank"
        rel="noreferrer"
        className="font-mono text-[11px] text-blue-300 hover:underline shrink-0"
      >
        {ticket.human_readable_id} ↗
      </a>
      <span className="flex-1 min-w-0 text-muted-foreground truncate">{ticket.title}</span>
      <div className="flex gap-1.5 items-center shrink-0 text-muted-foreground">
        {ticket.severity && (
          <span className={`text-[10px] px-1.5 py-0.5 font-medium ${severityClass[ticket.severity]}`}>
            {ticket.severity}
          </span>
        )}
        {ticket.type === 'maintenance' && (
          <span className="text-[10px] px-1.5 py-0.5 font-medium bg-blue-500/20 text-blue-300">Maintenance</span>
        )}
        <span className="text-[10px] px-1.5 py-0.5 font-medium bg-gray-400/15 text-gray-400">
          {ticket.status}
        </span>
        <span>{age}</span>
      </div>
    </div>
  )
}

function HistoryPanel({ entityPk, entityType }: { entityPk: string; entityType: 'link' | 'device' }) {
  const [tab, setTab] = useState<OpsTicketType>('incident')
  const { data } = useOpsTicketHistory(entityPk, tab, entityType)
  const tickets = data?.tickets ?? []

  const tabClass = (t: OpsTicketType) =>
    `text-[11px] uppercase tracking-wide px-3.5 py-1.5 border-none cursor-pointer font-sans transition-colors ${
      tab === t
        ? 'bg-muted/30 text-foreground'
        : 'bg-transparent text-muted-foreground hover:text-foreground'
    }`

  const viewAllLabel = tab === 'incident'
    ? 'View all incidents on Ops Management ↗'
    : 'View all maintenance on Ops Management ↗'

  return (
    <div className="border border-border">
      <div className="flex items-center justify-between bg-muted/50 border-b border-border">
        <span className="text-[11px] uppercase tracking-wide text-muted-foreground px-3.5 py-1.5">
          Past tickets{' '}
          <span className="normal-case tracking-normal text-[10px] text-muted-foreground/60">
            5 most recent
          </span>
        </span>
        <div className="flex border-l border-border">
          <button className={tabClass('incident')} onClick={() => setTab('incident')}>Incidents</button>
          <button className={tabClass('maintenance')} onClick={() => setTab('maintenance')}>Maintenance</button>
        </div>
      </div>

      {tickets.length === 0 ? (
        <div className="px-3.5 py-2 text-xs text-muted-foreground">No past {tab}s found.</div>
      ) : (
        <>
          {tickets.map(t => <HistoryRow key={t.id} ticket={t} />)}
          <a
            href="https://doublezero.xyz/ops-management"
            target="_blank"
            rel="noreferrer"
            className="block text-right px-3.5 py-1.5 text-[11px] text-blue-300 hover:underline border-t border-border"
          >
            {viewAllLabel}
          </a>
        </>
      )}
    </div>
  )
}

export function OpsPanel({
  entityPk, entityType, isDown, onCreateIncident,
}: OpsPanelProps) {
  const isOpsUser = useIsOpsUser()
  const tickets = useTicketsForEntity(entityPk)
  const now = useMemo(() => Date.now(), [])

  const activeTickets = tickets // already filtered to active by the hook
  const hasActiveIncident = activeTickets.some(t => t.type === 'incident')

  // Nothing to show for non-ops users
  if (!isOpsUser) return null

  return (
    <>
      {/* Active tickets panel */}
      {activeTickets.length > 0 && (
        <div className="border border-border">
          <div className="px-4 py-1.5 bg-muted/50 border-b border-border text-[11px] uppercase tracking-wide text-muted-foreground">
            Active tickets
          </div>
          <div className="p-3 space-y-2.5">
            {activeTickets.map(ticket => (
              <div
                key={ticket.id}
                className={`flex items-start gap-3 p-2.5 border border-border border-l-2 bg-muted/20 ${
                  ticket.type === 'incident' ? 'border-l-red-500' : 'border-l-blue-500/90'
                }`}
              >
                <div className="flex-1 min-w-0">
                  <div className="text-[13px] font-medium mb-1 truncate">{ticket.title}</div>
                  <div className="flex flex-wrap gap-1.5 items-center text-[11px] text-muted-foreground">
                    <a
                      href={opsTicketUrl(ticket.id)}
                      target="_blank"
                      rel="noreferrer"
                      className="font-mono text-[11px] text-blue-300 hover:underline"
                    >
                      {ticket.human_readable_id} ↗
                    </a>
                    {ticket.severity && (
                      <span className={`text-[10px] px-1.5 py-0.5 font-medium ${severityClass[ticket.severity]}`}>
                        {ticket.severity}
                      </span>
                    )}
                    <span className="text-[10px] px-1.5 py-0.5 font-medium bg-blue-500/20 text-blue-300">
                      {ticket.status}
                    </span>
                    {ticket.start_at && (
                      <span>· Started {Math.floor((now - new Date(ticket.start_at).getTime()) / 60_000)}m ago</span>
                    )}
                    {!ticket.start_at && ticket.created_at && (
                      <span>· {Math.floor((now - new Date(ticket.created_at).getTime()) / 60_000)}m ago</span>
                    )}
                  </div>
                </div>
                {ticket.slack_message_url && (
                  <a
                    href={ticket.slack_message_url}
                    target="_blank"
                    rel="noreferrer"
                    className="text-[11px] text-blue-300 hover:underline shrink-0"
                  >
                    Slack ↗
                  </a>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Create incident prompt — shown when down with no active incident */}
      {isDown && !hasActiveIncident && (
        <div className="flex items-center justify-between px-4 py-2.5 border border-border bg-red-500/[0.04] border-l border-red-500/30">
          <span className="text-xs text-muted-foreground">
            Link is down — no open incident
          </span>
          <button
            className="text-[11px] font-medium px-2.5 py-1 border border-gray-400/40 text-muted-foreground hover:text-foreground hover:border-gray-400/70 transition-colors"
            onClick={onCreateIncident}
          >
            Create incident
          </button>
        </div>
      )}

      {/* History panel */}
      <HistoryPanel entityPk={entityPk} entityType={entityType} />
    </>
  )
}
