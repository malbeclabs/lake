// web/src/components/ops/IncidentBadge.tsx
import { useState, useRef, useCallback } from 'react'
import { createPortal } from 'react-dom'
import { type OpsTicket, opsTicketUrl } from '@/lib/ops-api'

interface IncidentBadgeProps {
  tickets: OpsTicket[]
}

const severityClass: Record<string, string> = {
  sev1: 'bg-red-500/15 text-red-700 dark:text-red-300',
  sev2: 'bg-orange-500/15 text-orange-700 dark:text-orange-300',
  sev3: 'bg-gray-400/15 text-gray-600 dark:text-gray-300',
}

function timeAgo(isoStr: string): string {
  const diffMs = Date.now() - new Date(isoStr).getTime()
  const mins = Math.floor(diffMs / 60_000)
  if (mins < 60) return `${mins}m ago`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}h ${mins % 60}m ago`
  const days = Math.floor(hrs / 24)
  if (days < 30) return `${days}d ago`
  return `${Math.floor(days / 30)}mo ago`
}

/**
 * Badge shown on link/device rows when there is at least one active incident.
 * Always renders a single "Incident" label; hover reveals a fixed-position portal
 * tooltip listing all open incidents (ops users only — parent is responsible for gating).
 */
export function IncidentBadge({ tickets }: IncidentBadgeProps) {
  const first = tickets[0]
  const badgeRef = useRef<HTMLSpanElement>(null)
  const [visible, setVisible] = useState(false)
  const [pos, setPos] = useState<{ top: number; left: number }>({ top: 0, left: 0 })
  const hideTimer = useRef<ReturnType<typeof setTimeout> | null>(null)

  const show = useCallback(() => {
    if (hideTimer.current) clearTimeout(hideTimer.current)
    if (badgeRef.current) {
      const r = badgeRef.current.getBoundingClientRect()
      setPos({ top: r.bottom + 4, left: r.left })
    }
    setVisible(true)
  }, [])

  const scheduleHide = useCallback(() => {
    hideTimer.current = setTimeout(() => setVisible(false), 200)
  }, [])

  const cancelHide = useCallback(() => {
    if (hideTimer.current) clearTimeout(hideTimer.current)
  }, [])

  if (!first) return null

  return (
    <>
      <span
        ref={badgeRef}
        className="inline-block text-[10px] px-1.5 py-0.5 font-medium cursor-default bg-red-500/15 text-red-700 dark:text-red-300"
        onMouseEnter={show}
        onMouseLeave={scheduleHide}
      >
        Incident
      </span>
      {visible && createPortal(
        <div
          className="fixed z-[9999] min-w-[250px] max-w-[300px] bg-popover border border-border p-2.5 shadow-xl text-xs"
          style={{ top: pos.top, left: pos.left }}
          onMouseEnter={cancelHide}
          onMouseLeave={scheduleHide}
          onClick={(e) => e.stopPropagation()}
        >
          {tickets.length === 1 ? (
            <>
              <a
                href={opsTicketUrl(first.id)}
                target="_blank"
                rel="noreferrer"
                className="font-mono text-[11px] text-blue-600 dark:text-blue-300 hover:underline block mb-1"
              >
                {first.human_readable_id} ↗
              </a>
              <div className="font-medium mb-2">{first.title}</div>
              <div className="flex gap-1.5 mb-2">
                {first.severity && (
                  <span className={`text-[10px] px-1.5 py-0.5 font-medium ${severityClass[first.severity] ?? ''}`}>
                    {first.severity}
                  </span>
                )}
                <span className="text-[10px] px-1.5 py-0.5 font-medium bg-blue-500/15 text-blue-700 dark:text-blue-300">
                  {first.status}
                </span>
              </div>
              <div className="border-t border-border pt-1.5 text-muted-foreground">
                Started {first.created_at ? timeAgo(first.created_at) : '—'}
                {first.slack_message_url && (
                  <a
                    href={first.slack_message_url}
                    target="_blank"
                    rel="noreferrer"
                    className="text-blue-600 dark:text-blue-300 hover:underline block mt-1"
                  >
                    Slack ↗
                  </a>
                )}
              </div>
            </>
          ) : (
            <>
              <div className="text-muted-foreground text-[10px] mb-1.5">
                {tickets.length} open incidents
              </div>
              {tickets.map((t, ti) => (
                <div key={t.id} className={`py-1.5 ${ti > 0 ? 'border-t border-border/50' : ''}`}>
                  <div className="flex items-center gap-1.5 mb-0.5">
                    <a
                      href={opsTicketUrl(t.id)}
                      target="_blank"
                      rel="noreferrer"
                      className="font-mono text-[11px] text-blue-600 dark:text-blue-300 hover:underline"
                    >
                      {t.human_readable_id} ↗
                    </a>
                    {t.severity && (
                      <span className={`text-[10px] px-1.5 py-0.5 font-medium ${severityClass[t.severity] ?? ''}`}>
                        {t.severity}
                      </span>
                    )}
                    <span className="text-[10px] px-1.5 py-0.5 font-medium bg-blue-500/15 text-blue-700 dark:text-blue-300">
                      {t.status}
                    </span>
                  </div>
                  <div className="text-muted-foreground truncate">{t.title}</div>
                  <div className="text-muted-foreground text-[10px] mt-0.5">
                    Started {t.created_at ? timeAgo(t.created_at) : '—'}
                    {t.slack_message_url && (
                      <a
                        href={t.slack_message_url}
                        target="_blank"
                        rel="noreferrer"
                        className="text-blue-600 dark:text-blue-300 hover:underline ml-2"
                      >
                        Slack ↗
                      </a>
                    )}
                  </div>
                </div>
              ))}
            </>
          )}
        </div>,
        document.body
      )}
    </>
  )
}
