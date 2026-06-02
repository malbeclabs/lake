import { opsTicketUrl } from '@/lib/ops-api'
import { useIsOpsUser } from '@/hooks/use-is-ops-user'
import { fmtDT } from './date-utils'
import type { MaintenanceEvent } from './use-maintenance-events'

export interface TooltipState { ev: MaintenanceEvent; x: number; y: number }

export function EventTooltipPanel({
  state,
  onMouseEnter,
  onMouseLeave,
}: {
  state: TooltipState
  onMouseEnter: () => void
  onMouseLeave: () => void
}) {
  const { ev, x, y } = state
  const isOpsUser = useIsOpsUser()
  const W = 288
  const left = x + 16 + W > window.innerWidth ? x - W - 8 : x + 16
  const top = y + 12

  return (
    <div
      className="fixed z-50 shadow-xl"
      style={{ left, top, width: W }}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
    >
      <div className="bg-popover border border-border rounded-md p-3 text-xs">
        <div className="font-medium text-sm leading-snug mb-0.5">{ev.title}</div>
        <div className="text-muted-foreground mb-2">{ev.contributorName}</div>

        <div className="space-y-1 border-t border-border/50 pt-2">
          <div className="flex gap-2">
            <span className="text-muted-foreground w-14 flex-shrink-0">Status</span>
            <span className="capitalize">{ev.status.replace(/-/g, ' ')}</span>
          </div>
          <div className="flex gap-2">
            <span className="text-muted-foreground w-14 flex-shrink-0">Start</span>
            <span>{fmtDT(ev.startAt)}</span>
          </div>
          <div className="flex gap-2">
            <span className="text-muted-foreground w-14 flex-shrink-0">End</span>
            <span>{fmtDT(ev.endAt)}</span>
          </div>
          {ev.affectedLinks.length > 0 && (
            <div className="flex gap-2">
              <span className="text-muted-foreground w-14 flex-shrink-0">Links</span>
              <span className="break-all">{ev.affectedLinks.map((l) => l.code).join(', ')}</span>
            </div>
          )}
          {ev.affectedDevices.length > 0 && (
            <div className="flex gap-2">
              <span className="text-muted-foreground w-14 flex-shrink-0">Devices</span>
              <span className="break-all">{ev.affectedDevices.map((d) => d.code).join(', ')}</span>
            </div>
          )}
        </div>

        {isOpsUser && (
          <div className="flex flex-col mt-2 pt-2 border-t border-border/50">
            <a
              href={opsTicketUrl(ev.id)}
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-600 dark:text-blue-300 hover:underline"
            >
              View in ops management →
            </a>
            {ev.slackUrl && (
              <a
                href={ev.slackUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-600 dark:text-blue-300 hover:underline"
              >
                View Slack thread →
              </a>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
