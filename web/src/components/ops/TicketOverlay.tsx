// web/src/components/ops/TicketOverlay.tsx
import { useState, useRef, useEffect, useCallback } from 'react'
import { opsTicketUrl } from '@/lib/ops-api'

export interface TicketWindow {
  startAt: string       // ISO timestamp
  endAt?: string        // ISO timestamp; omit/undefined = ongoing
  type: 'incident' | 'maintenance'
  id: string            // UUID (used for key + ops URL)
  humanReadableId: string
  title: string
  status: string
  entityName?: string
  slackUrl?: string  // Slack message/thread URL, shown when present
}

interface TicketOverlayProps {
  windows: TicketWindow[]
  rangeStartMs: number  // unix ms
  rangeEndMs: number    // unix ms
  minWidthMs?: number   // minimum strip width; use visual bar span (bar.spanSeconds * 1000)
}

// Fully opaque — no alpha blending with health bar colors underneath
const COLORS = {
  incident: {
    bg: 'rgb(153,27,27)',
    border: 'rgb(120,20,20)',
    fg: 'rgb(255,255,255)',
  },
  maintenance: {
    bg: 'rgb(29,78,216)',
    border: 'rgb(20,55,180)',
    fg: 'rgb(255,255,255)',
  },
}

const LABELS = {
  incident:    { min: 'I', med: 'INC', long: 'Incident' },
  maintenance: { min: 'M', med: 'MNT', long: 'Maintenance' },
}
const MED_MIN = 28, LONG_MIN = 80

interface TicketCluster {
  startMs: number
  endMs: number
  tickets: TicketWindow[]  // all tickets are the same type within a cluster
}

interface ClusterStripProps {
  cluster: TicketCluster
  leftPct: number
  widthPct: number
  zIndex: number
}

function ClusterStrip({ cluster, leftPct, widthPct, zIndex }: ClusterStripProps) {
  const { tickets } = cluster
  const isSingle = tickets.length === 1
  const first = tickets[0]
  const type = first.type  // all tickets in a cluster share the same type

  const colors = COLORS[type]
  const L = LABELS[type]

  const [label, setLabel] = useState(L.min)
  const [tooltipVisible, setTooltipVisible] = useState(false)
  const [tooltipSide, setTooltipSide] = useState<'left' | 'right'>('left')
  const stripRef = useRef<HTMLDivElement>(null)
  const hideTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    const el = stripRef.current
    if (!el) return
    const ro = new ResizeObserver(() => {
      const px = el.getBoundingClientRect().width
      if (isSingle) {
        setLabel(px >= LONG_MIN ? L.long : px >= MED_MIN ? L.med : L.min)
      } else {
        const multi = `${tickets.length}${type === 'incident' ? 'I' : 'M'}`
        setLabel(px >= MED_MIN ? multi : px >= 10 ? `${tickets.length}` : '+')
      }
    })
    ro.observe(el)
    return () => ro.disconnect()
  }, [L, isSingle, tickets.length, type])

  const tooltipRight = leftPct + widthPct / 2 > 70

  const showTooltip = useCallback(() => {
    if (hideTimerRef.current) clearTimeout(hideTimerRef.current)
    setTooltipVisible(true)
    setTooltipSide(tooltipRight ? 'right' : 'left')
  }, [tooltipRight])

  const scheduleHide = useCallback(() => {
    hideTimerRef.current = setTimeout(() => setTooltipVisible(false), 600)
  }, [])

  const cancelHide = useCallback(() => {
    if (hideTimerRef.current) clearTimeout(hideTimerRef.current)
  }, [])

  const startSec = first.startAt
    ? new Date(first.startAt).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
    : '—'

  return (
    <div
      className="absolute top-0"
      style={{
        left: `${leftPct}%`, width: `${widthPct}%`,
        height: '24px', zIndex: tooltipVisible ? 100 : zIndex,
        pointerEvents: 'none', overflow: 'visible',
      }}
    >
      {/* Transparent bridge above — keeps tooltip reachable when mousing upward */}
      <div
        className="absolute"
        style={{ bottom: '100%', left: '-4px', right: '-4px', height: '24px', pointerEvents: 'auto' }}
        onMouseEnter={cancelHide}
        onMouseLeave={scheduleHide}
      />

      {/* Badge strip — overlays bottom of health bar; opaque so no color blending */}
      <div
        ref={stripRef}
        className="absolute"
        style={{
          left: 0, right: 0, bottom: '1px', height: '13px',
          borderRadius: '2px', overflow: 'hidden',
          background: colors.bg,
          border: `1px solid ${colors.border}`,
          display: 'flex', alignItems: 'center',
          padding: '0 4px',
          pointerEvents: 'auto',
        }}
        onMouseEnter={showTooltip}
        onMouseLeave={scheduleHide}
      >
        <span style={{
          fontSize: '9px', fontWeight: 700, lineHeight: 1,
          color: colors.fg, whiteSpace: 'nowrap', overflow: 'hidden',
        }}>
          {label}
        </span>
      </div>

      {/* Hover tooltip */}
      {tooltipVisible && (
        <div
          className="absolute bg-popover border border-border shadow-lg p-2.5 text-xs z-50"
          style={{
            bottom: 'calc(100% + 8px)',
            ...(tooltipSide === 'right' ? { right: 0 } : { left: 0 }),
            pointerEvents: 'auto',
            minWidth: '200px',
            maxWidth: isSingle ? '280px' : '300px',
          }}
          onMouseEnter={cancelHide}
          onMouseLeave={() => setTooltipVisible(false)}
        >
          {isSingle ? (
            <>
              <div className="font-mono text-[11px] text-blue-600 dark:text-blue-300 mb-1">{first.humanReadableId}</div>
              <div className="text-foreground font-medium mb-1 leading-tight">{first.title}</div>
              <div className="text-muted-foreground space-y-0.5">
                <div>Type: <span className="text-foreground capitalize">{first.type}</span></div>
                <div>Status: <span className="text-foreground">{first.status}</span></div>
                <div>Started: <span className="text-foreground">{startSec}</span></div>
                {first.entityName && <div>Entity: <span className="text-foreground font-mono text-[10px]">{first.entityName}</span></div>}
              </div>
              <a
                href={opsTicketUrl(first.id)}
                target="_blank"
                rel="noreferrer"
                className="block mt-2 text-blue-600 dark:text-blue-300 hover:underline"
              >
                View in ops management →
              </a>
              {first.slackUrl && (
                <a
                  href={first.slackUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="block mt-1 text-[#4A9CC7] hover:underline"
                >
                  View Slack thread →
                </a>
              )}
            </>
          ) : (
            <>
              <div className="text-muted-foreground text-[10px] mb-1.5">
                {tickets.length} {type}{tickets.length !== 1 ? 's' : ''} in this window
              </div>
              <div>
                {tickets.map((t, ti) => (
                  <div key={t.id} className={`py-1.5 ${ti > 0 ? 'border-t border-border/30' : ''}`}>
                    <div className="flex items-center gap-1.5">
                      <span className={`text-[9px] font-bold px-1 py-0.5 rounded shrink-0 ${
                        t.type === 'incident' ? 'bg-red-500/15 text-red-700 dark:bg-red-900/60 dark:text-red-200' : 'bg-blue-500/15 text-blue-700 dark:bg-blue-900/60 dark:text-blue-200'
                      }`}>
                        {t.type === 'incident' ? 'I' : 'M'}
                      </span>
                      <span className="font-mono text-[10px] text-blue-600 dark:text-blue-300 shrink-0">{t.humanReadableId}</span>
                      <a
                        href={opsTicketUrl(t.id)}
                        target="_blank"
                        rel="noreferrer"
                        className="text-blue-600 dark:text-blue-300 hover:underline text-[10px] ml-auto shrink-0"
                        title="Open in ops management"
                      >
                        ↗
                      </a>
                      {t.slackUrl && (
                        <a
                          href={t.slackUrl}
                          target="_blank"
                          rel="noreferrer"
                          className="text-[#4A9CC7] hover:underline text-[10px] shrink-0"
                          title="Open Slack thread"
                        >
                          #
                        </a>
                      )}
                    </div>
                    <div
                      className="text-[10px] text-foreground leading-snug mt-0.5 overflow-hidden"
                      style={{ maxWidth: '240px', display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical' }}
                    >
                      {t.title}
                    </div>
                    <div className="text-[9px] text-muted-foreground mt-0.5">{t.status}</div>
                  </div>
                ))}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  )
}

function buildClusters(
  windows: TicketWindow[],
  rangeStartMs: number,
  rangeEndMs: number,
  minWidthMs: number | undefined,
): TicketCluster[] {
  const expanded = windows.flatMap(w => {
    const rawStart = new Date(w.startAt).getTime()
    const rawEnd = w.endAt ? new Date(w.endAt).getTime() : rangeEndMs
    const start = Math.max(rawStart, rangeStartMs)
    let end = Math.min(rawEnd, rangeEndMs)
    if (end <= start) return []
    if (minWidthMs && end - start < minWidthMs) {
      end = Math.min(start + minWidthMs, rangeEndMs)
    }
    if (end <= start) return []
    return [{ w, startMs: start, endMs: end }]
  })

  expanded.sort((a, b) => a.startMs - b.startMs)

  const clusters: TicketCluster[] = []
  for (const { w, startMs, endMs } of expanded) {
    const last = clusters[clusters.length - 1]
    if (last && startMs < last.endMs) {
      last.endMs = Math.max(last.endMs, endMs)
      last.tickets.push(w)
    } else {
      clusters.push({ startMs, endMs, tickets: [w] })
    }
  }
  return clusters
}

/**
 * Renders ticket badge strips overlaid on the bottom of a health bar zone.
 *
 * Incidents and maintenance are clustered independently, then rendered in layers:
 * maintenance underneath (z-index 3), incidents on top (z-index 4). Where a short
 * incident overlaps a long maintenance window the incident strip covers only its
 * actual duration — the maintenance strip remains visible for the rest.
 */
export function TicketOverlay({ windows, rangeStartMs, rangeEndMs, minWidthMs }: TicketOverlayProps) {
  const rangeDuration = rangeEndMs - rangeStartMs
  if (rangeDuration <= 0 || windows.length === 0) return null

  const incidentClusters = buildClusters(
    windows.filter(w => w.type === 'incident'),
    rangeStartMs, rangeEndMs, minWidthMs,
  )
  const maintenanceClusters = buildClusters(
    windows.filter(w => w.type === 'maintenance'),
    rangeStartMs, rangeEndMs, minWidthMs,
  )

  function renderCluster(cluster: TicketCluster, zIndex: number) {
    const leftPct = ((cluster.startMs - rangeStartMs) / rangeDuration) * 100
    const widthPct = ((cluster.endMs - cluster.startMs) / rangeDuration) * 100
    const safeLeft = Math.max(0, Math.min(leftPct, 100 - widthPct))
    return (
      <ClusterStrip
        key={cluster.tickets[0].id}
        cluster={cluster}
        leftPct={safeLeft}
        widthPct={widthPct}
        zIndex={zIndex}
      />
    )
  }

  return (
    <>
      {/* Maintenance first (z 3), incidents on top (z 4) — incident covers only its
          own duration, maintenance strip remains visible for the rest */}
      {maintenanceClusters.map(c => renderCluster(c, 3))}
      {incidentClusters.map(c => renderCluster(c, 4))}
    </>
  )
}
