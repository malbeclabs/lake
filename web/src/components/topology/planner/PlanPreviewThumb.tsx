// Non-interactive SVG thumbnail for a plan landing card. Deliberately NOT a
// MapLibre instance: browsers cap WebGL contexts around 16, and a landing page
// can list far more plans than that. An SVG is cheap to mount N times and needs
// no tiles -- a faint neutral background plus dots/lines is enough to recognize
// "oh, that's the Warsaw one" at a glance.
import type { PlanPreviewGeometry } from './plan-preview'
import { linkChangeStyle, deviceChangeStyle } from './change-styles'

// ViewBox size the geometry is projected into (see buildPlanPreview callers).
// Only the aspect ratio matters -- the SVG scales to fill its container.
export const PREVIEW_VIEW_W = 240
export const PREVIEW_VIEW_H = 140

export function PlanPreviewThumb({
  geometry,
  isDark,
}: {
  geometry: PlanPreviewGeometry | null
  isDark: boolean
}) {
  const isEmpty = !geometry || (geometry.devices.length === 0 && geometry.links.length === 0)

  if (isEmpty) {
    return (
      <div className="flex h-full w-full items-center justify-center rounded bg-muted/40 text-[11px] text-muted-foreground">
        Empty plan
      </div>
    )
  }

  return (
    <svg
      viewBox={`0 0 ${PREVIEW_VIEW_W} ${PREVIEW_VIEW_H}`}
      className="h-full w-full rounded bg-muted/40"
      role="img"
      aria-label="Plan change preview"
    >
      {geometry.links.map((l) => {
        const style = linkChangeStyle(l.state, isDark)
        return (
          <line
            key={l.key}
            x1={l.x1}
            y1={l.y1}
            x2={l.x2}
            y2={l.y2}
            stroke={style.color}
            strokeWidth={Math.max(1, style.weight / 2)}
            strokeOpacity={style.opacity}
            strokeDasharray={style.dashed ? '3 2' : undefined}
            strokeLinecap="round"
          />
        )
      })}
      {geometry.devices.map((d) => {
        const style = deviceChangeStyle(d.state, isDark)
        return (
          <circle
            key={d.key}
            cx={d.x}
            cy={d.y}
            r={3}
            fill={style.color}
            fillOpacity={style.opacity}
            stroke={style.ring ? (isDark ? '#f8fafc' : '#0f172a') : 'none'}
            strokeWidth={style.ring ? 1 : 0}
          />
        )
      })}
    </svg>
  )
}
