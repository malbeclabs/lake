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

  const contextColor = isDark ? '#475569' : '#cbd5e1'

  return (
    <svg
      viewBox={`0 0 ${PREVIEW_VIEW_W} ${PREVIEW_VIEW_H}`}
      className="h-full w-full rounded bg-muted/40"
      role="img"
      aria-label="Plan change preview"
    >
      {geometry.context.links.map((l, i) => (
        <line
          key={`ctx-link-${i}`}
          x1={l.x1}
          y1={l.y1}
          x2={l.x2}
          y2={l.y2}
          stroke={contextColor}
          strokeWidth={0.75}
          strokeOpacity={0.35}
        />
      ))}
      {geometry.context.devices.map((d, i) => (
        <circle
          key={`ctx-device-${i}`}
          cx={d.x}
          cy={d.y}
          r={1.5}
          fill={contextColor}
          fillOpacity={0.5}
        />
      ))}
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
      {geometry.metros.map((m) => (
        <text
          key={m.code}
          x={m.x}
          y={m.y - 5}
          textAnchor="middle"
          fontSize={8}
          fill={isDark ? '#e2e8f0' : '#334155'}
          stroke={isDark ? '#0f172a' : '#f8fafc'}
          strokeWidth={2}
          paintOrder="stroke"
        >
          {m.code}
        </text>
      ))}
    </svg>
  )
}
