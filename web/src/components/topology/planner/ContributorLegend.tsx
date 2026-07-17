import type { ContributorColorInfo } from './contributor-colors'

const MAX_ROWS = 24

// Compact legend for the planner map's contributor color overlay: a color swatch,
// code, and device/link counts per contributor, matching the look of the
// topology map's ContributorsOverlayPanel list rows.
export function ContributorLegend({ contributors }: { contributors: ContributorColorInfo[] }) {
  // A contributor that appears only as a link's non-owning side (noted for its
  // code but never counted as a device or link owner) would otherwise show a
  // bare "0d 0l" row, so drop rows with nothing to report.
  const visible = contributors.filter((c) => c.deviceCount > 0 || c.linkCount > 0)
  if (visible.length === 0) return null

  const shown = visible.slice(0, MAX_ROWS)
  const extra = visible.length - shown.length

  return (
    <div className="bg-card/90 border border-border rounded-md p-2 text-xs w-48 max-h-72 overflow-y-auto">
      <div className="font-medium mb-1.5">Contributors ({visible.length})</div>
      <div className="space-y-0.5">
        {shown.map((c) => (
          <div key={c.pk} className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-1.5 min-w-0">
              <span
                className="w-3 h-3 rounded-full flex-shrink-0"
                style={{ backgroundColor: c.color }}
              />
              <span className="truncate" title={c.code}>
                {c.code}
              </span>
            </div>
            <span className="text-muted-foreground flex-shrink-0">
              {c.deviceCount}d {c.linkCount}l
            </span>
          </div>
        ))}
      </div>
      {extra > 0 && <div className="text-muted-foreground mt-1">+{extra} more</div>}
    </div>
  )
}
