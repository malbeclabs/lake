// Formatting and color helpers shared by StatCard and plain-text stats (MiniStat).
// Kept in a non-component module so the component files stay fast-refresh clean
// (a file may not export both a component and plain helpers).

// Color for a delta given the metric's good direction. Green marks movement in
// the good direction, red the bad direction; a neutral metric stays muted.
export function deltaColorClass(delta: number, goodDirection: 'up' | 'down' | 'neutral'): string {
  if (goodDirection === 'neutral') return 'text-muted-foreground'
  const good = goodDirection === 'up' ? delta > 0 : delta < 0
  return good ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'
}

// Color for a graded value (tone). Kept separate from deltaColorClass so the
// value grade and the delta arrow can be colored independently.
export function toneColorClass(tone: 'good' | 'warn' | 'bad'): string {
  switch (tone) {
    case 'good':
      return 'text-green-600 dark:text-green-400'
    case 'warn':
      return 'text-amber-600 dark:text-amber-400'
    case 'bad':
      return 'text-red-600 dark:text-red-400'
  }
}

// Format a delta with a leading sign, as a percent or percentage points.
export function formatDelta(delta: number, unit: 'pct' | 'pp' = 'pct'): string {
  const sign = delta >= 0 ? '+' : ''
  if (unit === 'pp') return `${sign}${delta.toFixed(1)} pp`
  return `${sign}${delta.toFixed(2)}%`
}
