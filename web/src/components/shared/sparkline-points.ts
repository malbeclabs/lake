// Inline SVG only. The repo has deliberately migrated off charting libraries for
// small visuals, and a polyline needs no dependency.
//
// Kept out of sparkline.tsx so that file exports only its component: mixing
// component and non-component exports breaks Fast Refresh.

/**
 * Builds SVG `points` strings, scaling values to fit width x height — one
 * string per contiguous run of measured (> 0) values. Zero is treated as
 * "no data" and splits the series into a new run, so a gap breaks the line
 * rather than being bridged by a straight segment that implies a trend
 * across hours that were never measured.
 *
 * `scaleAgainst` lets two series share one scale so the gap between them is
 * readable; it defaults to the series itself.
 */
export function sparklinePoints(
  values: number[],
  width: number,
  height: number,
  scaleAgainst?: number[]
): string[] {
  const scaleVals = (scaleAgainst ?? values).filter((v) => v > 0)
  if (scaleVals.length === 0) return []
  const min = Math.min(...scaleVals)
  const max = Math.max(...scaleVals)
  const span = max - min
  const lastIdx = values.length - 1

  const segments: string[][] = []
  let current: string[] = []
  for (let i = 0; i < values.length; i++) {
    const v = values[i]
    if (v > 0) {
      const x = lastIdx === 0 ? 0 : (i / lastIdx) * width
      const y = span === 0 ? height / 2 : height - ((v - min) / span) * height
      current.push(`${round(x)},${round(y)}`)
    } else if (current.length > 0) {
      segments.push(current)
      current = []
    }
  }
  if (current.length > 0) segments.push(current)

  return segments.map((points) => points.join(' '))
}

function round(n: number): number {
  return Math.round(n * 100) / 100
}

/** Parses a single-point "x,y" segment string produced by sparklinePoints. */
export function parsePoint(segment: string): { x: number; y: number } {
  const [x, y] = segment.split(',').map(Number)
  return { x, y }
}
