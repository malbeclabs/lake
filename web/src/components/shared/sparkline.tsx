// Inline SVG only. The repo has deliberately migrated off charting libraries for
// small visuals, and a polyline needs no dependency.

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
function parsePoint(segment: string): { x: number; y: number } {
  const [x, y] = segment.split(',').map(Number)
  return { x, y }
}

type SparklineProps = {
  dz: number[]
  internet: number[]
  width?: number
  height?: number
  className?: string
}

function SparklineSeries({ segments, className }: { segments: string[]; className: string }) {
  return (
    <>
      {segments.map((segment, i) => {
        if (!segment.includes(' ')) {
          // A run of exactly one point draws nothing as a polyline; mark it with a dot
          // instead so an isolated measurement is still visible rather than silently dropped.
          const { x, y } = parsePoint(segment)
          return <circle key={i} cx={x} cy={y} r="1.5" fill="currentColor" className={className} />
        }
        return (
          <polyline
            key={i}
            points={segment}
            fill="none"
            stroke="currentColor"
            strokeWidth="1.5"
            className={className}
          />
        )
      })}
    </>
  )
}

/** Two overlaid series: DoubleZero and the public internet, sharing one scale. */
export function Sparkline({
  dz,
  internet,
  width = 220,
  height = 40,
  className,
}: SparklineProps) {
  // Both series share a scale so the gap between them is readable.
  const combined = [...dz, ...internet]
  const dzSegments = sparklinePoints(dz, width, height, combined)
  const inetSegments = sparklinePoints(internet, width, height, combined)

  if (dzSegments.length === 0 && inetSegments.length === 0) {
    return (
      <div
        className={`flex items-center justify-center text-xs text-muted-foreground ${className ?? ''}`}
        style={{ width, height }}
      >
        no history
      </div>
    )
  }

  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      className={className}
      role="img"
      aria-label="7-day latency history: DoubleZero versus public internet"
    >
      <SparklineSeries segments={inetSegments} className="text-muted-foreground/50" />
      <SparklineSeries segments={dzSegments} className="text-cyan-600 dark:text-cyan-400" />
    </svg>
  )
}
