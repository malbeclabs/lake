// Inline SVG only. The repo has deliberately migrated off charting libraries for
// small visuals, and a polyline needs no dependency.

/**
 * Builds an SVG `points` string, scaling values to fit width x height. Zero is
 * treated as "no data" and omitted, so gaps break the line rather than plunging
 * it to the floor and implying a latency collapse that never happened.
 *
 * `scaleAgainst` lets two series share one scale so the gap between them is
 * readable; it defaults to the series itself.
 */
export function sparklinePoints(
  values: number[],
  width: number,
  height: number,
  scaleAgainst?: number[]
): string {
  const present = values.map((v, i) => ({ v, i })).filter((p) => p.v > 0)
  if (present.length === 0) return ''

  const scaleVals = (scaleAgainst ?? values).filter((v) => v > 0)
  if (scaleVals.length === 0) return ''
  const min = Math.min(...scaleVals)
  const max = Math.max(...scaleVals)
  const span = max - min
  const lastIdx = values.length - 1

  return present
    .map(({ v, i }) => {
      const x = lastIdx === 0 ? 0 : (i / lastIdx) * width
      const y = span === 0 ? height / 2 : height - ((v - min) / span) * height
      return `${round(x)},${round(y)}`
    })
    .join(' ')
}

function round(n: number): number {
  return Math.round(n * 100) / 100
}

type SparklineProps = {
  dz: number[]
  internet: number[]
  width?: number
  height?: number
  className?: string
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
  const dzPoints = sparklinePoints(dz, width, height, combined)
  const inetPoints = sparklinePoints(internet, width, height, combined)

  if (!dzPoints && !inetPoints) {
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
      {inetPoints && (
        <polyline
          points={inetPoints}
          fill="none"
          stroke="currentColor"
          strokeWidth="1.5"
          className="text-muted-foreground/50"
        />
      )}
      {dzPoints && (
        <polyline
          points={dzPoints}
          fill="none"
          stroke="currentColor"
          strokeWidth="1.5"
          className="text-cyan-400"
        />
      )}
    </svg>
  )
}
