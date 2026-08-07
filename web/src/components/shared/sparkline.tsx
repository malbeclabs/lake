// Inline SVG only. The repo has deliberately migrated off charting libraries for
// small visuals, and a polyline needs no dependency.

import { sparklinePoints, parsePoint } from './sparkline-points'

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
