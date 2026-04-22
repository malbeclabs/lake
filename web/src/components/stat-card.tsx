import { useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'

interface StatCardPeer {
  label: string
  value: number | undefined
  href?: string
}

interface StatCardProps {
  label: string
  value: number | undefined
  format: 'number' | 'stake' | 'bandwidth' | 'percent'
  decimals?: number // Override default decimal places for the format
  delta?: number // Optional delta value to show change (percentage points for percent format)
  max?: number // Optional maximum to display as "value / max"
  href?: string // Optional link to entity listing page
  peer?: StatCardPeer // Optional second stat shown side-by-side in the same card
}

function useAnimatedNumber(target: number | undefined, duration = 500) {
  const [current, setCurrent] = useState<number | undefined>(undefined)
  const prevRef = useRef<number | undefined>(undefined)

  useEffect(() => {
    if (target === undefined) return

    const start = prevRef.current ?? target
    const startTime = performance.now()

    const animate = (time: number) => {
      const elapsed = time - startTime
      const progress = Math.min(elapsed / duration, 1)
      // Ease-out cubic
      const eased = 1 - Math.pow(1 - progress, 3)
      const value = start + (target - start) * eased
      setCurrent(value)

      if (progress < 1) {
        requestAnimationFrame(animate)
      } else {
        prevRef.current = target
      }
    }

    requestAnimationFrame(animate)
  }, [target, duration])

  return current
}

function formatValue(
  value: number | undefined,
  format: 'number' | 'stake' | 'bandwidth' | 'percent',
  decimals?: number,
): string {
  if (value === undefined) return '—'

  switch (format) {
    case 'stake': {
      // Convert to millions of SOL
      const millions = value / 1_000_000
      if (millions >= 1) {
        return `${millions.toFixed(decimals ?? 1)}M`
      }
      // Less than 1M, show in K
      const thousands = value / 1_000
      return `${thousands.toFixed(decimals ?? 0)}K`
    }
    case 'bandwidth': {
      const d = decimals ?? 1
      // Convert bps to Mbps, Gbps, or Tbps
      const gbps = value / 1_000_000_000
      if (gbps >= 1000) {
        return `${(gbps / 1000).toFixed(d)} Tbps`
      }
      if (gbps >= 1) {
        return `${gbps.toFixed(d)} Gbps`
      }
      // Less than 1 Gbps, show in Mbps
      const mbps = value / 1_000_000
      return `${mbps.toFixed(d)} Mbps`
    }
    case 'percent':
      return `${value.toFixed(decimals ?? 1)}%`
    case 'number':
    default:
      return value.toLocaleString('en-US', {
        maximumFractionDigits: decimals ?? 0,
      })
  }
}

function formatDelta(delta: number): string {
  const sign = delta >= 0 ? '+' : ''
  return `${sign}${delta.toFixed(2)}%`
}

function StatCardContent({
  label,
  value,
  format,
  decimals,
  delta,
  max,
}: Pick<StatCardProps, 'label' | 'value' | 'format' | 'decimals' | 'delta' | 'max'>) {
  const animatedValue = useAnimatedNumber(value)
  const isLoading = value === undefined
  const [showSkeleton, setShowSkeleton] = useState(false)

  useEffect(() => {
    if (isLoading) {
      const timer = setTimeout(() => setShowSkeleton(true), 150)
      return () => clearTimeout(timer)
    } else {
      setShowSkeleton(false)
    }
  }, [isLoading])

  const showDelta = delta !== undefined && delta !== 0

  return (
    <>
      <div className="text-2xl lg:text-3xl font-medium tabular-nums tracking-tight mb-1">
        {isLoading ? (
          showSkeleton ? (
            <span className="inline-block h-10 w-16 rounded bg-muted animate-pulse" />
          ) : (
            <span className="inline-block h-10 w-16" />
          )
        ) : (
          <span className="inline-flex items-baseline gap-2">
            <span className="tabular-nums">
              {formatValue(animatedValue, format, decimals)}
              {max !== undefined && (
                <span className="text-muted-foreground">/{formatValue(max, format, decimals)}</span>
              )}
            </span>
            {showDelta && (
              <span
                className={`text-sm font-normal ${delta > 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}
              >
                {formatDelta(delta)}
              </span>
            )}
          </span>
        )}
      </div>
      <div className="text-sm text-muted-foreground">{label}</div>
    </>
  )
}

export function StatCard({ label, value, format, decimals, delta, max, href, peer }: StatCardProps) {
  if (peer) {
    return (
      <div className="rounded-[0.3rem] bg-muted/50 p-2 lg:p-4 flex items-stretch divide-x divide-border">
        <div className="flex-1 text-center pr-2 lg:pr-4">
          {href ? (
            <Link to={href} className="block hover:text-foreground transition-colors">
              <StatCardContent label={label} value={value} format={format} decimals={decimals} delta={delta} max={max} />
            </Link>
          ) : (
            <StatCardContent label={label} value={value} format={format} decimals={decimals} delta={delta} max={max} />
          )}
        </div>
        <div className="flex-1 text-center pl-2 lg:pl-4">
          {peer.href ? (
            <Link to={peer.href} className="block hover:text-foreground transition-colors">
              <StatCardContent label={peer.label} value={peer.value} format={format} />
            </Link>
          ) : (
            <StatCardContent label={peer.label} value={peer.value} format={format} />
          )}
        </div>
      </div>
    )
  }

  const content = (
    <StatCardContent label={label} value={value} format={format} decimals={decimals} delta={delta} max={max} />
  )

  if (href) {
    return (
      <Link
        to={href}
        className="text-center block rounded-[0.3rem] bg-muted/50 hover:bg-muted transition-colors p-2 lg:p-4"
      >
        {content}
      </Link>
    )
  }

  return (
    <div className="text-center rounded-[0.3rem] bg-muted/50 hover:bg-muted transition-colors p-2 lg:p-4">
      {content}
    </div>
  )
}
