import { AlertTriangle, TrendingUp } from 'lucide-react'
import type { ReactNode } from 'react'
import type { ShredClientSeat } from '@/lib/api'
import { balanceDollars, deriveStatus, runwayEpochs } from './subscription-status'

interface ConsoleStatStripProps {
  seats: ShredClientSeat[]
  currentEpoch: number
}

export function ConsoleStatStrip({ seats, currentEpoch }: ConsoleStatStripProps) {
  const stats = computeStats(seats, currentEpoch)

  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 mb-5">
      <StatCard
        label="Active subscriptions"
        value={stats.activeCount.toString()}
        delta={stats.activeCount > 0 ? <span className="text-green-400 inline-flex items-center gap-1"><TrendingUp className="h-3 w-3" /> live</span> : null}
      />
      <StatCard
        label="Seats in use"
        value={
          <span>
            {stats.seatsActive}{' '}
            <span className="text-[13px] font-normal text-muted-foreground">/ {stats.seatsTotal} total</span>
          </span>
        }
        delta={`${stats.utilizationPct}% utilization`}
      />
      <StatCard
        label="Run rate"
        value={
          <span>
            ${stats.runRateDollars.toLocaleString()}{' '}
            <span className="text-[13px] font-normal text-muted-foreground">/ epoch</span>
          </span>
        }
        delta={`≈ $${Math.round(stats.runRateDollars * 15).toLocaleString()} / month`}
      />
      <StatCard
        warn
        label={<><AlertTriangle className="h-3 w-3" /> Escrow &lt; 1 epoch</>}
        value={stats.lowCount.toString()}
        delta={stats.lowCount > 0 ? 'needs deposit' : 'all funded'}
      />
    </div>
  )
}

function StatCard({
  label, value, delta, warn = false,
}: {
  label: ReactNode
  value: ReactNode
  delta?: ReactNode
  warn?: boolean
}) {
  return (
    <div
      className={`rounded-xl border px-4 py-3.5 ${
        warn
          ? 'border-amber-500/35 bg-gradient-to-b from-amber-500/5 to-card'
          : 'border-border bg-card'
      }`}
    >
      <div className={`mb-1.5 flex items-center gap-1.5 text-xs ${warn ? 'text-amber-400' : 'text-muted-foreground'}`}>
        {label}
      </div>
      <div className={`text-2xl font-semibold tracking-tight tabular-nums ${warn ? 'text-amber-400' : ''}`}>
        {value}
      </div>
      {delta != null && (
        <div className="mt-1 flex items-center gap-1 text-[11.5px] text-muted-foreground">{delta}</div>
      )}
    </div>
  )
}

function computeStats(seats: ShredClientSeat[], currentEpoch: number) {
  let activeCount = 0
  let lowCount = 0
  let runRateDollars = 0
  let seatsActive = 0
  for (const s of seats) {
    const status = deriveStatus(s, currentEpoch)
    if (status === 'active' || status === 'low') runRateDollars += s.price_per_epoch_dollars
    if (status === 'active') { activeCount++; seatsActive++ }
    if (status === 'low')    { activeCount++; seatsActive++; lowCount++ }
    if (status === 'expired' && balanceDollars(s) > 0 && runwayEpochs(s) < 1) lowCount++
  }
  const seatsTotal = seats.length
  const utilizationPct = seatsTotal > 0 ? Math.round((seatsActive / seatsTotal) * 100) : 0
  return { activeCount, lowCount, runRateDollars, seatsActive, seatsTotal, utilizationPct }
}
