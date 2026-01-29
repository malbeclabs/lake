import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  TrendingUp,
  RefreshCw,
  AlertCircle,
  ChevronDown,
  Lock,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import {
  ResponsiveContainer,
  ComposedChart,
  LineChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
} from 'recharts'
import {
  fetchTimeline,
  fetchStakeHistory,
  fetchStakeChanges,
  fetchConfig,
  type TimelineEvent,
  type ValidatorEventDetails,
  type StakeHistoryPoint,
} from '@/lib/api'
import { useAuth } from '@/contexts/AuthContext'

type TimePeriod = 'day' | 'week' | 'month'

const timePeriodLabels: Record<TimePeriod, string> = {
  day: 'Day',
  week: 'Week',
  month: 'Month',
}

function Skeleton({ className }: { className?: string }) {
  return <div className={cn('animate-pulse bg-muted rounded', className)} />
}

function formatSol(sol: number): string {
  if (Math.abs(sol) >= 1_000_000) return `${(sol / 1_000_000).toFixed(2)}M`
  if (Math.abs(sol) >= 1_000) return `${(sol / 1_000).toFixed(1)}K`
  return sol.toLocaleString(undefined, { maximumFractionDigits: 0 })
}

function formatSolFull(sol: number): string {
  return sol.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function formatPercent(pct: number): string {
  return `${pct.toFixed(2)}%`
}

function formatDateShort(dateStr: string): string {
  const date = new Date(dateStr)
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}

// Stat card component
function StatCard({
  value,
  label,
  comparison,
  className,
}: {
  value: string
  label: string
  comparison?: string
  className?: string
}) {
  return (
    <div className={cn('border border-border rounded-lg p-4 bg-card', className)}>
      <div className="text-2xl sm:text-3xl font-semibold tabular-nums text-center">{value}</div>
      <div className="text-sm text-muted-foreground text-center mt-1">{label}</div>
      {comparison && (
        <div className="text-xs text-muted-foreground text-center mt-2">{comparison}</div>
      )}
    </div>
  )
}

// Time period selector
function TimePeriodSelector({
  value,
  onChange,
}: {
  value: TimePeriod
  onChange: (value: TimePeriod) => void
}) {
  const [isOpen, setIsOpen] = useState(false)

  return (
    <div className="relative inline-block">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="px-3 py-1.5 text-sm border border-border rounded-md hover:bg-muted transition-colors inline-flex items-center gap-1.5 w-full"
      >
        {timePeriodLabels[value]}
        <ChevronDown className="h-4 w-4 ml-auto" />
      </button>
      {isOpen && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setIsOpen(false)} />
          <div className="absolute left-0 right-0 top-full mt-1 z-50 bg-popover border border-border rounded-md shadow-lg py-1">
            {(Object.keys(timePeriodLabels) as TimePeriod[]).map((period) => (
              <button
                key={period}
                onClick={() => {
                  onChange(period)
                  setIsOpen(false)
                }}
                className={`w-full px-3 py-1.5 text-left text-sm transition-colors ${
                  value === period
                    ? 'bg-muted text-foreground'
                    : 'text-muted-foreground hover:bg-muted/50 hover:text-foreground'
                }`}
              >
                {timePeriodLabels[period]}
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

// Helper to get start of period
function getStartOfPeriod(date: Date, period: 'month' | 'week' | 'quarter'): Date {
  const d = new Date(date)
  if (period === 'month') {
    d.setDate(1)
    d.setHours(0, 0, 0, 0)
  } else if (period === 'week') {
    const day = d.getDay()
    d.setDate(d.getDate() - day)
    d.setHours(0, 0, 0, 0)
  } else if (period === 'quarter') {
    const month = d.getMonth()
    const quarterStart = Math.floor(month / 3) * 3
    d.setMonth(quarterStart, 1)
    d.setHours(0, 0, 0, 0)
  }
  return d
}

// Helper to aggregate timeline events by period
function aggregateByPeriod(
  events: TimelineEvent[],
  period: TimePeriod
): { date: string; joined: number; left: number; stakeJoined: number; stakeLeft: number }[] {
  const periodMap = new Map<string, { joined: number; left: number; stakeJoined: number; stakeLeft: number }>()

  for (const event of events) {
    const date = new Date(event.timestamp)
    let periodKey: string

    if (period === 'day') {
      periodKey = date.toISOString().split('T')[0]
    } else if (period === 'week') {
      const weekStart = getStartOfPeriod(date, 'week')
      periodKey = weekStart.toISOString().split('T')[0]
    } else {
      const monthStart = getStartOfPeriod(date, 'month')
      periodKey = monthStart.toISOString().split('T')[0]
    }

    const existing = periodMap.get(periodKey) || { joined: 0, left: 0, stakeJoined: 0, stakeLeft: 0 }
    const details = event.details as ValidatorEventDetails | undefined
    const stake = details?.stake_sol ?? 0

    if (event.event_type === 'validator_joined') {
      existing.joined++
      existing.stakeJoined += stake
    } else if (event.event_type === 'validator_left' || event.event_type === 'validator_offline') {
      existing.left++
      existing.stakeLeft += stake
    }

    periodMap.set(periodKey, existing)
  }

  return Array.from(periodMap.entries())
    .map(([date, data]) => ({ date, ...data }))
    .sort((a, b) => a.date.localeCompare(b.date))
}

// Helper to aggregate stake history by period
function aggregateStakeHistory(
  points: StakeHistoryPoint[],
  period: TimePeriod
): { date: string; dzStakeSol: number; stakeSharePct: number }[] {
  const periodMap = new Map<string, { dzStakeSol: number; stakeSharePct: number; timestamp: string }>()

  for (const point of points) {
    const date = new Date(point.timestamp)
    let periodKey: string

    if (period === 'day') {
      periodKey = date.toISOString().split('T')[0]
    } else if (period === 'week') {
      const weekStart = getStartOfPeriod(date, 'week')
      periodKey = weekStart.toISOString().split('T')[0]
    } else {
      const monthStart = getStartOfPeriod(date, 'month')
      periodKey = monthStart.toISOString().split('T')[0]
    }

    const existing = periodMap.get(periodKey)
    // Keep the latest point for each period
    if (!existing || point.timestamp > existing.timestamp) {
      periodMap.set(periodKey, {
        dzStakeSol: point.dz_stake_sol,
        stakeSharePct: point.stake_share_pct,
        timestamp: point.timestamp,
      })
    }
  }

  return Array.from(periodMap.entries())
    .map(([date, data]) => ({ date, dzStakeSol: data.dzStakeSol, stakeSharePct: data.stakeSharePct }))
    .sort((a, b) => a.date.localeCompare(b.date))
}

export function MomentumPage() {
  const [timePeriod, setTimePeriod] = useState<TimePeriod>('week')
  const { user, isAuthenticated } = useAuth()

  // Fetch app config to get internal domains
  const { data: config, isLoading: configLoading } = useQuery({
    queryKey: ['app-config'],
    queryFn: fetchConfig,
    staleTime: Infinity, // Config doesn't change
  })

  // Check if user is internal (email domain in allowlist)
  const isInternalUser = useMemo(() => {
    if (!isAuthenticated || !user?.email_domain || !config?.internalDomains) {
      return false
    }
    return config.internalDomains.includes(user.email_domain)
  }, [isAuthenticated, user?.email_domain, config?.internalDomains])

  // Fetch timeline events for validators (only if internal user)
  const { data: timelineData, isLoading: timelineLoading, error: timelineError, refetch } = useQuery({
    queryKey: ['momentum-timeline'],
    queryFn: () => fetchTimeline({
      entity_type: 'validator',
      action: 'added,removed',
      range: '7d',
      dz_filter: 'on_dz',
      limit: 10000,
    }),
    staleTime: 60_000,
    refetchInterval: 60_000,
    enabled: isInternalUser,
  })

  // Fetch stake changes for different periods
  const { data: changes7d } = useQuery({
    queryKey: ['momentum-changes-7d'],
    queryFn: () => fetchStakeChanges('7d'),
    staleTime: 60_000,
    enabled: isInternalUser,
  })

  const { data: changes30d } = useQuery({
    queryKey: ['momentum-changes-30d'],
    queryFn: () => fetchStakeChanges('30d'),
    staleTime: 60_000,
    enabled: isInternalUser,
  })

  // Fetch stake history for charts
  const { data: stakeHistory30d, isLoading: historyLoading } = useQuery({
    queryKey: ['momentum-history-30d'],
    queryFn: () => fetchStakeHistory('30d', '6h'),
    staleTime: 60_000,
    enabled: isInternalUser,
  })

  // Calculate velocity stats from timeline data
  const velocityStats = useMemo(() => {
    if (!timelineData?.events) return null

    const now = new Date()
    const thisWeekStart = getStartOfPeriod(now, 'week')
    const lastWeekStart = new Date(thisWeekStart)
    lastWeekStart.setDate(lastWeekStart.getDate() - 7)
    const thisMonthStart = getStartOfPeriod(now, 'month')
    const lastMonthStart = new Date(thisMonthStart)
    lastMonthStart.setMonth(lastMonthStart.getMonth() - 1)
    const thisQuarterStart = getStartOfPeriod(now, 'quarter')
    const lastQuarterStart = new Date(thisQuarterStart)
    lastQuarterStart.setMonth(lastQuarterStart.getMonth() - 3)

    let solThisWeek = 0
    let solThisMonth = 0
    let solThisQuarter = 0
    let latestJoinedTs: Date | null = null
    let largestStake90d = 0
    let largestStake90dTs: Date | null = null
    let latestTier12Ts: Date | null = null

    for (const event of timelineData.events) {
      if (event.event_type !== 'validator_joined') continue

      const details = event.details as ValidatorEventDetails | undefined
      const stake = details?.stake_sol ?? 0
      const ts = new Date(event.timestamp)

      // Track latest joined
      if (!latestJoinedTs || ts > latestJoinedTs) {
        latestJoinedTs = ts
      }

      // Track largest stake in last 90 days
      const ninetyDaysAgo = new Date(now)
      ninetyDaysAgo.setDate(ninetyDaysAgo.getDate() - 90)
      if (ts >= ninetyDaysAgo && stake >= 100000) {
        if (!largestStake90dTs || ts > largestStake90dTs) {
          largestStake90d = stake
          largestStake90dTs = ts
        }
      }

      // Track latest tier 1/2 (>= 500k SOL)
      if (stake >= 500000) {
        if (!latestTier12Ts || ts > latestTier12Ts) {
          latestTier12Ts = ts
        }
      }

      // Sum by period
      if (ts >= thisWeekStart) solThisWeek += stake
      if (ts >= thisMonthStart) solThisMonth += stake
      if (ts >= thisQuarterStart) solThisQuarter += stake
    }

    const daysSinceLatest = latestJoinedTs
      ? Math.floor((now.getTime() - latestJoinedTs.getTime()) / (1000 * 60 * 60 * 24))
      : null
    const daysSinceLargest90d = largestStake90dTs
      ? Math.floor((now.getTime() - largestStake90dTs.getTime()) / (1000 * 60 * 60 * 24))
      : null
    const daysSinceTier12 = latestTier12Ts
      ? Math.floor((now.getTime() - latestTier12Ts.getTime()) / (1000 * 60 * 60 * 24))
      : null

    return {
      solThisWeek,
      solThisMonth,
      solThisQuarter,
      solLastWeek: changes7d?.summary.joined_stake_sol ?? 0,
      solLastMonth: changes30d?.summary.joined_stake_sol ?? 0,
      daysSinceLatest,
      daysSinceLargest90d,
      daysSinceTier12,
    }
  }, [timelineData, changes7d, changes30d])

  // Aggregate data for charts
  const validatorGrowthData = useMemo(() => {
    if (!timelineData?.events) return []
    const aggregated = aggregateByPeriod(timelineData.events, timePeriod)
    let cumulative = 0
    return aggregated.map((d, idx) => {
      const netChange = d.joined - d.left
      const prevCumulative = cumulative
      cumulative += netChange
      const growthPct = prevCumulative > 0 ? (netChange / prevCumulative) * 100 : 0
      return {
        date: d.date,
        validatorsGrowth: netChange,
        validatorsGrowthPct: growthPct,
      }
    })
  }, [timelineData, timePeriod])

  const stakeAddedData = useMemo(() => {
    if (!timelineData?.events) return []
    const aggregated = aggregateByPeriod(timelineData.events, 'day')
    // Calculate 4-week moving average
    return aggregated.map((d, idx) => {
      const stakeAdded = d.stakeJoined - d.stakeLeft
      const windowStart = Math.max(0, idx - 27)
      const window = aggregated.slice(windowStart, idx + 1)
      const avg = window.reduce((sum, w) => sum + (w.stakeJoined - w.stakeLeft), 0) / window.length
      return {
        date: d.date,
        stakeAdded,
        stakeAdded4wAvg: avg,
      }
    })
  }, [timelineData])

  const stakeGrowthData = useMemo(() => {
    if (!stakeHistory30d?.points) return []
    const aggregated = aggregateStakeHistory(stakeHistory30d.points, timePeriod)
    return aggregated.map((d, idx) => {
      const prev = idx > 0 ? aggregated[idx - 1] : null
      const stakeGrowthSol = prev ? d.dzStakeSol - prev.dzStakeSol : 0
      const stakeGrowthPct = prev && prev.dzStakeSol > 0
        ? ((d.dzStakeSol - prev.dzStakeSol) / prev.dzStakeSol) * 100
        : 0
      return {
        date: d.date,
        stakeGrowthSol,
        stakeGrowthPct,
        stakeSharePct: d.stakeSharePct,
      }
    }).filter((_, idx) => idx > 0) // Skip first entry (no previous to compare)
  }, [stakeHistory30d, timePeriod])

  const stakeWeightData = useMemo(() => {
    if (!stakeHistory30d?.points) return []
    const aggregated = aggregateStakeHistory(stakeHistory30d.points, 'day')
    if (aggregated.length === 0) return []
    const firstShare = aggregated[0].stakeSharePct
    return aggregated.map(d => ({
      date: d.date,
      absoluteChangePct: d.stakeSharePct - firstShare,
    }))
  }, [stakeHistory30d])

  const isLoading = timelineLoading || historyLoading || configLoading

  // Show access denied if not internal user
  if (!configLoading && !isInternalUser) {
    return (
      <div className="flex-1 overflow-auto">
        <div className="max-w-6xl mx-auto px-4 sm:px-8 py-8">
          <div className="text-center py-24">
            <Lock className="h-16 w-16 text-muted-foreground/50 mx-auto mb-4" />
            <h1 className="text-2xl font-semibold mb-2">Access Restricted</h1>
            <p className="text-muted-foreground max-w-md mx-auto">
              This page is only available to internal users. Please sign in with an authorized email address.
            </p>
          </div>
        </div>
      </div>
    )
  }

  if (timelineError) {
    return (
      <div className="flex-1 overflow-auto">
        <div className="max-w-6xl mx-auto px-4 sm:px-8 py-8">
          <div className="text-center py-12">
            <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-3" />
            <div className="text-sm text-muted-foreground">
              {timelineError instanceof Error ? timelineError.message : 'Failed to load data'}
            </div>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-6xl mx-auto px-4 sm:px-8 py-8">
        {/* Header */}
        <div className="mb-8">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <TrendingUp className="h-6 w-6 text-muted-foreground" />
              <h1 className="text-2xl font-semibold">Momentum</h1>
            </div>
            <button
              onClick={() => refetch()}
              className="text-muted-foreground hover:text-foreground transition-colors"
              title="Refresh"
            >
              <RefreshCw className={cn('h-4 w-4', isLoading && 'animate-spin')} />
            </button>
          </div>
          <p className="text-muted-foreground mt-1">
            Track DoubleZero validator growth and stake onboarding trends
          </p>
        </div>

        {/* Onboarding Velocity Section */}
        <div className="mb-8">
          <h2 className="text-lg font-semibold mb-4">Onboarding Velocity</h2>

          {isLoading || !velocityStats ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-4 mb-4">
              {Array.from({ length: 6 }).map((_, i) => (
                <Skeleton key={i} className="h-28" />
              ))}
            </div>
          ) : (
            <>
              {/* SOL Onboarded stats */}
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-4">
                <StatCard
                  value={formatSolFull(velocityStats.solThisMonth)}
                  label="SOL Onboarded This Month"
                  comparison={`${formatSolFull(velocityStats.solLastMonth)} SOL Onboarded Last Month`}
                />
                <StatCard
                  value={formatSolFull(velocityStats.solThisWeek)}
                  label="SOL Onboarded This Week"
                  comparison={`${formatSolFull(velocityStats.solLastWeek)} SOL Onboarded Last Week`}
                />
                <StatCard
                  value={formatSolFull(velocityStats.solThisQuarter)}
                  label="SOL Onboarded This Quarter"
                />
              </div>

              {/* Days since stats */}
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                <StatCard
                  value={velocityStats.daysSinceLatest?.toString() ?? '—'}
                  label="Days Since Latest Validator Added"
                />
                <StatCard
                  value={velocityStats.daysSinceLargest90d?.toString() ?? '—'}
                  label="Days Since Largest Validator Added (Last 90 Days)"
                />
                <StatCard
                  value={velocityStats.daysSinceTier12?.toString() ?? '—'}
                  label="Days Since Latest Tier 1/2 Validator Added"
                />
              </div>
            </>
          )}
        </div>

        {/* Time Period Selector */}
        <div className="mb-6">
          <div className="text-sm text-muted-foreground mb-2">Time Period</div>
          <div className="w-48">
            <TimePeriodSelector value={timePeriod} onChange={setTimePeriod} />
          </div>
        </div>

        {/* DZ Stake Growth Chart */}
        <div className="mb-8">
          <h3 className="text-sm text-muted-foreground mb-3">DZ Stake Growth</h3>
          <div className="border border-border rounded-lg p-4 bg-card">
            {historyLoading ? (
              <Skeleton className="h-[300px]" />
            ) : stakeGrowthData.length > 0 ? (
              <ResponsiveContainer width="100%" height={300}>
                <ComposedChart data={stakeGrowthData} margin={{ top: 10, right: 60, left: 10, bottom: 20 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" opacity={0.5} />
                  <XAxis
                    dataKey="date"
                    tickFormatter={formatDateShort}
                    tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                    axisLine={{ stroke: 'hsl(var(--border))' }}
                  />
                  <YAxis
                    yAxisId="left"
                    tickFormatter={(v) => formatSol(v)}
                    tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                    axisLine={{ stroke: 'hsl(var(--border))' }}
                  />
                  <YAxis
                    yAxisId="right"
                    orientation="right"
                    tickFormatter={(v) => `${v.toFixed(0)}%`}
                    tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                    axisLine={{ stroke: 'hsl(var(--border))' }}
                  />
                  <ReferenceLine yAxisId="left" y={0} stroke="hsl(var(--border))" />
                  <Tooltip
                    formatter={(value: number, name: string) => [
                      name.includes('%') ? formatPercent(value) : formatSol(value),
                      name
                    ]}
                    labelFormatter={formatDateShort}
                  />
                  <Legend />
                  <Bar yAxisId="left" dataKey="stakeGrowthSol" name="Stake Growth (SOL)" fill="hsl(175, 40%, 50%)" opacity={0.8} />
                  <Line yAxisId="right" type="monotone" dataKey="stakeGrowthPct" name="Stake Growth (%)" stroke="hsl(0, 70%, 70%)" strokeWidth={2} dot={false} />
                </ComposedChart>
              </ResponsiveContainer>
            ) : (
              <div className="h-[300px] flex items-center justify-center text-muted-foreground">
                No data available
              </div>
            )}
          </div>
        </div>

        {/* DZ Stake Weight Absolute Change Chart */}
        <div className="mb-8">
          <h3 className="text-sm text-muted-foreground mb-3">DZ Stake Weight Absolute Change</h3>
          <div className="border border-border rounded-lg p-4 bg-card">
            {historyLoading ? (
              <Skeleton className="h-[250px]" />
            ) : stakeWeightData.length > 0 ? (
              <ResponsiveContainer width="100%" height={250}>
                <LineChart data={stakeWeightData} margin={{ top: 10, right: 30, left: 10, bottom: 20 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" opacity={0.5} />
                  <XAxis
                    dataKey="date"
                    tickFormatter={formatDateShort}
                    tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                    axisLine={{ stroke: 'hsl(var(--border))' }}
                  />
                  <YAxis
                    tickFormatter={(v) => `${v.toFixed(1)}%`}
                    tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                    axisLine={{ stroke: 'hsl(var(--border))' }}
                  />
                  <Tooltip
                    formatter={(value: number) => [`${value.toFixed(2)}%`, 'Absolute Change']}
                    labelFormatter={formatDateShort}
                  />
                  <Line type="monotone" dataKey="absoluteChangePct" stroke="hsl(0, 70%, 70%)" strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <div className="h-[250px] flex items-center justify-center text-muted-foreground">
                No data available
              </div>
            )}
          </div>
        </div>

        {/* DZ Validators Growth Chart */}
        <div className="mb-8">
          <h3 className="text-sm text-muted-foreground mb-3">DZ Validators Growth</h3>
          <div className="border border-border rounded-lg p-4 bg-card">
            {timelineLoading ? (
              <Skeleton className="h-[300px]" />
            ) : validatorGrowthData.length > 0 ? (
              <ResponsiveContainer width="100%" height={300}>
                <ComposedChart data={validatorGrowthData} margin={{ top: 10, right: 60, left: 10, bottom: 20 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" opacity={0.5} />
                  <XAxis
                    dataKey="date"
                    tickFormatter={formatDateShort}
                    tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                    axisLine={{ stroke: 'hsl(var(--border))' }}
                  />
                  <YAxis
                    yAxisId="left"
                    tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                    axisLine={{ stroke: 'hsl(var(--border))' }}
                  />
                  <YAxis
                    yAxisId="right"
                    orientation="right"
                    tickFormatter={(v) => `${v.toFixed(0)}%`}
                    tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                    axisLine={{ stroke: 'hsl(var(--border))' }}
                  />
                  <ReferenceLine yAxisId="left" y={0} stroke="hsl(var(--border))" />
                  <Tooltip
                    formatter={(value: number, name: string) => [
                      name.includes('%') ? formatPercent(value) : value,
                      name
                    ]}
                    labelFormatter={formatDateShort}
                  />
                  <Legend />
                  <Bar yAxisId="left" dataKey="validatorsGrowth" name="Validators Growth" fill="hsl(175, 40%, 50%)" opacity={0.8} />
                  <Line yAxisId="right" type="monotone" dataKey="validatorsGrowthPct" name="Validators Growth (%)" stroke="hsl(0, 70%, 70%)" strokeWidth={2} dot={false} />
                </ComposedChart>
              </ResponsiveContainer>
            ) : (
              <div className="h-[300px] flex items-center justify-center text-muted-foreground">
                No data available
              </div>
            )}
          </div>
        </div>

        {/* Stake Added Chart */}
        <div className="mb-8">
          <h3 className="text-sm text-muted-foreground mb-3">Stake Added</h3>
          <div className="border border-border rounded-lg p-4 bg-card">
            {timelineLoading ? (
              <Skeleton className="h-[300px]" />
            ) : stakeAddedData.length > 0 ? (
              <ResponsiveContainer width="100%" height={300}>
                <ComposedChart data={stakeAddedData} margin={{ top: 10, right: 60, left: 10, bottom: 20 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" opacity={0.5} />
                  <XAxis
                    dataKey="date"
                    tickFormatter={formatDateShort}
                    tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                    axisLine={{ stroke: 'hsl(var(--border))' }}
                  />
                  <YAxis
                    yAxisId="left"
                    tickFormatter={(v) => formatSol(v)}
                    tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                    axisLine={{ stroke: 'hsl(var(--border))' }}
                  />
                  <YAxis
                    yAxisId="right"
                    orientation="right"
                    tickFormatter={(v) => formatSol(v)}
                    tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                    axisLine={{ stroke: 'hsl(var(--border))' }}
                  />
                  <ReferenceLine yAxisId="left" y={0} stroke="hsl(var(--border))" />
                  <Tooltip
                    formatter={(value: number, name: string) => [formatSol(value), name]}
                    labelFormatter={formatDateShort}
                  />
                  <Legend />
                  <Bar yAxisId="left" dataKey="stakeAdded" name="Stake Added" fill="hsl(175, 40%, 50%)" opacity={0.8} />
                  <Line yAxisId="right" type="monotone" dataKey="stakeAdded4wAvg" name="Stake Added (4-week Avg)" stroke="hsl(0, 70%, 70%)" strokeWidth={2} dot={false} />
                </ComposedChart>
              </ResponsiveContainer>
            ) : (
              <div className="h-[300px] flex items-center justify-center text-muted-foreground">
                No data available
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
