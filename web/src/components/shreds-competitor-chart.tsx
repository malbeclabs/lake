import { useQuery } from '@tanstack/react-query'
import {
  Area,
  AreaChart,
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import { fetchShredsCompetitors, type ShredsCompetitorDay } from '@/lib/api'
import { FEED_COLORS } from '@/lib/feed-colors'
import { formatDay, formatLeadMs } from './shreds-competitor-day'

const WINDOW_DAYS = 30


const DZ_COLOR = FEED_COLORS.dz
const REFETCH_MS = 10 * 60 * 1000
const Y_TICKS = [0, 25, 50, 75, 100]

function StatCell({ label, value, accent }: { label: string; value: string; accent?: boolean }) {
  return (
    <div className="px-4 py-3">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</div>
      <div className="text-lg tabular-nums" style={accent ? { color: DZ_COLOR } : undefined}>
        {value}
      </div>
    </div>
  )
}

export function ShredsCompetitorChart() {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['shreds-competitors', WINDOW_DAYS],
    queryFn: () => fetchShredsCompetitors(WINDOW_DAYS),
    refetchInterval: REFETCH_MS,
    staleTime: REFETCH_MS,
  })

  const series = data ?? []
  const latest: ShredsCompetitorDay | undefined = series[series.length - 1]

  return (
    <div className="border border-border rounded-lg bg-card overflow-hidden mb-6">
      <div className="flex items-baseline justify-between gap-4 flex-wrap px-4 py-3">
        <h2 className="text-sm font-semibold">Win Rate vs Competitors</h2>
        {series.length > 0 && (
          <span className="text-xs text-muted-foreground tabular-nums">
            {formatDay(series[0].day)} – {formatDay(series[series.length - 1].day)}
          </span>
        )}
      </div>

      {isLoading ? (
        <div className="h-[248px] flex items-center justify-center text-sm text-muted-foreground">
          Loading…
        </div>
      ) : isError ? (
        <div className="h-[248px] flex items-center justify-center text-sm text-muted-foreground">
          Could not load competitor win rate.
        </div>
      ) : series.length === 0 ? (
        <div className="h-[248px] flex items-center justify-center text-sm text-muted-foreground">
          No completed days yet.
        </div>
      ) : (
        <>
          <div className="px-2 pt-4 pb-1">
            <ResponsiveContainer width="100%" height={224}>
              <AreaChart data={series} margin={{ top: 8, right: 16, left: 4, bottom: 0 }}>
                <defs>
                  <linearGradient id="dzWinFade" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={DZ_COLOR} stopOpacity={0.22} />
                    <stop offset="100%" stopColor={DZ_COLOR} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                <XAxis
                  dataKey="day"
                  tickLine={false}
                  axisLine={false}
                  tick={{ fontSize: 11, fill: 'var(--muted-foreground)' }}
                  tickFormatter={formatDay}
                  minTickGap={28}
                  dy={6}
                />
                <YAxis
                  domain={[0, 100]}
                  ticks={Y_TICKS}
                  tickLine={false}
                  axisLine={false}
                  tick={{ fontSize: 11, fill: 'var(--muted-foreground)' }}
                  tickFormatter={(v: number) => `${v}%`}
                  width={40}
                />
                {/* Even money. What lets a lone line be read without a legend. */}
                <ReferenceLine
                  y={50}
                  stroke="var(--muted-foreground)"
                  strokeDasharray="4 4"
                  label={{
                    value: 'EVEN',
                    position: 'insideTopRight',
                    fontSize: 10,
                    fill: 'var(--muted-foreground)',
                  }}
                />
                <Tooltip
                  cursor={{ stroke: 'var(--muted-foreground)', strokeDasharray: '3 3' }}
                  content={({ active, payload }) => {
                    if (!active || !payload?.length) return null
                    const d = payload[0].payload as ShredsCompetitorDay
                    return (
                      <div className="bg-card border border-border rounded-lg px-3 py-2 text-xs shadow-xl">
                        <div className="text-muted-foreground mb-1 tabular-nums">{d.day}</div>
                        <div className="font-semibold text-foreground tabular-nums">
                          {d.win_typical_pct.toFixed(1)}% win rate
                        </div>
                        <div className="text-muted-foreground tabular-nums mt-0.5">
                          {formatLeadMs(d.lead_typical_ms)} lead
                        </div>
                        <div className="text-muted-foreground tabular-nums">
                          {d.leader_slots.toLocaleString()} leader slots
                        </div>
                      </div>
                    )
                  }}
                />
                <Area
                  type="monotone"
                  dataKey="win_typical_pct"
                  stroke={DZ_COLOR}
                  strokeWidth={2}
                  fill="url(#dzWinFade)"
                  dot={false}
                  activeDot={{ r: 4, stroke: 'var(--card)', strokeWidth: 2 }}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>

          {latest && (
            <div className="grid grid-cols-2 border-t border-border divide-x divide-border">
              <StatCell label="Yesterday" value={`${latest.win_typical_pct.toFixed(1)}%`} accent />
              <StatCell label="DZ lead, p50" value={formatLeadMs(latest.lead_typical_ms)} />
            </div>
          )}
        </>
      )}
    </div>
  )
}
