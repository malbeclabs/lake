import { useEffect, useState, useCallback } from 'react'
import { PageHeader } from './page-header'
import {
  fetchHyperliquidScoreboard,
  type HyperliquidScoreboardResponse,
} from '@/lib/api'

const WINDOWS = ['1h', '24h', '7d']

function pct(n: number): string {
  return `${n.toFixed(1)}%`
}
function ms(n: number): string {
  return `+${n.toFixed(2)} ms`
}

function CompetitorTable({ competitors }: { competitors: HyperliquidScoreboardResponse['competitors'] }) {
  if (!competitors.length) {
    return <div className="text-sm text-muted-foreground">No competitor races in this window.</div>
  }
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-left text-muted-foreground">
          <th className="py-1 pr-4 font-normal">vs Competitor</th>
          <th className="py-1 pr-4 font-normal">DZ win%</th>
          <th className="py-1 pr-4 font-normal">median lead</th>
          <th className="py-1 pr-4 font-normal">p95 lead</th>
          <th className="py-1 pr-4 font-normal">races</th>
        </tr>
      </thead>
      <tbody>
        {competitors.map((c) => (
          <tr key={c.feed} className="border-t border-border/50">
            <td className="py-1 pr-4">{c.label}</td>
            <td className="py-1 pr-4 tabular-nums">{pct(c.dz_win_pct)}</td>
            <td className="py-1 pr-4 tabular-nums">{ms(c.lead_p50_ms)}</td>
            <td className="py-1 pr-4 tabular-nums">{ms(c.lead_p95_ms)}</td>
            <td className="py-1 pr-4 tabular-nums">{c.races.toLocaleString()}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

export function HyperliquidScoreboardPage() {
  const [window, setWindow] = useState('24h')
  const [data, setData] = useState<HyperliquidScoreboardResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      setData(await fetchHyperliquidScoreboard(window))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [window])

  // Initial + window-change load, then poll every 15s for fresh recent races.
  useEffect(() => {
    load()
    const id = setInterval(load, 15000)
    return () => clearInterval(id)
  }, [load])

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageHeader title="Hyperliquid · BBO Scoreboard" />

      <div className="flex items-center gap-2">
        <span className="text-sm text-muted-foreground">window:</span>
        {WINDOWS.map((w) => (
          <button
            key={w}
            onClick={() => setWindow(w)}
            className={`rounded px-2 py-1 text-sm ${w === window ? 'bg-primary text-primary-foreground' : 'bg-muted'}`}
          >
            {w}
          </button>
        ))}
      </div>

      {error && <div className="text-sm text-red-500">{error}</div>}
      {!data && !error && <div className="text-sm text-muted-foreground">Loading…</div>}

      {data && (
        <>
          <div className="rounded-lg border border-border p-4">
            <div className="text-2xl font-semibold tabular-nums">
              DoubleZero wins {pct(data.dz_win_share_pct)} of races
            </div>
            <div className="text-sm text-muted-foreground">
              all vantages · {data.window} · {data.total_races.toLocaleString()} comparisons
            </div>
          </div>

          <div className="rounded-lg border border-border p-4">
            <div className="mb-2 text-sm font-medium">DoubleZero vs competitors</div>
            <CompetitorTable competitors={data.competitors} />
          </div>

          <div className="rounded-lg border border-border p-4">
            <div className="mb-3 text-sm font-medium">By vantage</div>
            <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
              {data.nodes.map((n) => (
                <div key={n.measurement_node_id} className="rounded border border-border/60 p-3">
                  <div className="mb-1 flex items-baseline justify-between">
                    <span className="font-medium uppercase">{n.location_code}</span>
                    <span className="tabular-nums">{pct(n.dz_win_share_pct)}</span>
                  </div>
                  <div className="mb-2 text-xs text-muted-foreground">
                    {n.measurement_node_id} · {n.total_races.toLocaleString()} races
                  </div>
                  <CompetitorTable competitors={n.competitors} />
                </div>
              ))}
            </div>
          </div>

          <div className="rounded-lg border border-border p-4">
            <div className="mb-2 text-sm font-medium">Recent races (live)</div>
            <table className="w-full text-sm">
              <tbody>
                {data.recent_races.map((r, i) => (
                  <tr key={`${r.symbol}-${r.event_ts}-${i}`} className="border-t border-border/50">
                    <td className="py-1 pr-4 font-medium">{r.symbol}</td>
                    <td className="py-1 pr-4">
                      {r.is_dz ? (
                        <span className="text-emerald-500">DoubleZero ({r.winner_feed})</span>
                      ) : (
                        <span className="text-muted-foreground">{r.winner_feed}</span>
                      )}
                    </td>
                    <td className="py-1 pr-4 tabular-nums">
                      {ms(r.lead_ms)} vs {r.runner_up_label}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}
