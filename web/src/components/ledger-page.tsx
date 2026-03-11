import { useQuery } from '@tanstack/react-query'
import { BookOpen } from 'lucide-react'
import { PageHeader } from './page-header'
import { fetchDZLedger, fetchSolanaLedger, type LedgerResponse } from '@/lib/api'

function formatDuration(seconds: number): string {
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  if (h > 0) return `~${h}h ${m}m`
  return `~${m}m`
}

function formatNumber(n: number, decimals = 0): string {
  return n.toLocaleString('en-US', { maximumFractionDigits: decimals })
}

function formatCompact(n: number): string {
  if (n >= 1e12) return `${(n / 1e12).toFixed(2)}T`
  if (n >= 1e9) return `${(n / 1e9).toFixed(2)}B`
  if (n >= 1e6) return `${(n / 1e6).toFixed(2)}M`
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K`
  return formatNumber(n)
}

function formatSOL(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return formatNumber(n, 2)
}

function Skeleton() {
  return <span className="inline-block h-5 w-16 rounded bg-muted animate-pulse align-middle" />
}

function Card({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="rounded-lg border bg-card p-4 sm:p-5">
      <div className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-3">{title}</div>
      {children}
    </div>
  )
}

function Metric({ label, value, sub }: { label: string; value: React.ReactNode; sub?: React.ReactNode }) {
  return (
    <div className="min-w-0">
      <div className="text-lg sm:text-2xl font-medium tabular-nums tracking-tight truncate">{value}</div>
      <div className="text-xs sm:text-sm text-muted-foreground">{label}</div>
      {sub && <div className="text-xs text-muted-foreground mt-0.5">{sub}</div>}
    </div>
  )
}

function EpochProgress({ data }: { data: LedgerResponse | undefined }) {
  if (!data) {
    return (
      <Card title="Epoch">
        <div className="space-y-3">
          <Skeleton />
          <div className="h-3 rounded-full bg-muted" />
        </div>
      </Card>
    )
  }

  const pct = data.epoch_pct
  const remaining = formatDuration(data.epoch_eta_sec)
  const started = formatDuration(data.slot_index * 0.4)

  return (
    <Card title="Epoch">
      <div className="space-y-3">
        <div className="flex items-baseline justify-between">
          <span className="text-2xl sm:text-3xl font-medium tabular-nums">{formatNumber(data.epoch)}</span>
          <span className="text-sm text-muted-foreground tabular-nums">{pct.toFixed(1)}%</span>
        </div>
        <div className="h-3 rounded-full bg-muted overflow-hidden">
          <div
            className="h-full rounded-full bg-blue-500 transition-all duration-500"
            style={{ width: `${pct}%` }}
          />
        </div>
        <div className="flex flex-col sm:flex-row sm:justify-between gap-0.5 sm:gap-0 text-xs text-muted-foreground tabular-nums">
          <span>started {started} ago</span>
          <span>{formatCompact(data.slot_index)} / {formatCompact(data.slots_in_epoch)} slots</span>
          <span>{remaining} remaining</span>
        </div>
      </div>
    </Card>
  )
}

function ChainState({ data }: { data: LedgerResponse | undefined }) {
  return (
    <Card title="Chain State">
      <div className="grid grid-cols-2 gap-4 sm:gap-6 md:grid-cols-4">
        <Metric
          label="Slot"
          value={data ? formatCompact(data.absolute_slot) : <Skeleton />}
        />
        <Metric
          label="Block Height"
          value={data ? formatCompact(data.block_height) : <Skeleton />}
        />
        <Metric
          label="Transactions"
          value={data ? formatCompact(data.transaction_count) : <Skeleton />}
        />
        <Metric
          label="Skip Rate"
          value={data ? `${data.skip_rate.toFixed(2)}%` : <Skeleton />}
        />
      </div>
    </Card>
  )
}

function LedgerDashboard({ data, full = false }: { data: LedgerResponse | undefined; full?: boolean }) {
  return (
    <div className="space-y-4 sm:space-y-6">
      <EpochProgress data={data} />
      <ChainState data={data} />

      {full && (
        <>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 sm:gap-6">
            <Card title="Performance">
              <div className="grid grid-cols-2 gap-4 sm:gap-6">
                <Metric
                  label="TPS (avg)"
                  value={data ? formatNumber(data.tps, 0) : <Skeleton />}
                  sub={data ? 'recent 10 samples' : undefined}
                />
                <Metric
                  label="Node Version"
                  value={data ? data.node_version : <Skeleton />}
                />
              </div>
            </Card>

            <Card title="Validators">
              <div className="grid grid-cols-3 gap-4 sm:gap-6">
                <Metric
                  label="Active"
                  value={data ? formatNumber(data.active_validators) : <Skeleton />}
                />
                <Metric
                  label="Delinquent"
                  value={data ? formatNumber(data.delinquent_validators) : <Skeleton />}
                />
                <Metric
                  label="Total Stake"
                  value={data ? `${formatSOL(data.total_stake_sol)} SOL` : <Skeleton />}
                />
              </div>
            </Card>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 sm:gap-6">
            <Card title="Supply">
              <div className="grid grid-cols-2 gap-4 sm:gap-6">
                <Metric
                  label="Total Supply"
                  value={data ? `${formatSOL(data.total_supply)} SOL` : <Skeleton />}
                />
                <Metric
                  label="Circulating"
                  value={data ? `${formatSOL(data.circulating_supply)} SOL` : <Skeleton />}
                />
              </div>
            </Card>

            <Card title="Inflation">
              <div className="grid grid-cols-2 gap-4 sm:gap-6">
                <Metric
                  label="Total"
                  value={data ? `${data.inflation_total.toFixed(2)}%` : <Skeleton />}
                />
                <Metric
                  label="Validator"
                  value={data ? `${data.inflation_validator.toFixed(2)}%` : <Skeleton />}
                />
              </div>
            </Card>
          </div>
        </>
      )}
    </div>
  )
}

export function DZLedgerPage() {
  const { data } = useQuery({
    queryKey: ['dz-ledger'],
    queryFn: fetchDZLedger,
    refetchInterval: 30_000,
  })

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-5xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader icon={BookOpen} title="DoubleZero Ledger" />
        <LedgerDashboard data={data} />
      </div>
    </div>
  )
}

export function SolanaLedgerPage() {
  const { data } = useQuery({
    queryKey: ['solana-ledger'],
    queryFn: fetchSolanaLedger,
    refetchInterval: 30_000,
  })

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-5xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader icon={BookOpen} title="Solana Ledger" />
        <LedgerDashboard data={data} full />
      </div>
    </div>
  )
}
