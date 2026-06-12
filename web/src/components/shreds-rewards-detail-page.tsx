import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, useParams } from 'react-router-dom'
import { Loader2, AlertCircle, ArrowLeft, Trophy } from 'lucide-react'
import { fetchShredsRewardsDetail } from '@/lib/api'
import { cn } from '@/lib/utils'
import { PageHeader } from './page-header'
import { CopyableText } from './copyable-text'
import { format2Z } from './shreds-rewards-format'

function truncatePK(pk: string): string {
  if (!pk) return ''
  if (pk.length <= 12) return pk
  return `${pk.slice(0, 6)}...${pk.slice(-4)}`
}

function formatStake(lamports: number): string {
  if (!lamports || lamports <= 0) return '—'
  const sol = lamports / 1e9
  if (sol >= 1e6) return `${(sol / 1e6).toFixed(2)}M SOL`
  if (sol >= 1e3) return `${(sol / 1e3).toFixed(0)}K SOL`
  return `${sol.toLocaleString(undefined, { maximumFractionDigits: 0 })} SOL`
}

function FactCard({
  label,
  children,
}: {
  label: string
  children: React.ReactNode
}) {
  return (
    <div className="rounded-lg border border-border bg-card px-4 py-3">
      <div className="text-[10px] font-medium text-muted-foreground/60 uppercase tracking-widest mb-1.5">
        {label}
      </div>
      <div className="text-sm font-medium tabular-nums break-all">{children}</div>
    </div>
  )
}

export function ShredsRewardsDetailPage() {
  const { nodeId = '' } = useParams<{ nodeId: string }>()

  const { data, isLoading, error } = useQuery({
    queryKey: ['shreds-rewards-detail', nodeId],
    queryFn: () => fetchShredsRewardsDetail(nodeId),
    enabled: !!nodeId,
    refetchInterval: 60_000,
  })

  const totals = useMemo(() => {
    if (!data) return { all: 0, claimable: 0 }
    const all = data.epochs.reduce((acc, e) => acc + (e.earned_2z || 0), 0)
    const claimable = data.epochs
      .filter((e) => e.state === 'claimable')
      .reduce((acc, e) => acc + (e.earned_2z || 0), 0)
    return { all, claimable }
  }, [data])

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Unable to load validator rewards</div>
          <div className="text-sm text-muted-foreground">
            {(error as Error)?.message || 'Unknown error'}
          </div>
          <Link
            to="/dz/shreds/rewards"
            className="mt-4 inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
          >
            <ArrowLeft className="h-3.5 w-3.5" />
            Back to Edge Rewards
          </Link>
        </div>
      </div>
    )
  }

  const displayName = data.validator_name?.trim() || truncatePK(data.node_id)

  return (
    <div className="flex-1 overflow-auto">
      <div className="mx-auto px-4 sm:px-8 py-8 max-w-6xl">
        <Link
          to="/dz/shreds/rewards"
          className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors mb-3"
        >
          <ArrowLeft className="h-3.5 w-3.5" />
          Edge Rewards
        </Link>
        <PageHeader
          icon={Trophy}
          title={displayName}
          subtitle={
            <span className="text-xs text-muted-foreground font-mono">
              {truncatePK(data.node_id)}
            </span>
          }
        />

        <div className="grid grid-cols-2 md:grid-cols-3 gap-3 mb-8">
          <FactCard label="Validator Identity">
            <CopyableText text={data.node_id} className="text-xs">
              <span className="font-mono">{truncatePK(data.node_id)}</span>
            </CopyableText>
          </FactCard>
          <FactCard label="Vote ID">
            {data.vote_pubkey ? (
              <CopyableText text={data.vote_pubkey} className="text-xs">
                <span className="font-mono">{truncatePK(data.vote_pubkey)}</span>
              </CopyableText>
            ) : (
              <span className="text-muted-foreground/60">—</span>
            )}
          </FactCard>
          <FactCard label="Stake">{formatStake(data.activated_stake)}</FactCard>
          <FactCard label="DZ IP">
            {data.dz_user_ip ? (
              <span className="font-mono text-xs">{data.dz_user_ip}</span>
            ) : (
              <span className="text-muted-foreground/60">—</span>
            )}
          </FactCard>
          <FactCard label="All-time Earned">{format2Z(totals.all)}</FactCard>
          <FactCard label="Immediately Claimable">
            <span
              className={cn(
                totals.claimable > 0
                  ? 'text-amber-500 dark:text-amber-400'
                  : 'text-muted-foreground/60',
              )}
            >
              {format2Z(totals.claimable)}
            </span>
          </FactCard>
        </div>

        <div className="mb-4 rounded-lg bg-muted/50 px-4 py-3 text-xs xxs:text-sm text-muted-foreground">
          Accrued rewards will be claimable approximately 10 epochs after they are
          earned.
        </div>

        <div className="border border-border rounded-lg overflow-hidden bg-card">
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="text-xs text-muted-foreground border-b border-border bg-muted/30">
                  <th className="px-4 py-3 text-left font-medium uppercase tracking-wider">
                    Solana Epoch
                  </th>
                  <th className="px-4 py-3 text-left font-medium uppercase tracking-wider">
                    Subscription Epoch
                  </th>
                  <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                    Leader Slots
                  </th>
                  <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                    Client
                  </th>
                  <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                    Earned 2Z
                  </th>
                  <th className="px-4 py-3 text-left font-medium uppercase tracking-wider">
                    Status
                  </th>
                </tr>
              </thead>
              <tbody>
                {data.epochs.length === 0 ? (
                  <tr>
                    <td
                      colSpan={6}
                      className="px-4 py-12 text-center text-muted-foreground"
                    >
                      No rewards recorded for this validator
                    </td>
                  </tr>
                ) : (
                  data.epochs.map((epoch) => {
                    // Drive the badge off the derived lifecycle `state`:
                    //   claimable   -> Claimable (claimable now)
                    //   distributed -> Paid (rewards already distributed)
                    //   pending     -> Pending (accruing / not finalized)
                    //   unknown     -> Unknown (journal swept before we tracked
                    //                  it; per-leaf claim state is unrecoverable)
                    const claimStyle: Record<string, string> = {
                      claimable:
                        'bg-amber-500/10 text-amber-500 dark:text-amber-400 border-amber-500/30',
                      distributed: 'bg-muted text-muted-foreground border-border',
                      pending:
                        'bg-sky-500/10 text-sky-600 dark:text-sky-400 border-sky-500/30',
                      unknown:
                        'bg-transparent text-muted-foreground/50 border-border/50',
                    }
                    const claimLabel: Record<string, string> = {
                      claimable: 'Claimable',
                      distributed: 'Paid',
                      pending: 'Pending',
                      unknown: 'Unknown',
                    }
                    const bucket =
                      epoch.state === 'claimable' ||
                      epoch.state === 'distributed' ||
                      epoch.state === 'unknown'
                        ? epoch.state
                        : 'pending'
                    const statusStyle = claimStyle[bucket]
                    const statusLabel = claimLabel[bucket]
                    return (
                      <tr
                        key={`${epoch.solana_epoch}-${epoch.subscription_epoch}`}
                        className="border-b border-border last:border-b-0 hover:bg-muted/30 transition-colors"
                      >
                        <td className="px-4 py-3 tabular-nums font-medium">
                          {epoch.solana_epoch}
                        </td>
                        <td className="px-4 py-3 tabular-nums text-muted-foreground">
                          {epoch.subscription_epoch}
                        </td>
                        <td className="px-4 py-3 tabular-nums text-right">
                          {epoch.leader_slots.toLocaleString()}
                        </td>
                        <td className="px-4 py-3 tabular-nums text-right text-muted-foreground">
                          {epoch.client_id}
                        </td>
                        <td className="px-4 py-3 tabular-nums text-right font-medium">
                          {format2Z(epoch.earned_2z)}
                        </td>
                        <td className="px-4 py-3">
                          <span
                            className={cn(
                              'inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border',
                              statusStyle,
                            )}
                          >
                            {statusLabel}
                          </span>
                        </td>
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  )
}
