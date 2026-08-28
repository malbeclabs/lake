import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, useParams } from 'react-router-dom'
import { AlertCircle, ArrowLeft, Trophy } from 'lucide-react'
import { fetchShredsRewardsDetail } from '@/lib/api'
import { cn } from '@/lib/utils'
import { useDelayedLoading } from '@/hooks/use-delayed-loading'
import { PageHeader } from './page-header'
import { CopyableText } from './copyable-text'
import { formatTokenAmount } from './shreds-rewards-format'
import {
  RewardsShimmerBar,
  SkeletonCell,
  SkeletonRows,
} from './shreds-rewards-skeleton'

// Enough placeholder rows to fill the fold. A validator's history runs to
// dozens of epochs, so this deliberately under-promises rather than guessing.
const SKELETON_ROWS = 10

function truncatePK(pk: string): string {
  if (!pk) return ''
  if (pk.length <= 12) return pk
  return `${pk.slice(0, 6)}...${pk.slice(-4)}`
}

// formatTokenTotals renders a {symbol: whole-token amount} map as a compact
// per-token list (e.g. "12.34 2Z · 5.00 USDC"). Rewards span multiple tokens
// from epoch 968, so an all-time total is a per-token breakdown, not one number.
function formatTokenTotals(totals: Record<string, number>): string {
  const parts = Object.entries(totals)
    .filter(([, amt]) => amt > 0)
    .map(([sym, amt]) => formatTokenAmount(amt, sym))
  return parts.length > 0 ? parts.join(' · ') : '—'
}


// The validator's name is deliberately NOT read out of the cached rewards
// listing. react-query keeps that entry after the list unmounts and stops
// refetching it, so its age is unbounded — the heading would assert a name
// nothing is keeping current, beside epoch rows this page refetches every 60s.
// Gating it on a freshness window is worse, not better: the heading would flip
// from the name to the pubkey while the reader watched.
//
// So the heading is the truncated node id, which comes from the URL and cannot
// go stale. Bringing the name back means fetching it under this page's own
// refetch cycle; now that a search request is served from the page cache,
// ?search=node:<id>&limit=1 is a cheap way to do that.

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

  const { data, isLoading, isFetching, error } = useQuery({
    queryKey: ['shreds-rewards-detail', nodeId],
    queryFn: () => fetchShredsRewardsDetail(nodeId),
    enabled: !!nodeId,
    refetchInterval: 60_000,
  })

  // Show loading progress on first read.
  const showSkeleton = useDelayedLoading(isLoading)
  const showShimmer = useDelayedLoading(isFetching && !isLoading)

  const totals = useMemo(() => {
    const all: Record<string, number> = {}
    const claimable: Record<string, number> = {}
    if (data) {
      for (const e of data.epochs) {
        const sym = e.token_symbol || '2Z'
        all[sym] = (all[sym] || 0) + (e.earned || 0)
        if (e.state === 'claimable') {
          claimable[sym] = (claimable[sym] || 0) + (e.earned || 0)
        }
      }
    }
    const hasClaimable = Object.values(claimable).some((v) => v > 0)
    return { all, claimable, hasClaimable }
  }, [data])

  if (error || (!data && !isLoading)) {
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

  const displayName = truncatePK(nodeId)
  const epochs = data?.epochs ?? []

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
        {/* No subtitle: the title IS the truncated node id, so a subtitle would
            print the same pubkey twice side by side, and the Validator Identity
            card below makes three. */}
        <PageHeader icon={Trophy} title={displayName} />

        <div className="grid grid-cols-2 md:grid-cols-3 gap-3 mb-8">
          {/* Identity is known from the URL, so it is never a skeleton. */}
          <FactCard label="Validator Identity">
            <CopyableText text={nodeId} className="text-xs">
              <span className="font-mono">{truncatePK(nodeId)}</span>
            </CopyableText>
          </FactCard>
          <FactCard label="All-time Earned">
            {data ? formatTokenTotals(totals.all) : <SkeletonCell className="w-24" />}
          </FactCard>
          <FactCard label="Immediately Claimable">
            {!data ? (
              <SkeletonCell className="w-24" />
            ) : (
              <span
                className={cn(
                  totals.hasClaimable
                    ? 'text-amber-500 dark:text-amber-400'
                    : 'text-muted-foreground/60',
                )}
              >
                {formatTokenTotals(totals.claimable)}
              </span>
            )}
          </FactCard>
        </div>

        <div className="mb-4 rounded-lg bg-muted/50 px-4 py-3 text-xs xxs:text-sm text-muted-foreground">
          Accrued rewards will be claimable approximately 10 epochs after they are
          earned.
        </div>

        <div className="border border-border rounded-lg overflow-hidden bg-card">
          <RewardsShimmerBar show={showShimmer} />
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
                  <th className="px-4 py-3 text-left font-medium uppercase tracking-wider">
                    Client
                  </th>
                  <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                    Earned
                  </th>
                  <th className="px-4 py-3 text-left font-medium uppercase tracking-wider">
                    Status
                  </th>
                </tr>
              </thead>
              <tbody>
                {/* Checked before the empty case so an arriving history never
                    flashes "No rewards recorded for this validator", which
                    reads as a finding rather than as waiting. */}
                {!data ? (
                  showSkeleton && (
                    <SkeletonRows
                      rows={SKELETON_ROWS}
                      widths={['w-12', 'w-12', 'w-16', 'w-24', 'w-20', 'w-16']}
                      align={['left', 'left', 'right', 'left', 'right', 'left']}
                    />
                  )
                ) : epochs.length === 0 ? (
                  <tr>
                    <td
                      colSpan={6}
                      className="px-4 py-12 text-center text-muted-foreground"
                    >
                      No rewards recorded for this validator
                    </td>
                  </tr>
                ) : (
                  epochs.map((epoch) => {
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
                        key={`${epoch.subscription_epoch}-${epoch.client_id}`}
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
                        <td
                          className="px-4 py-3 text-muted-foreground"
                          title={`Client ID ${epoch.client_id}`}
                        >
                          {epoch.client_name || `Client ${epoch.client_id}`}
                        </td>
                        <td className="px-4 py-3 tabular-nums text-right font-medium">
                          {formatTokenAmount(epoch.earned, epoch.token_symbol)}
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
