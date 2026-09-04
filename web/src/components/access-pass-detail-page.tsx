import { useParams, useNavigate, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { useMemo, useState } from 'react'
import { AlertCircle, ArrowLeft, ChevronLeft, ChevronRight, KeyRound, Loader2 } from 'lucide-react'
import { fetchAccessPass, fetchAccessPasses, fetchAccessPassConnections } from '@/lib/api'
import type { AccessPass, AccessPassConnection, AccessPassShredsSeat, MulticastGroupRef } from '@/lib/api'
import { CopyableText } from '@/components/copyable-text'
import { useBackLink } from '@/hooks/use-back-link'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { handleRowClick } from '@/lib/utils'

const SHREDS_INTERNAL_USER_PAYERS = new Set([
  '331ov6bjNUTLTATEUC4m7wxdHfAE5KxWwA6ng1Y1VZh8',
  '3b2Ze7VYUvhwQBfx5oCMCmsc2xvyZ74s2Lata5vmQeeN',
])
const PAGE_SIZE = 10

const TYPE_TAG_COLORS: Record<string, string> = {
  prepaid: 'bg-zinc-500/10 text-zinc-600 dark:text-zinc-400 border-zinc-500/20',
  solana_validator: 'bg-violet-500/10 text-violet-600 dark:text-violet-400 border-violet-500/20',
  solana_rpc: 'bg-blue-500/10 text-blue-600 dark:text-blue-400 border-blue-500/20',
  others: 'bg-yellow-500/10 text-yellow-600 dark:text-yellow-400 border-yellow-500/20',
  edge_seat: 'bg-green-500/10 text-green-600 dark:text-green-400 border-green-500/20',
}

const STATUS_COLORS: Record<string, string> = {
  connected: 'text-green-600 dark:text-green-400',
  requested: 'text-blue-600 dark:text-blue-400',
  disconnected: 'text-muted-foreground',
  expired: 'text-red-600 dark:text-red-400',
}

function TypeTagBadge({ tag }: { tag: string }) {
  const colors = TYPE_TAG_COLORS[tag] ?? 'bg-muted text-muted-foreground border-border'
  const label = tag.replace(/_/g, ' ')
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-md text-xs font-medium border ${colors}`}>
      {label}
    </span>
  )
}

function MulticastAllowlistTable({ pubGroups, subGroups }: { pubGroups: MulticastGroupRef[]; subGroups: MulticastGroupRef[] }) {
  const rows = useMemo(() => {
    const pubSet = new Set(pubGroups.map(g => g.pk))
    const subSet = new Set(subGroups.map(g => g.pk))
    const byPk = new Map<string, MulticastGroupRef>()
    for (const g of [...pubGroups, ...subGroups]) byPk.set(g.pk, g)
    return [...byPk.keys()].map(pk => ({
      group: byPk.get(pk)!,
      isPub: pubSet.has(pk),
      isSub: subSet.has(pk),
    }))
  }, [pubGroups, subGroups])

  if (rows.length === 0) return null
  return (
    <div>
      <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider mb-3">
        Multicast Allowlist ({rows.length})
      </h2>
      <div className="border border-border rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-muted/30 text-xs text-muted-foreground uppercase tracking-wider">
              <th className="px-4 py-3 font-medium text-left">Code</th>
              <th className="px-4 py-3 font-medium text-left">Multicast IP</th>
              <th className="px-4 py-3 font-medium text-left">Role</th>
              <th className="px-4 py-3 font-medium text-left">Status</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(({ group: g, isPub, isSub }) => (
              <tr key={g.pk} className="border-b border-border last:border-b-0 hover:bg-muted cursor-pointer transition-colors">
                <td className="px-4 py-3">
                  <Link
                    to={`/dz/multicast-groups/${g.pk}`}
                    className="font-mono text-sm text-blue-600 dark:text-blue-400 hover:underline"
                  >
                    {g.code || `${g.pk.slice(0, 8)}…`}
                  </Link>
                </td>
                <td className="px-4 py-3 font-mono text-sm text-muted-foreground">{g.multicast_ip || '—'}</td>
                <td className="px-4 py-3">
                  <span className="inline-flex items-center gap-1">
                    {isPub && (
                      <span className="inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-green-500/10 text-green-600 dark:text-green-400 border border-green-500/20">pub</span>
                    )}
                    {isSub && (
                      <span className="inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-orange-500/10 text-orange-600 dark:text-orange-400 border border-orange-500/20">sub</span>
                    )}
                  </span>
                </td>
                <td className="px-4 py-3 text-sm capitalize text-muted-foreground">{g.status || '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function RelatedPassesTable({ byIpPasses, byPayerPasses, currentPk, navigate }: {
  byIpPasses: AccessPass[]
  byPayerPasses: AccessPass[]
  currentPk: string
  navigate: ReturnType<typeof useNavigate>
}) {
  const [page, setPage] = useState(0)

  const rows = useMemo(() => {
    const ipSet = new Set(byIpPasses.filter(p => p.pk !== currentPk).map(p => p.pk))
    const payerSet = new Set(byPayerPasses.filter(p => p.pk !== currentPk).map(p => p.pk))
    const allPks = new Set([...ipSet, ...payerSet])
    const byPk = new Map<string, AccessPass>()
    for (const p of [...byIpPasses, ...byPayerPasses]) {
      if (p.pk !== currentPk) byPk.set(p.pk, p)
    }
    return [...allPks].map(pk => ({
      pass: byPk.get(pk)!,
      sameIp: ipSet.has(pk),
      samePayer: payerSet.has(pk),
    }))
  }, [byIpPasses, byPayerPasses, currentPk])

  if (rows.length === 0) return null

  const totalPages = Math.ceil(rows.length / PAGE_SIZE)
  const pageRows = rows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)

  return (
    <div>
      <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider mb-3">
        Related Access Passes ({rows.length})
      </h2>
      <div className="border border-border rounded-lg overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border bg-muted/30 text-xs text-muted-foreground uppercase tracking-wider">
                <th className="px-4 py-3 font-medium text-left">Owner</th>
                <th className="px-4 py-3 font-medium text-left">Client IP</th>
                <th className="px-4 py-3 font-medium text-left">Type</th>
                <th className="px-4 py-3 font-medium text-left">Status</th>
                <th className="px-4 py-3 font-medium text-left">Match</th>
                <th className="px-4 py-3 font-medium text-right">Connections</th>
              </tr>
            </thead>
            <tbody>
              {pageRows.map(({ pass: p, sameIp, samePayer }) => (
                <tr
                  key={p.pk}
                  className="border-b border-border last:border-b-0 hover:bg-muted cursor-pointer transition-colors"
                  onClick={(e) => handleRowClick(e, `/dz/access-passes/${p.pk}`, navigate)}
                >
                  <td className="px-4 py-3 font-mono text-sm text-muted-foreground">
                    {p.owner_pubkey ? `${p.owner_pubkey.slice(0, 6)}…${p.owner_pubkey.slice(-4)}` : '—'}
                  </td>
                  <td className="px-4 py-3 font-mono text-sm">
                    {p.client_ip || <span className="text-muted-foreground">—</span>}
                  </td>
                  <td className="px-4 py-3">
                    <TypeTagBadge tag={p.type_tag} />
                  </td>
                  <td className="px-4 py-3 text-sm capitalize">{p.status}</td>
                  <td className="px-4 py-3">
                    <span className="inline-flex items-center gap-1 flex-wrap">
                      {sameIp && (
                        <span className="inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-sky-500/10 text-sky-600 dark:text-sky-400 border border-sky-500/20">
                          same IP
                        </span>
                      )}
                      {samePayer && (
                        <span className="inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-purple-500/10 text-purple-600 dark:text-purple-400 border border-purple-500/20">
                          same payer
                        </span>
                      )}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-sm tabular-nums text-right">
                    {p.connection_count > 0 ? p.connection_count : <span className="text-muted-foreground">—</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {totalPages > 1 && (
          <div className="flex items-center justify-between px-4 py-2 border-t border-border bg-muted/20 text-xs text-muted-foreground">
            <span>{page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, rows.length)} of {rows.length}</span>
            <div className="flex items-center gap-1">
              <button
                onClick={() => setPage(p => p - 1)}
                disabled={page === 0}
                className="p-1 rounded hover:bg-muted disabled:opacity-30 disabled:cursor-not-allowed"
              >
                <ChevronLeft className="h-3.5 w-3.5" />
              </button>
              <span>{page + 1} / {totalPages}</span>
              <button
                onClick={() => setPage(p => p + 1)}
                disabled={page === totalPages - 1}
                className="p-1 rounded hover:bg-muted disabled:opacity-30 disabled:cursor-not-allowed"
              >
                <ChevronRight className="h-3.5 w-3.5" />
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

function ShredsSubscriptionCard({ seat }: { seat: AccessPassShredsSeat }) {
  const balanceDollars = seat.spendable_usdc_balance / 1_000_000
  const allEscrowsDollars = seat.all_escrows_usdc_balance / 1_000_000
  return (
    <div className="border border-border rounded-lg p-4 bg-card">
      <h3 className="text-sm font-medium text-muted-foreground mb-3 flex items-center gap-2">
        Shreds Subscription
        <span className="inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border border-emerald-500/20">
          seat
        </span>
      </h3>
      <dl className="space-y-2">
        <div className="flex justify-between gap-4">
          <dt className="text-sm text-muted-foreground shrink-0">Seat</dt>
          <dd className="text-sm font-mono min-w-0">
            <Link
              to={`/dz/shreds/subscribers?system=seats&search=seat%3A${seat.pk}`}
              className="text-blue-600 dark:text-blue-400 hover:underline"
            >
              {seat.pk.slice(0, 6)}…{seat.pk.slice(-4)}
            </Link>
          </dd>
        </div>
        {seat.device_code && (
          <div className="flex justify-between gap-4">
            <dt className="text-sm text-muted-foreground shrink-0">Device</dt>
            <dd className="text-sm">
              {seat.device_code}
              {seat.metro_code && <span className="text-muted-foreground ml-1">({seat.metro_code})</span>}
            </dd>
          </div>
        )}
        <div className="flex justify-between gap-4">
          <dt className="text-sm text-muted-foreground shrink-0">Tenure</dt>
          <dd className="text-sm tabular-nums">
            {seat.tenure_epochs} epoch{seat.tenure_epochs !== 1 ? 's' : ''}
          </dd>
        </div>
        <div className="flex justify-between gap-4">
          <dt className="text-sm text-muted-foreground shrink-0">Price / Epoch</dt>
          <dd className="text-sm tabular-nums">${seat.price_per_epoch_dollars}</dd>
        </div>
        <div className="flex justify-between gap-4">
          <dt className="text-sm text-muted-foreground shrink-0">Escrow Balance</dt>
          <dd className="text-sm tabular-nums text-right">
            ${balanceDollars.toFixed(2)}
            {seat.escrow_count > 1 && (
              <div
                className="text-xs text-muted-foreground"
                title="Balances are evaluated per escrow; only the largest single escrow can cover a charge."
              >
                {seat.escrow_count} escrows · ${allEscrowsDollars.toFixed(2)} total
              </div>
            )}
          </dd>
        </div>

      </dl>
    </div>
  )
}

const KIND_COLORS: Record<string, string> = {
  unicast: 'bg-blue-500/10 text-blue-600 dark:text-blue-400 border-blue-500/20',
  multicast: 'bg-green-500/10 text-green-600 dark:text-green-400 border-green-500/20',
}

function ConnectionsTable({ connections, navigate }: {
  connections: AccessPassConnection[]
  navigate: ReturnType<typeof useNavigate>
}) {
  const [page, setPage] = useState(0)

  if (connections.length === 0) return null

  const totalPages = Math.ceil(connections.length / PAGE_SIZE)
  const pageRows = connections.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)

  return (
    <div>
      <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider mb-3">
        Connections ({connections.length})
      </h2>
      <div className="border border-border rounded-lg overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border bg-muted/30 text-xs text-muted-foreground uppercase tracking-wider">
                <th className="px-4 py-3 font-medium text-left">Owner</th>
                <th className="px-4 py-3 font-medium text-left">Kind</th>
                <th className="px-4 py-3 font-medium text-left">Status</th>
                <th className="px-4 py-3 font-medium text-left">DZ IP</th>
                <th className="px-4 py-3 font-medium text-left">Client IP</th>
                <th className="px-4 py-3 font-medium text-left">Device</th>
                <th className="px-4 py-3 font-medium text-left">Metro</th>
              </tr>
            </thead>
            <tbody>
              {pageRows.map((c) => (
                <tr
                  key={c.pk}
                  className="border-b border-border last:border-b-0 hover:bg-muted cursor-pointer transition-colors"
                  onClick={(e) => handleRowClick(e, `/dz/users/${c.pk}`, navigate)}
                >
                  <td className="px-4 py-3 font-mono text-sm text-muted-foreground">
                    {c.owner_pubkey ? `${c.owner_pubkey.slice(0, 6)}…${c.owner_pubkey.slice(-4)}` : '—'}
                  </td>
                  <td className="px-4 py-3">
                    {c.kind ? (
                      <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium border ${KIND_COLORS[c.kind] ?? 'bg-muted text-muted-foreground border-border'}`}>
                        {c.kind}
                      </span>
                    ) : <span className="text-muted-foreground">—</span>}
                  </td>
                  <td className="px-4 py-3 text-sm capitalize">{c.status}</td>
                  <td className="px-4 py-3 font-mono text-sm">{c.dz_ip || <span className="text-muted-foreground">—</span>}</td>
                  <td className="px-4 py-3 font-mono text-sm">{c.client_ip || <span className="text-muted-foreground">—</span>}</td>
                  <td className="px-4 py-3 text-sm font-mono">{c.device_code || <span className="text-muted-foreground">—</span>}</td>
                  <td className="px-4 py-3 text-sm">{c.metro_code || <span className="text-muted-foreground">—</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {totalPages > 1 && (
          <div className="flex items-center justify-between px-4 py-2 border-t border-border bg-muted/20 text-xs text-muted-foreground">
            <span>{page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, connections.length)} of {connections.length}</span>
            <div className="flex items-center gap-1">
              <button
                onClick={() => setPage(p => p - 1)}
                disabled={page === 0}
                className="p-1 rounded hover:bg-muted disabled:opacity-30 disabled:cursor-not-allowed"
              >
                <ChevronLeft className="h-3.5 w-3.5" />
              </button>
              <span>{page + 1} / {totalPages}</span>
              <button
                onClick={() => setPage(p => p + 1)}
                disabled={page === totalPages - 1}
                className="p-1 rounded hover:bg-muted disabled:opacity-30 disabled:cursor-not-allowed"
              >
                <ChevronRight className="h-3.5 w-3.5" />
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export function AccessPassDetailPage() {
  const { pk } = useParams<{ pk: string }>()
  const navigate = useNavigate()
  const back = useBackLink({ to: '/dz/access-passes', label: 'access passes' })

  const { data: ap, isLoading, error } = useQuery({
    queryKey: ['access-pass', pk],
    queryFn: () => fetchAccessPass(pk!),
    enabled: !!pk,
  })

  const isShredsProductPayer = !!ap?.user_payer && SHREDS_INTERNAL_USER_PAYERS.has(ap.user_payer)

  const { data: byIpData } = useQuery({
    queryKey: ['access-passes-by-ip', ap?.client_ip],
    queryFn: () => fetchAccessPasses(50, 0, 'type', 'asc', [`client_ip:${ap!.client_ip}`]),
    enabled: !!ap?.client_ip,
  })

  const { data: byPayerData } = useQuery({
    queryKey: ['access-passes-by-payer', ap?.user_payer],
    queryFn: () => fetchAccessPasses(50, 0, 'type', 'asc', [`user_payer:${ap!.user_payer}`]),
    enabled: !!ap?.user_payer && !isShredsProductPayer,
  })

  const { data: connectionsData } = useQuery({
    queryKey: ['access-pass-connections', pk],
    queryFn: () => fetchAccessPassConnections(pk!),
    enabled: !!pk,
  })

  useDocumentTitle(ap?.pk ? `${ap.pk.slice(0, 8)}…` : 'Access Pass')

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error || !ap) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Access pass not found</div>
          <button
            onClick={() => navigate(back.to)}
            className="text-sm text-muted-foreground hover:text-foreground"
          >
            Back to {back.label}
          </button>
        </div>
      </div>
    )
  }

  const statusColor = STATUS_COLORS[ap.status] ?? 'text-muted-foreground'

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 py-8">
        {/* Back button */}
        <button
          onClick={() => navigate(back.to)}
          className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground mb-6"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to {back.label}
        </button>

        {/* Header */}
        <div className="flex items-center gap-3 mb-8">
          <KeyRound className="h-8 w-8 text-muted-foreground shrink-0" />
          <div>
            <div className="flex items-center gap-3 flex-wrap">
              <h1 className="text-2xl font-medium font-mono">
                {ap.pk.slice(0, 8)}…{ap.pk.slice(-4)}
              </h1>
              <TypeTagBadge tag={ap.type_tag} />
              <span className={`text-sm capitalize font-medium ${statusColor}`}>{ap.status}</span>
            </div>
            <CopyableText text={ap.pk} className="text-xs font-mono text-muted-foreground mt-0.5" />
          </div>
        </div>

        {/* Info grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-10">
          {/* Identity */}
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Identity</h3>
            <dl className="space-y-2">
              <div className="flex justify-between gap-4">
                <dt className="text-sm text-muted-foreground shrink-0">Owner</dt>
                <dd className="text-sm font-mono min-w-0">
                  {ap.owner_pubkey
                    ? <CopyableText text={ap.owner_pubkey}>{ap.owner_pubkey.slice(0, 6)}…{ap.owner_pubkey.slice(-4)}</CopyableText>
                    : <span className="text-muted-foreground">—</span>}
                </dd>
              </div>
              <div className="flex justify-between gap-4">
                <dt className="text-sm text-muted-foreground shrink-0">User Payer</dt>
                <dd className="text-sm font-mono min-w-0">
                  {ap.user_payer
                    ? <CopyableText text={ap.user_payer}>{ap.user_payer.slice(0, 6)}…{ap.user_payer.slice(-4)}</CopyableText>
                    : <span className="text-muted-foreground">—</span>}
                </dd>
              </div>

            </dl>
          </div>

          {/* Connection */}
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Connection</h3>
            <dl className="space-y-2">
              <div className="flex justify-between gap-4">
                <dt className="text-sm text-muted-foreground shrink-0">Client IP</dt>
                <dd className="text-sm font-mono">{ap.client_ip || <span className="text-muted-foreground">—</span>}</dd>
              </div>
              <div className="flex justify-between gap-4">
                <dt className="text-sm text-muted-foreground shrink-0">Connections</dt>
                <dd className="text-sm tabular-nums">{ap.connection_count}</dd>
              </div>

            </dl>
          </div>

          {/* Shreds Subscription (only shown when user_payer is the Shreds product payer) */}
          {ap.shreds_seat && <ShredsSubscriptionCard seat={ap.shreds_seat} />}

          {/* Association */}
          {!ap.shreds_seat && <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Association</h3>
            <dl className="space-y-2">
              {ap.type_tag === 'solana_validator' && (
                <>
                  <div className="flex justify-between gap-4">
                    <dt className="text-sm text-muted-foreground shrink-0">Pubkey</dt>
                    <dd className="text-sm font-mono min-w-0">
                      {ap.associated_pubkey
                        ? <CopyableText text={ap.associated_pubkey}>{ap.associated_pubkey.slice(0, 6)}…{ap.associated_pubkey.slice(-4)}</CopyableText>
                        : <span className="text-muted-foreground">—</span>}
                    </dd>
                  </div>
                  {ap.validator_vote_pubkey && (
                    <div className="flex justify-between gap-4">
                      <dt className="text-sm text-muted-foreground shrink-0">Vote Account</dt>
                      <dd className="text-sm font-mono">
                        <Link
                          to={`/solana/validators/${ap.validator_vote_pubkey}`}
                          className="text-blue-600 dark:text-blue-400 hover:underline"
                        >
                          {ap.validator_vote_pubkey.slice(0, 6)}…{ap.validator_vote_pubkey.slice(-4)}
                        </Link>
                      </dd>
                    </div>
                  )}
                  {ap.validator_node_pubkey && (
                    <div className="flex justify-between gap-4">
                      <dt className="text-sm text-muted-foreground shrink-0">Identity</dt>
                      <dd className="text-sm font-mono">
                        <Link
                          to={`/solana/gossip-nodes/${ap.validator_node_pubkey}`}
                          className="text-blue-600 dark:text-blue-400 hover:underline"
                        >
                          {ap.validator_node_pubkey.slice(0, 6)}…{ap.validator_node_pubkey.slice(-4)}
                        </Link>
                      </dd>
                    </div>
                  )}
                </>
              )}

              {ap.type_tag === 'solana_rpc' && (
                <div className="flex justify-between gap-4">
                  <dt className="text-sm text-muted-foreground shrink-0">Pubkey</dt>
                  <dd className="text-sm font-mono min-w-0">
                    {ap.associated_pubkey
                      ? <CopyableText text={ap.associated_pubkey}>{ap.associated_pubkey.slice(0, 6)}…{ap.associated_pubkey.slice(-4)}</CopyableText>
                      : <span className="text-muted-foreground">—</span>}
                  </dd>
                </div>
              )}

              {ap.type_tag === 'others' && (
                <>
                  {ap.others_type_name && (
                    <div className="flex justify-between gap-4">
                      <dt className="text-sm text-muted-foreground shrink-0">Type Name</dt>
                      <dd className="text-sm font-mono">{ap.others_type_name}</dd>
                    </div>
                  )}
                  {ap.others_key && (
                    <div className="flex justify-between gap-4">
                      <dt className="text-sm text-muted-foreground shrink-0">Key</dt>
                      <dd className="text-sm font-mono">{ap.others_key}</dd>
                    </div>
                  )}
                </>
              )}

              {ap.type_tag === 'prepaid' && (
                <div className="text-sm text-muted-foreground">No association</div>
              )}
            </dl>
          </div>}
        </div>

        <div className="space-y-8">
          <MulticastAllowlistTable
            pubGroups={ap.mgroup_pub_allowlist}
            subGroups={ap.mgroup_sub_allowlist}
          />

          <RelatedPassesTable
            byIpPasses={byIpData?.items ?? []}
            byPayerPasses={byPayerData?.items ?? []}
            currentPk={ap.pk}
            navigate={navigate}
          />

          <ConnectionsTable
            connections={connectionsData ?? []}
            navigate={navigate}
          />
        </div>
      </div>
    </div>
  )
}
