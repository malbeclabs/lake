import { useMemo, useState, useCallback, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams, Link } from 'react-router-dom'
import { useWallet } from '@solana/wallet-adapter-react'
import { WalletMultiButton } from '@solana/wallet-adapter-react-ui'
import { Download, RefreshCw, Plus, Layers, LogIn, ArrowRight } from 'lucide-react'
import {
  fetchShredClientSeats,
  fetchShredsOverview,
  type ShredClientSeat,
} from '@/lib/api'
import { useAuth } from '@/contexts/AuthContext'
import { useIsOpsUser } from '@/hooks/use-is-ops-user'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { UserPopover } from '@/components/auth/UserPopover'
import { ConsoleStatStrip } from './console-stat-strip'
import { ConsoleToolbar, type ConsoleFilters } from './console-toolbar'
import { ConsoleTable } from './console-table'
import { ConsoleBulkBar } from './console-bulk-bar'
import { ConsoleEmptyState } from './console-empty-state'
import { SubscriptionDrawer } from './subscription-drawer'
import { BulkDepositModal } from './bulk-deposit-modal'
import { ShredFundModal } from '@/components/shred-fund-modal'
import { ShredWithdrawModal } from '@/components/shred-withdraw-modal'
import { deriveStatus, runwayEpochs } from './subscription-status'
import { PREVIEW_SEATS, PREVIEW_CURRENT_EPOCH } from './preview-fixtures'
import { PreviewBanner } from './preview-banner'

export function SubscriptionsConsolePage() {
  useDocumentTitle('My Subscriptions')
  const { user, isAuthenticated, loginWithGoogle, loginWithWallet, isLoading } = useAuth()
  const { publicKey: walletPubkey } = useWallet()
  const isInternal = useIsOpsUser()
  const [searchParams, setSearchParams] = useSearchParams()
  const previewParam = searchParams.get('preview') === 'true'
  const preview = isInternal && previewParam

  // Funder pubkey resolution: connected wallet → linked wallet on account → null.
  const funderPubkey = useMemo(() => {
    if (walletPubkey) return walletPubkey.toBase58()
    if (user?.wallet_address) return user.wallet_address
    return null
  }, [walletPubkey, user?.wallet_address])

  const enterPreview = useCallback(() => {
    const next = new URLSearchParams(searchParams)
    next.set('preview', 'true')
    setSearchParams(next)
  }, [searchParams, setSearchParams])

  const exitPreview = useCallback(() => {
    const next = new URLSearchParams(searchParams)
    next.delete('preview')
    setSearchParams(next)
  }, [searchParams, setSearchParams])

  let body: React.ReactNode
  if (isLoading) {
    body = (
      <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
        Loading…
      </div>
    )
  } else if (!isAuthenticated) {
    body = <SignInPanel onGoogle={loginWithGoogle} onWallet={loginWithWallet} />
  } else if (!funderPubkey && !preview) {
    body = <ConnectWalletPanel isInternal={isInternal} onEnterPreview={enterPreview} />
  } else {
    body = (
      <ConsoleInner
        funderPubkey={funderPubkey ?? 'preview-funder'}
        userEmail={user?.email ?? null}
        isInternal={isInternal}
        preview={preview}
        onEnterPreview={enterPreview}
        onExitPreview={exitPreview}
      />
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="mx-auto w-full max-w-6xl space-y-4 px-4 py-6 sm:px-8">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <Layers className="h-6 w-6 text-primary" />
            <h1 className="text-2xl font-medium">My Subscriptions</h1>
          </div>
          <div className="min-w-[140px]">
            <UserPopover collapsed={false} />
          </div>
        </div>
        <ShredsModeTabs current="manage" isAuthenticated={isAuthenticated} />
        {body}
      </div>
    </div>
  )
}

function ShredsModeTabs({
  current, isAuthenticated,
}: { current: 'subscribe' | 'withdraw' | 'manage'; isAuthenticated: boolean }) {
  const cls = (active: boolean) =>
    `px-4 py-1.5 transition-colors ${
      active ? 'bg-foreground text-background' : 'bg-background text-muted-foreground hover:bg-muted'
    }`
  return (
    <div className="inline-flex overflow-hidden rounded-lg border border-border text-sm">
      <Link to="/dz/shreds/pay" className={cls(current === 'subscribe')}>Subscribe</Link>
      <Link to="/dz/shreds/pay?mode=withdraw" className={`${cls(current === 'withdraw')} border-l border-border`}>Withdraw</Link>
      {isAuthenticated && (
        <Link to="/account/subscriptions" className={`${cls(current === 'manage')} border-l border-border`}>Manage</Link>
      )}
    </div>
  )
}

function ConsoleInner({
  funderPubkey, userEmail, isInternal, preview, onEnterPreview, onExitPreview,
}: {
  funderPubkey: string
  userEmail: string | null
  isInternal: boolean
  preview: boolean
  onEnterPreview: () => void
  onExitPreview: () => void
}) {
  const [searchParams, setSearchParams] = useSearchParams()
  const selectedPk = searchParams.get('selected')
  const modal = searchParams.get('modal')
  const modalSeats = searchParams.get('seats')?.split(',').filter(Boolean) ?? []

  const seatsQuery = useQuery({
    queryKey: ['shred-client-seats', 'console', funderPubkey],
    queryFn: () =>
      fetchShredClientSeats({
        limit: 500,
        filters: [`funder:${funderPubkey}`],
        sortBy: 'active_epoch',
        sortDir: 'desc',
      }),
    enabled: !preview,
  })

  const overviewQuery = useQuery({
    queryKey: ['shreds-overview'],
    queryFn: fetchShredsOverview,
    enabled: !preview,
  })

  const allSeats = preview ? PREVIEW_SEATS : (seatsQuery.data?.items ?? [])
  const currentEpoch = preview ? PREVIEW_CURRENT_EPOCH : (overviewQuery.data?.current_subscription_epoch ?? 0)
  const metros = useMemo(() => uniq(allSeats.map((s) => s.metro_code).filter(Boolean)), [allSeats])

  const [filters, setFilters] = useState<ConsoleFilters>({
    query: '',
    status: 'all',
    metro: 'all',
    lowEscrowOnly: false,
  })

  const visibleSeats = useMemo(
    () => filterSeats(allSeats, filters, currentEpoch),
    [allSeats, filters, currentEpoch],
  )

  const [selectedSet, setSelectedSet] = useState<Set<string>>(new Set())

  const toggleOne = useCallback((pk: string) => {
    setSelectedSet((prev) => {
      const next = new Set(prev)
      if (next.has(pk)) next.delete(pk); else next.add(pk)
      return next
    })
  }, [])

  const toggleAll = useCallback(() => {
    setSelectedSet((prev) => {
      const allHere = visibleSeats.every((s) => prev.has(s.pk))
      const next = new Set(prev)
      if (allHere) {
        for (const s of visibleSeats) next.delete(s.pk)
      } else {
        for (const s of visibleSeats) next.add(s.pk)
      }
      return next
    })
  }, [visibleSeats])

  const clearSelection = useCallback(() => setSelectedSet(new Set()), [])

  // Keep URL-driven modal seats in sync when the selection is cleared.
  useEffect(() => {
    if (modal === 'bulk-deposit' && modalSeats.length === 0) {
      const next = new URLSearchParams(searchParams)
      next.delete('modal'); next.delete('seats')
      setSearchParams(next, { replace: true })
    }
  }, [modal, modalSeats.length, searchParams, setSearchParams])

  const openDrawer = useCallback((seat: ShredClientSeat) => {
    const next = new URLSearchParams(searchParams)
    next.set('selected', seat.pk)
    setSearchParams(next)
  }, [searchParams, setSearchParams])

  const closeDrawer = useCallback(() => {
    const next = new URLSearchParams(searchParams)
    next.delete('selected')
    setSearchParams(next)
  }, [searchParams, setSearchParams])

  const openModal = useCallback((kind: 'deposit' | 'withdraw' | 'bulk-deposit' | 'bulk-withdraw', seats: string[]) => {
    const next = new URLSearchParams(searchParams)
    next.set('modal', kind)
    next.set('seats', seats.join(','))
    setSearchParams(next)
  }, [searchParams, setSearchParams])

  const closeModal = useCallback(() => {
    const next = new URLSearchParams(searchParams)
    next.delete('modal'); next.delete('seats')
    setSearchParams(next)
  }, [searchParams, setSearchParams])

  const handleRowAction = useCallback(
    (seat: ShredClientSeat, action: 'deposit' | 'withdraw') => openModal(action, [seat.pk]),
    [openModal],
  )

  const selectedSeats = useMemo(
    () => allSeats.filter((s) => selectedSet.has(s.pk)),
    [allSeats, selectedSet],
  )

  const modalSeatsResolved = useMemo(
    () => allSeats.filter((s) => modalSeats.includes(s.pk)),
    [allSeats, modalSeats],
  )

  const openSeat = useMemo(
    () => allSeats.find((s) => s.pk === selectedPk) ?? null,
    [allSeats, selectedPk],
  )

  const totalActive = allSeats.filter((s) => deriveStatus(s, currentEpoch) !== 'expired').length

  return (
    <>
      {preview && <PreviewBanner onExit={onExitPreview} />}
      <div className="flex flex-wrap items-center gap-3 rounded-lg border border-border bg-card px-4 py-2 text-sm">
        <span>
          <span className="text-muted-foreground">Signed in as </span>
          <span className="font-medium">{userEmail ?? 'wallet user'}</span>
          <span className="text-muted-foreground"> · Your wallet still signs the on-chain transaction.</span>
        </span>
      </div>

      <div className="flex flex-wrap items-end justify-between gap-3">
        <div className="text-[13px] text-muted-foreground">
          {totalActive} active across {metros.length} metro{metros.length === 1 ? '' : 's'} · each funded
          by a USDC escrow that drains per epoch.
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => exportSeatsCSV(visibleSeats, currentEpoch)}
            className="inline-flex items-center gap-1.5 rounded-md border border-border bg-background px-3 py-1.5 text-[12.5px] hover:bg-card"
          >
            <Download className="h-3.5 w-3.5" /> Export
          </button>
          <button
            onClick={() => { if (!preview) { seatsQuery.refetch(); overviewQuery.refetch() } }}
            disabled={preview}
            className="inline-flex items-center gap-1.5 rounded-md border border-border bg-background px-3 py-1.5 text-[12.5px] hover:bg-card disabled:opacity-50"
          >
            <RefreshCw className={`h-3.5 w-3.5 ${seatsQuery.isFetching ? 'animate-spin' : ''}`} /> Refresh
          </button>
          <Link
            to="/dz/shreds/pay"
            className="inline-flex items-center gap-1.5 rounded-md bg-primary px-3 py-1.5 text-[12.5px] font-medium text-primary-foreground hover:bg-primary/90"
          >
            <Plus className="h-3.5 w-3.5" /> New subscription
          </Link>
        </div>
      </div>

      {!preview && seatsQuery.isLoading ? (
        <div className="rounded-xl border border-border bg-card p-12 text-center text-sm text-muted-foreground">
          Loading subscriptions…
        </div>
      ) : !preview && seatsQuery.error ? (
        <div className="rounded-xl border border-red-500/40 bg-red-500/5 p-6 text-sm text-red-400">
          Failed to load subscriptions.
        </div>
      ) : allSeats.length === 0 ? (
        <ConsoleEmptyState isInternal={isInternal} onEnterPreview={onEnterPreview} />
      ) : (
        <>
          {selectedSet.size > 0 ? (
            <ConsoleBulkBar
              selected={selectedSeats}
              totalRows={allSeats.length}
              onClear={clearSelection}
              onDeposit={() => openModal('bulk-deposit', [...selectedSet])}
              onExport={() => exportSeatsCSV(selectedSeats, currentEpoch)}
              onWithdraw={() => openModal('bulk-withdraw', [...selectedSet])}
            />
          ) : (
            <ConsoleStatStrip seats={allSeats} currentEpoch={currentEpoch} />
          )}

          {openSeat && (
            <div className="mb-4">
              <SubscriptionDrawer
                seat={openSeat}
                currentEpoch={currentEpoch}
                onClose={closeDrawer}
                onDeposit={() => openModal('deposit', [openSeat.pk])}
                onWithdraw={() => openModal('withdraw', [openSeat.pk])}
                preview={preview}
              />
            </div>
          )}

          <ConsoleToolbar filters={filters} metros={metros} onChange={setFilters} />
          <ConsoleTable
            seats={visibleSeats}
            currentEpoch={currentEpoch}
            selected={selectedSet}
            onToggle={toggleOne}
            onToggleAll={toggleAll}
            onOpenRow={openDrawer}
            onRowAction={handleRowAction}
          />

          <div className="mt-3 flex items-center justify-between text-[12px] text-muted-foreground">
            <span>
              {selectedSet.size > 0
                ? <>{selectedSet.size} selected · ${selectedSeats.reduce((s, x) => s + x.price_per_epoch_dollars, 0).toLocaleString()}/epoch combined</>
                : <>{visibleSeats.length} of {allSeats.length} subscription{allSeats.length === 1 ? '' : 's'}</>}
            </span>
          </div>
        </>
      )}

      {modal === 'deposit' && modalSeatsResolved.length === 1 && (
        <ShredFundModal seat={modalSeatsResolved[0]} onClose={closeModal} preview={preview} />
      )}
      {modal === 'withdraw' && modalSeatsResolved.length === 1 && (
        <ShredWithdrawModal seat={modalSeatsResolved[0]} onClose={closeModal} preview={preview} />
      )}
      {modal === 'bulk-deposit' && modalSeatsResolved.length > 0 && (
        <BulkDepositModal seats={modalSeatsResolved} onClose={closeModal} preview={preview} />
      )}
      {modal === 'bulk-withdraw' && modalSeatsResolved.length > 0 && (
        <BulkWithdrawConfirm seats={modalSeatsResolved} onClose={closeModal} onConfirm={(s) => {
          // Open per-seat withdraw modals one at a time by setting modal=withdraw.
          closeModal(); openModal('withdraw', [s[0].pk])
        }} />
      )}
    </>
  )
}

function SignInPanel({ onGoogle, onWallet }: { onGoogle: () => void; onWallet: () => Promise<void> }) {
  return (
    <div className="mx-auto max-w-md py-24 text-center">
      <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-xl border border-border bg-card text-primary">
        <Layers className="h-5 w-5" />
      </div>
      <h2 className="mb-1.5 text-[18px] font-semibold">Sign in to manage your subscriptions</h2>
      <p className="mb-6 text-[13px] text-muted-foreground">
        Connect your wallet or sign in to see your shred subscriptions, top up escrow, or withdraw remaining USDC.
      </p>
      <div className="flex flex-col items-center gap-2">
        <button
          onClick={() => { void onWallet() }}
          className="inline-flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
        >
          <LogIn className="h-4 w-4" /> Continue with wallet
        </button>
        <button
          onClick={onGoogle}
          className="rounded-md border border-border bg-background px-4 py-2 text-sm hover:bg-card"
        >
          Continue with Google
        </button>
      </div>
    </div>
  )
}

function ConnectWalletPanel({
  isInternal, onEnterPreview,
}: { isInternal: boolean; onEnterPreview: () => void }) {
  return (
    <div className="mx-auto max-w-md py-24 text-center">
      <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-xl border border-border bg-card text-primary">
        <Layers className="h-5 w-5" />
      </div>
      <h2 className="mb-1.5 text-[18px] font-semibold">Connect a wallet</h2>
      <p className="mb-6 text-[13px] text-muted-foreground">
        Your subscriptions are scoped to the wallet that funded them. Connect that wallet to view and manage them.
      </p>
      <div className="flex flex-col items-center gap-3">
        <WalletMultiButton />
        {isInternal && (
          <button
            type="button"
            onClick={onEnterPreview}
            className="inline-flex items-center gap-1.5 text-[12.5px] text-muted-foreground transition-colors hover:text-foreground"
          >
            Or see a sample workflow <ArrowRight className="h-3 w-3" />
          </button>
        )}
      </div>
    </div>
  )
}

function BulkWithdrawConfirm({
  seats, onClose, onConfirm,
}: { seats: ShredClientSeat[]; onClose: () => void; onConfirm: (seats: ShredClientSeat[]) => void }) {
  const totalBal = seats.reduce((s, x) => s + x.total_usdc_balance / 1e6, 0)
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/50" onClick={onClose} />
      <div className="relative mx-4 w-full max-w-md rounded-xl border border-border bg-card shadow-2xl">
        <div className="border-b border-border p-4">
          <h3 className="m-0 text-[15px] font-semibold">Withdraw escrow &amp; cancel {seats.length} subscription{seats.length === 1 ? '' : 's'}</h3>
        </div>
        <div className="space-y-3 p-4 text-[13px]">
          <p>
            This will end shred delivery for these seats and refund <b className="font-mono">${totalBal.toFixed(2)} USDC</b>{' '}
            in total to your wallet. Each seat requires a separate signature.
          </p>
          <p className="text-muted-foreground text-[12px]">
            Bulk withdraw walks through one wallet prompt per seat. You can cancel between seats.
          </p>
          <div className="flex justify-end gap-2">
            <button
              onClick={onClose}
              className="rounded-md px-3 py-2 text-sm text-muted-foreground hover:bg-background hover:text-foreground"
            >
              Cancel
            </button>
            <button
              onClick={() => onConfirm(seats)}
              className="rounded-md bg-red-600 px-4 py-2 text-sm text-white hover:bg-red-700"
            >
              Continue
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}

function uniq<T>(arr: T[]): T[] {
  return Array.from(new Set(arr))
}

function filterSeats(
  seats: ShredClientSeat[],
  f: ConsoleFilters,
  currentEpoch: number,
): ShredClientSeat[] {
  const q = f.query.trim().toLowerCase()
  return seats.filter((s) => {
    if (q) {
      const hay = `${s.pk} ${s.device_code} ${s.device_key} ${s.metro_code} ${s.client_ip}`.toLowerCase()
      if (!hay.includes(q)) return false
    }
    if (f.status !== 'all' && deriveStatus(s, currentEpoch) !== f.status) return false
    if (f.metro !== 'all' && s.metro_code !== f.metro) return false
    if (f.lowEscrowOnly && runwayEpochs(s) >= 1) return false
    return true
  })
}

function exportSeatsCSV(seats: ShredClientSeat[], currentEpoch: number) {
  const header = ['subscription', 'status', 'device', 'metro', 'ip', 'price_per_epoch', 'balance_usdc', 'runway_epochs', 'active_epoch', 'funder']
  const lines = [header.join(',')]
  for (const s of seats) {
    lines.push([
      s.pk,
      deriveStatus(s, currentEpoch),
      s.device_code || s.device_key,
      s.metro_code,
      s.client_ip,
      s.price_per_epoch_dollars,
      (s.total_usdc_balance / 1e6).toFixed(2),
      Number.isFinite(runwayEpochs(s)) ? runwayEpochs(s).toFixed(2) : '∞',
      s.active_epoch,
      s.funding_authority_key,
    ].map(csvCell).join(','))
  }
  const blob = new Blob([lines.join('\n')], { type: 'text/csv' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `subscriptions-${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

function csvCell(v: unknown): string {
  const s = String(v ?? '')
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`
  return s
}
