import { useState, useMemo, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useWallet } from '@solana/wallet-adapter-react'
import { PublicKey } from '@solana/web3.js'
import { useSearchParams } from 'react-router-dom'
import { Loader2, AlertCircle, Zap } from 'lucide-react'
import {
  fetchShredDevices,
  fetchShredsOverview,
  fetchMetros,
  type ShredDevice,
} from '@/lib/api'
import { ipv4ToU32, isValidIpv4 } from '@/lib/shred-program'
import {
  deriveShredAccounts,
  buildSubscribeInstructions,
} from '@/lib/shred-transactions'
import { useShredAccounts, useUsdcBalance } from '@/hooks/use-shred-accounts'
import { useShredTransaction } from '@/hooks/use-shred-transaction'
import { useEpochInfo } from '@/hooks/use-epoch-info'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { useAuth } from '@/contexts/AuthContext'
import { MapLanding } from './shreds/map-landing'
import { Wizard } from './shreds/wizard'
import { WithdrawWizard } from './shreds/withdraw-wizard'
import { UserPopover } from './auth/UserPopover'

export function ShredsSubscribePage() {
  useDocumentTitle('Buy Shreds')

  const [searchParams, setSearchParams] = useSearchParams()
  const deviceParam = searchParams.get('device') || ''
  const metroParam = searchParams.get('metro')
  const modeParam = searchParams.get('mode') === 'withdraw' ? 'withdraw' : 'subscribe'
  const { publicKey: wallet, connected } = useWallet()
  const { isAuthenticated, user } = useAuth()

  const handleSelectMode = useCallback((next: 'subscribe' | 'withdraw') => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (next === 'subscribe') p.delete('mode')
      else p.set('mode', next)
      return p
    })
  }, [setSearchParams])

  // ---- Data fetching ------------------------------------------------------

  const { data: pricing, isLoading: pricingLoading, error: pricingError } = useQuery({
    queryKey: ['shred-devices-subscribe'],
    queryFn: () => fetchShredDevices({ limit: 1000, offset: 0, sortBy: 'device', sortDir: 'asc' }),
    select: (data) => data.items,
    refetchInterval: 30_000,
  })

  const { data: metros } = useQuery({
    queryKey: ['metros-for-shreds'],
    queryFn: () => fetchMetros(1000, 0),
    select: (data) => data.items,
  })

  const { data: epochInfo } = useEpochInfo()

  const { data: overview } = useQuery({
    queryKey: ['shreds-overview'],
    queryFn: fetchShredsOverview,
    refetchInterval: 30_000,
  })

  // ---- Form state (cross-stage) -------------------------------------------

  const [clientIp, setClientIp] = useState('')
  const [amountStr, setAmountStr] = useState('')
  const ipValid = clientIp === '' || isValidIpv4(clientIp)
  const amount = parseFloat(amountStr)
  const amountValid = !isNaN(amount) && amount > 0

  // ---- Derived ------------------------------------------------------------

  const devices = useMemo(() => pricing ?? [], [pricing])
  const allMetros = useMemo(() => metros ?? [], [metros])

  // selectedDevice is purely derived from the URL `?device=` param.
  const selectedDevice = useMemo<ShredDevice | null>(() => {
    if (!deviceParam || devices.length === 0) return null
    return devices.find(d => d.device_code === deviceParam || d.device_key === deviceParam) ?? null
  }, [deviceParam, devices])

  const devicePubkey = useMemo(() => {
    if (!selectedDevice) return null
    try { return new PublicKey(selectedDevice.device_key) } catch { return null }
  }, [selectedDevice])

  const shredState = useShredAccounts(devicePubkey, clientIp && isValidIpv4(clientIp) ? clientIp : null)
  const { balance: usdcBalance } = useUsdcBalance()

  const { status: txStatus, txSignature, error: txError, execute, simulate, reset: resetTx } = useShredTransaction()

  // Simulate mode: dev-only, activated via ?simulate=true in the URL
  const simulateMode = import.meta.env.DEV && searchParams.get('simulate') === 'true'

  const pricePerEpoch = selectedDevice ? selectedDevice.total_price_dollars : 0
  const prepaidEpochs = pricePerEpoch > 0 && amountValid ? Math.floor(amount / pricePerEpoch) : 0
  const amountMicro = amountValid ? BigInt(Math.floor(amount * 1_000_000)) : 0n
  const minAmount = pricePerEpoch > 0 ? pricePerEpoch : 0
  const amountBelowMin = amountValid && minAmount > 0 && amount < minAmount
  const insufficientBalance = amountValid && amountMicro > usdcBalance

  const canSubmit = Boolean(
    connected &&
    selectedDevice &&
    isValidIpv4(clientIp) &&
    amountValid &&
    !amountBelowMin &&
    (!insufficientBalance || simulateMode) &&
    txStatus === 'idle',
  )

  // ---- Transaction handlers (preserved verbatim) --------------------------

  const handleSubscribe = useCallback(async () => {
    if (!canSubmit || !wallet || !selectedDevice || !devicePubkey) return

    const clientIpBits = ipv4ToU32(clientIp)

    const accounts = deriveShredAccounts({
      device: devicePubkey,
      metroExchange: new PublicKey(selectedDevice.metro_exchange_key),
      clientIpBits,
      wallet,
    })

    const instructions = buildSubscribeInstructions({
      accounts,
      wallet,
      clientIpBits,
      amountMicro,
      seatExists: shredState.seatExists,
      escrowExists: shredState.escrowExists,
      seatActive: shredState.seatActive,
    })

    await execute(instructions)
  }, [canSubmit, wallet, selectedDevice, devicePubkey, clientIp, amountMicro, shredState, execute])

  const handleSimulate = useCallback(async () => {
    if (!canSubmit || !wallet || !selectedDevice || !devicePubkey) return

    const clientIpBits = ipv4ToU32(clientIp)

    const accounts = deriveShredAccounts({
      device: devicePubkey,
      metroExchange: new PublicKey(selectedDevice.metro_exchange_key),
      clientIpBits,
      wallet,
    })

    const instructions = buildSubscribeInstructions({
      accounts,
      wallet,
      clientIpBits,
      amountMicro,
      seatExists: shredState.seatExists,
      escrowExists: shredState.escrowExists,
      seatActive: shredState.seatActive,
    })

    await simulate(instructions)
  }, [canSubmit, wallet, selectedDevice, devicePubkey, clientIp, amountMicro, shredState, simulate])

  // ---- URL helpers --------------------------------------------------------

  const handleSelectDevice = useCallback((d: ShredDevice) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      next.set('device', d.device_code || d.device_key)
      next.set('metro', d.metro_code)
      return next
    })
  }, [setSearchParams])

  const handleSelectMetro = useCallback((metro: string | null) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      if (metro) next.set('metro', metro)
      else next.delete('metro')
      return next
    })
  }, [setSearchParams])

  const handleChangeDevice = useCallback(() => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      next.delete('device')
      // keep ?metro= so the drawer pre-opens
      return next
    })
    resetTx()
  }, [setSearchParams, resetTx])

  const handleStartOver = useCallback(() => {
    resetTx()
    setClientIp('')
    setAmountStr('')
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      next.delete('device')
      next.delete('metro')
      return next
    })
  }, [resetTx, setSearchParams])

  // ---- Loading / error ----------------------------------------------------

  if (pricingLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (pricingError) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Unable to load pricing</div>
          <div className="text-sm text-muted-foreground">{pricingError.message}</div>
        </div>
      </div>
    )
  }

  // ---- Chrome -------------------------------------------------------------

  const userEmail = user?.email ?? null
  const laneBanner = (
    <div className="px-4 py-2 rounded-lg border border-border bg-card text-sm">
      {isAuthenticated ? (
        <span>
          <span className="text-muted-foreground">Signed in as </span>
          <span className="font-medium">{userEmail ?? 'user'}</span>
          <span className="text-muted-foreground"> · Your wallet still signs the on-chain transaction.</span>
        </span>
      ) : (
        <span className="text-muted-foreground">
          You're checking out as <span className="font-medium text-foreground">guest</span>.{' '}
          Sign in for receipt history & team seats <span className="italic">(coming soon)</span>.
        </span>
      )}
    </div>
  )

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-6xl mx-auto px-4 sm:px-8 py-6 space-y-4">
        {/* Persistent chrome: title + sign-in */}
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <Zap className="h-6 w-6 text-primary" />
            <h1 className="text-2xl font-medium">Buy Shreds</h1>
          </div>
          <div className="min-w-[140px]">
            <UserPopover collapsed={false} />
          </div>
        </div>

        <ModeTabs mode={modeParam} onChange={handleSelectMode} />

        {modeParam === 'withdraw' ? (
          <WithdrawWizard />
        ) : selectedDevice ? (
          <>
            {laneBanner}
            <Wizard
              selectedDevice={selectedDevice}
              clientIp={clientIp}
              setClientIp={setClientIp}
              ipValid={ipValid}
              amountStr={amountStr}
              setAmountStr={setAmountStr}
              amount={amount}
              amountValid={amountValid}
              amountBelowMin={amountBelowMin}
              pricePerEpoch={pricePerEpoch}
              prepaidEpochs={prepaidEpochs}
              minAmount={minAmount}
              usdcBalance={usdcBalance}
              insufficientBalance={insufficientBalance}
              shredState={shredState}
              epochInfo={epochInfo}
              overview={overview}
              connected={connected}
              walletPublicKey={wallet}
              txStatus={txStatus}
              txSignature={txSignature}
              txError={txError}
              simulateMode={simulateMode}
              canSubmit={canSubmit}
              onSubscribe={handleSubscribe}
              onSimulate={handleSimulate}
              onReset={resetTx}
              onChangeDevice={handleChangeDevice}
              onStartOver={handleStartOver}
              isAuthenticated={isAuthenticated}
              userEmail={userEmail}
            />
          </>
        ) : (
          <MapLanding
            devices={devices}
            metros={allMetros}
            selectedMetro={metroParam}
            onSelectMetro={handleSelectMetro}
            onSelectDevice={handleSelectDevice}
            laneBanner={laneBanner}
          />
        )}
      </div>
    </div>
  )
}

function ModeTabs({
  mode,
  onChange,
}: {
  mode: 'subscribe' | 'withdraw'
  onChange: (m: 'subscribe' | 'withdraw') => void
}) {
  return (
    <div className="inline-flex rounded-lg border border-border overflow-hidden text-sm">
      <button
        onClick={() => onChange('subscribe')}
        className={`px-4 py-1.5 transition-colors ${
          mode === 'subscribe' ? 'bg-foreground text-background' : 'bg-background text-muted-foreground hover:bg-muted'
        }`}
      >
        Subscribe
      </button>
      <button
        onClick={() => onChange('withdraw')}
        className={`px-4 py-1.5 transition-colors border-l border-border ${
          mode === 'withdraw' ? 'bg-foreground text-background' : 'bg-background text-muted-foreground hover:bg-muted'
        }`}
      >
        Withdraw
      </button>
    </div>
  )
}
