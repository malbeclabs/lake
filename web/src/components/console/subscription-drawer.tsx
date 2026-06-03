import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { X, Coins, Trash2, AlertTriangle, Check, Clock, ArrowDown, Copy, ExternalLink } from 'lucide-react'
import {
  fetchShredEscrowEvents,
  type ShredClientSeat,
  type ShredEscrowEvent,
} from '@/lib/api'
import { PREVIEW_EVENTS_BY_SEAT } from './preview-fixtures'
import { StatusPill } from './primitives/status-pill'
import { KvGrid, type KvRow } from './primitives/kv-grid'
import {
  balanceDollars,
  deriveStatus,
  formatFundedThrough,
  formatRunway,
  runwayEpochs,
  statusPillFor,
} from './subscription-status'

type DrawerTab = 'overview' | 'ips' | 'activity' | 'receipts' | 'cli'

interface SubscriptionDrawerProps {
  seat: ShredClientSeat
  currentEpoch: number
  onClose: () => void
  onDeposit: () => void
  onWithdraw: () => void
  preview?: boolean
}

export function SubscriptionDrawer({
  seat, currentEpoch, onClose, onDeposit, onWithdraw, preview = false,
}: SubscriptionDrawerProps) {
  const [tab, setTab] = useState<DrawerTab>('overview')
  const status = deriveStatus(seat, currentEpoch)
  const pill = statusPillFor(status)

  return (
    <div className="overflow-hidden rounded-xl border border-border bg-card">
      <div className="flex items-start gap-4 border-b border-border p-4">
        <div className="min-w-0">
          <h2 className="m-0 flex items-center gap-2 text-[17px] font-semibold">
            <StatusPill tone={pill.tone}>{status === 'low' ? 'Expiring' : pill.label}</StatusPill>
            <span className="font-mono text-[14px] text-muted-foreground">{seat.pk}</span>
          </h2>
          <div className="mt-1 text-[12.5px] text-muted-foreground">
            <span className="font-mono">{seat.device_code || seat.device_key}</span> · {seat.metro_code || '—'} ·{' '}
            {seat.escrow_count || 1} {seat.escrow_count === 1 ? 'seat' : 'seats'}
          </div>
        </div>
        <div className="ml-auto flex items-center gap-2">
          <button
            type="button"
            onClick={onDeposit}
            className="inline-flex items-center gap-1.5 rounded-md bg-primary px-3 py-1.5 text-[13px] font-medium text-primary-foreground hover:bg-primary/90"
          >
            <Coins className="h-3.5 w-3.5" /> Deposit USDC
          </button>
          <button
            type="button"
            onClick={onWithdraw}
            disabled={balanceDollars(seat) <= 0}
            className="inline-flex items-center gap-1.5 rounded-md border border-red-500/40 bg-red-500/5 px-3 py-1.5 text-[13px] text-red-400 hover:bg-red-500/10 disabled:opacity-50"
          >
            <Trash2 className="h-3.5 w-3.5" /> Withdraw &amp; cancel
          </button>
          <button
            type="button"
            onClick={onClose}
            className="flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground hover:bg-background hover:text-foreground"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
      </div>

      <div className="flex gap-0 border-b border-border bg-background/40 px-4">
        {([
          ['overview', 'Overview'],
          ['ips', 'IPs & seats'],
          ['activity', 'Activity'],
          ['receipts', 'Receipts'],
          ['cli', 'CLI & tokens'],
        ] as const).map(([k, label]) => (
          <button
            key={k}
            type="button"
            onClick={() => setTab(k)}
            className={`-mb-px border-b-2 px-3 py-2.5 text-[12.5px] transition-colors ${
              tab === k
                ? 'border-primary text-foreground'
                : 'border-transparent text-muted-foreground hover:text-foreground'
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      <div className="grid gap-6 p-5 md:grid-cols-[1.4fr_1fr]">
        {tab === 'overview' && <OverviewTab seat={seat} currentEpoch={currentEpoch} />}
        {tab === 'ips' && <IpsTab seat={seat} />}
        {tab === 'activity' && <ActivityTab seatPk={seat.pk} preview={preview} />}
        {tab === 'receipts' && <ReceiptsTab seatPk={seat.pk} />}
        {tab === 'cli' && <CliTab seat={seat} />}
      </div>

      {status === 'low' && (
        <div className="m-4 mt-0 flex items-start gap-2 rounded-md border border-amber-500/30 bg-amber-500/5 p-3 text-[12.5px] text-amber-300">
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          <div>
            <b className="text-amber-200">Escrow low.</b> Burn rate is <b>${seat.price_per_epoch_dollars}/epoch</b> and the
            balance is <b>${balanceDollars(seat).toFixed(2)}</b> &mdash; shreds stop in{' '}
            <b>{formatRunway(seat)}</b>. Deposit USDC to extend.
          </div>
        </div>
      )}
    </div>
  )
}

function OverviewTab({ seat, currentEpoch }: { seat: ShredClientSeat; currentEpoch: number }) {
  const status = deriveStatus(seat, currentEpoch)
  const pill = statusPillFor(status)
  const epochs = runwayEpochs(seat)

  const rows: KvRow[] = [
    { label: 'Status', value: <StatusPill tone={pill.tone}>{pill.label}</StatusPill> },
    { label: 'Balance', value: <span className="font-mono text-[12.5px]">${balanceDollars(seat).toFixed(2)} USDC</span> },
    {
      label: 'Burn rate',
      value: (
        <span>
          <span className="font-mono text-[12.5px]">${seat.price_per_epoch_dollars} / epoch</span>{' '}
          <span className="text-muted-foreground">
            ({seat.escrow_count || 1} × ${seat.price_per_epoch_dollars})
          </span>
        </span>
      ),
    },
    {
      label: 'Runway',
      value: <span>{formatRunway(seat)}{Number.isFinite(epochs) ? null : ''}</span>,
    },
    { label: 'Funded through', value: <span className="font-mono text-[12.5px]">{formatFundedThrough(seat, currentEpoch)}</span> },
    { label: 'Active epoch', value: <span className="font-mono text-[12.5px]">{seat.active_epoch}</span> },
    { label: 'Funder', value: <span className="font-mono text-[12.5px]">{seat.funding_authority_key}</span> },
    {
      label: 'Tags',
      value: <span className="text-muted-foreground">Coming soon</span>,
    },
  ]

  return (
    <>
      <div className="space-y-6">
        <Section title="Escrow">
          <KvGrid rows={rows} />
        </Section>
        <Section title="Live throughput · last 30 min">
          <div className="rounded-md border border-border bg-background p-4 text-[12.5px] text-muted-foreground">
            Per-seat shred throughput is not yet available.{' '}
            <a href="/dz/shreds/economics" className="text-primary hover:underline">
              View aggregate shred metrics →
            </a>
          </div>
        </Section>
      </div>
      <IpsTab seat={seat} />
    </>
  )
}

function IpsTab({ seat }: { seat: ShredClientSeat }) {
  return (
    <Section title={`IPs & seats · ${seat.client_ip ? '1 active' : '0 active'}`}>
      {seat.client_ip ? (
        <div className="flex items-center gap-3 rounded-md border border-border bg-background px-3 py-2">
          <StatusPill tone="green">receiving</StatusPill>
          <span className="font-mono text-[12.5px]">{seat.client_ip}</span>
          <div className="ml-auto flex items-center gap-2 text-[12px] text-muted-foreground">
            {seat.last_activity ? (
              <span>last shred {shortRelative(seat.last_activity)}</span>
            ) : (
              <span>no traffic yet</span>
            )}
            <CopyButton text={seat.client_ip} />
          </div>
        </div>
      ) : (
        <div className="text-[12.5px] text-muted-foreground">No IP assigned.</div>
      )}
      <p className="mt-2 text-[11.5px] text-muted-foreground">
        Each seat is keyed to one IP. To change the receiving IP, withdraw and re-subscribe.
      </p>
    </Section>
  )
}

function ActivityTab({ seatPk, preview }: { seatPk: string; preview: boolean }) {
  const { data, isLoading, error } = useQuery({
    queryKey: ['shred-escrow-events', 'seat', seatPk],
    queryFn: () =>
      fetchShredEscrowEvents({
        limit: 50,
        sortBy: 'time',
        sortDir: 'desc',
        filters: [`seat:${seatPk}`],
        range: '30d',
      }),
    enabled: !preview,
  })

  if (preview) {
    const events = PREVIEW_EVENTS_BY_SEAT[seatPk] ?? []
    if (events.length === 0) {
      return <div className="col-span-2 text-[12.5px] text-muted-foreground">No activity yet.</div>
    }
    return (
      <div className="col-span-2">
        <Section title="Activity">
          <ul className="m-0 list-none p-0">
            {events.map((ev, i) => <ActivityRow key={`${ev.tx_signature}-${i}`} ev={ev} />)}
          </ul>
        </Section>
      </div>
    )
  }

  if (isLoading) return <div className="col-span-2 text-[12.5px] text-muted-foreground">Loading activity…</div>
  if (error) return <div className="col-span-2 text-[12.5px] text-red-400">Failed to load activity</div>
  const events = data?.items ?? []
  if (events.length === 0) return <div className="col-span-2 text-[12.5px] text-muted-foreground">No activity yet.</div>

  return (
    <div className="col-span-2">
      <Section title="Activity">
        <ul className="m-0 list-none p-0">
          {events.map((ev, i) => <ActivityRow key={`${ev.tx_signature}-${i}`} ev={ev} />)}
        </ul>
      </Section>
    </div>
  )
}

function ActivityRow({ ev }: { ev: ShredEscrowEvent }) {
  const meta = describeEvent(ev)
  return (
    <li className="grid grid-cols-[110px_18px_1fr_auto] items-start gap-3 border-b border-dashed border-border py-2 last:border-b-0">
      <span className="font-mono text-[11.5px] text-muted-foreground">{shortDateTime(ev.event_ts)}</span>
      <span className={`mt-0.5 inline-flex ${meta.tone}`}>{meta.icon}</span>
      <span className="text-[12.5px] text-foreground/90">
        {meta.label}
        {ev.epoch != null && <> · <span className="font-mono">ep {ev.epoch}</span></>}
        {ev.tx_signature && (
          <>
            {' '}
            <a
              href={ev.solscan_url || `https://solscan.io/tx/${ev.tx_signature}`}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-0.5 text-[11.5px] text-muted-foreground hover:text-primary"
            >
              <ExternalLink className="h-3 w-3" />
            </a>
          </>
        )}
      </span>
      <span className={`font-mono text-[12px] ${meta.amountClass}`}>
        {ev.amount_usdc != null && ev.amount_usdc !== 0
          ? (meta.amountSign === '-' ? '−' : '+') + '$' + Math.abs(ev.amount_usdc / 1e6).toFixed(2)
          : ''}
      </span>
    </li>
  )
}

function ReceiptsTab({ seatPk }: { seatPk: string }) {
  return (
    <div className="col-span-2 text-[12.5px] text-muted-foreground">
      Per-epoch receipts are available in the global{' '}
      <a href={`/dz/shreds/activity?filter=seat:${seatPk}`} className="text-primary hover:underline">
        Escrow Events log
      </a>{' '}
      for this seat.
    </div>
  )
}

function CliTab({ seat }: { seat: ShredClientSeat }) {
  const cmd =
    `# run on the receiving server\n` +
    `doublezero subscribe \\\n` +
    `  --device ${seat.device_code || seat.device_key} \\\n` +
    `  --ip ${seat.client_ip || '<your-ip>'} \\\n` +
    `  --token $SHRED_TOKEN`
  return (
    <div className="col-span-2 space-y-3">
      <Section title="CLI handoff">
        <pre className="overflow-x-auto rounded-md border border-border bg-black p-3 font-mono text-[12px] leading-relaxed text-zinc-300">
{cmd}
        </pre>
        <div className="mt-2 flex gap-2">
          <CopyButton text={cmd} label="Copy" />
          <button
            type="button"
            disabled
            title="Token rotation coming soon"
            className="inline-flex items-center gap-1.5 rounded-md border border-border bg-background px-2.5 py-1 text-[12px] opacity-50"
          >
            Rotate token
          </button>
        </div>
      </Section>
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section>
      <h4 className="mb-2 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">{title}</h4>
      {children}
    </section>
  )
}

function CopyButton({ text, label }: { text: string; label?: string }) {
  const [copied, setCopied] = useState(false)
  return (
    <button
      type="button"
      onClick={() => {
        navigator.clipboard.writeText(text).then(() => {
          setCopied(true)
          setTimeout(() => setCopied(false), 1500)
        })
      }}
      className="inline-flex items-center gap-1.5 rounded-md border border-border bg-background px-2.5 py-1 text-[12px] hover:bg-card"
    >
      {copied ? <Check className="h-3 w-3 text-green-400" /> : <Copy className="h-3 w-3" />}
      {label ?? (copied ? 'Copied' : 'Copy')}
    </button>
  )
}

function describeEvent(ev: ShredEscrowEvent): { label: string; icon: React.ReactNode; tone: string; amountSign: '+' | '-' | ''; amountClass: string } {
  switch (ev.event_type) {
    case 'fund':
      return { label: 'Funded escrow', icon: <Check className="h-3.5 w-3.5" />, tone: 'text-green-400', amountSign: '+', amountClass: 'text-green-400' }
    case 'allocate_seat':
    case 'batch_allocate':
    case 'batch_settle':
      return { label: 'Epoch settle', icon: <Clock className="h-3.5 w-3.5" />, tone: 'text-muted-foreground', amountSign: '-', amountClass: 'text-red-400' }
    case 'withdraw_seat':
    case 'ack_withdraw':
      return { label: 'Withdraw', icon: <ArrowDown className="h-3.5 w-3.5" />, tone: 'text-muted-foreground', amountSign: '-', amountClass: 'text-foreground' }
    case 'close':
      return { label: 'Escrow closed', icon: <X className="h-3.5 w-3.5" />, tone: 'text-muted-foreground', amountSign: '', amountClass: 'text-foreground' }
    case 'initialize_seat':
      return { label: 'Subscription created', icon: <Check className="h-3.5 w-3.5" />, tone: 'text-primary', amountSign: '', amountClass: 'text-foreground' }
    case 'initialize_escrow':
      return { label: 'Escrow initialized', icon: <Check className="h-3.5 w-3.5" />, tone: 'text-primary', amountSign: '', amountClass: 'text-foreground' }
    case 'ack_allocate':
      return { label: 'Allocation acknowledged', icon: <Check className="h-3.5 w-3.5" />, tone: 'text-muted-foreground', amountSign: '', amountClass: 'text-foreground' }
    case 'reject_allocate':
      return { label: 'Allocation rejected', icon: <AlertTriangle className="h-3.5 w-3.5" />, tone: 'text-amber-400', amountSign: '', amountClass: 'text-foreground' }
    case 'set_price_override':
      return { label: 'Price override set', icon: <Check className="h-3.5 w-3.5" />, tone: 'text-muted-foreground', amountSign: '', amountClass: 'text-foreground' }
    default:
      return { label: ev.event_type, icon: <Clock className="h-3.5 w-3.5" />, tone: 'text-muted-foreground', amountSign: '', amountClass: 'text-foreground' }
  }
}

function shortDateTime(iso: string): string {
  try {
    const d = new Date(iso)
    return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
  } catch { return iso }
}

function shortRelative(iso: string): string {
  try {
    const ms = Date.now() - new Date(iso).getTime()
    if (ms < 60_000) return `${Math.floor(ms / 1000)}s ago`
    if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m ago`
    if (ms < 86_400_000) return `${Math.floor(ms / 3_600_000)}h ago`
    return `${Math.floor(ms / 86_400_000)}d ago`
  } catch { return '—' }
}
