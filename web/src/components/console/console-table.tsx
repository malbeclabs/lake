import { MoreHorizontal } from 'lucide-react'
import type { ShredClientSeat } from '@/lib/api'
import { StatusPill } from './primitives/status-pill'
import { RunwayBar } from './primitives/runway-bar'
import {
  balanceDollars,
  barState,
  deriveStatus,
  formatFundedThrough,
  formatRunwayCaption,
  runwayBarPct,
  statusPillFor,
} from './subscription-status'

interface ConsoleTableProps {
  seats: ShredClientSeat[]
  currentEpoch: number
  selected: Set<string>
  onToggle: (pk: string) => void
  onToggleAll: () => void
  onOpenRow: (seat: ShredClientSeat) => void
  onRowAction: (seat: ShredClientSeat, action: 'deposit' | 'withdraw') => void
}

export function ConsoleTable({
  seats, currentEpoch, selected, onToggle, onToggleAll, onOpenRow, onRowAction,
}: ConsoleTableProps) {
  const allSelected = seats.length > 0 && seats.every((s) => selected.has(s.pk))
  const indeterminate = !allSelected && seats.some((s) => selected.has(s.pk))

  return (
    <div className="overflow-x-auto rounded-b-xl border border-t-0 border-border bg-card">
      <table className="w-full text-[13px]">
        <thead>
          <tr className="border-b border-border bg-background/50">
            <ThCheckbox checked={allSelected} indeterminate={indeterminate} onClick={onToggleAll} />
            <Th>Subscription</Th>
            <Th>Status</Th>
            <Th>Device · metro</Th>
            <Th>Seats / IPs</Th>
            <Th className="text-right">Burn rate</Th>
            <Th>Escrow · runway</Th>
            <Th>Funded through</Th>
            <Th className="w-10" />
          </tr>
        </thead>
        <tbody>
          {seats.map((seat) => {
            const status = deriveStatus(seat, currentEpoch)
            const pill = statusPillFor(status)
            const isSelected = selected.has(seat.pk)
            return (
              <tr
                key={seat.pk}
                className={`border-b border-border last:border-b-0 cursor-pointer transition-colors ${
                  isSelected ? 'bg-primary/10 hover:bg-primary/15' : 'hover:bg-background/40'
                }`}
                onClick={() => onOpenRow(seat)}
              >
                <TdCheckbox checked={isSelected} onClick={(e) => { e.stopPropagation(); onToggle(seat.pk) }} />
                <Td>
                  <div className="flex flex-col">
                    <span className="font-mono text-[12px]">{truncate(seat.pk, 10)}</span>
                    <span className="font-mono text-[11.5px] text-muted-foreground">
                      {seat.last_activity ? `last activity ${shortDate(seat.last_activity)}` : '—'}
                    </span>
                  </div>
                </Td>
                <Td>
                  <StatusPill tone={pill.tone}>{pill.label}</StatusPill>
                </Td>
                <Td>
                  <div className="flex flex-col">
                    <span className="font-mono text-[12.5px]">{seat.device_code || truncate(seat.device_key, 12)}</span>
                    <span className="text-[11.5px] text-muted-foreground">{seat.metro_code || '—'}</span>
                  </div>
                </Td>
                <Td>
                  <div className="flex flex-col">
                    <span>{seat.escrow_count || 1} {seat.escrow_count === 1 ? 'seat' : 'seats'}</span>
                    <span className="font-mono text-[11.5px] text-muted-foreground">{seat.client_ip || '—'}</span>
                  </div>
                </Td>
                <Td className="text-right">
                  <div className="flex flex-col items-end">
                    <span className="font-mono text-[12.5px]">${seat.price_per_epoch_dollars}</span>
                    <span className="text-[11.5px] text-muted-foreground">/ epoch</span>
                  </div>
                </Td>
                <Td>
                  <RunwayBar pct={runwayBarPct(seat)} state={barState(seat)} caption={formatRunwayCaption(seat, status)} />
                </Td>
                <Td>
                  <span className="text-[12.5px]">{formatFundedThrough(seat, currentEpoch)}</span>
                </Td>
                <Td className="w-10">
                  <RowMenu onAction={(a) => onRowAction(seat, a)} disabled={balanceDollars(seat) <= 0 && status === 'expired'} />
                </Td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function Th({ children, className = '' }: { children?: React.ReactNode; className?: string }) {
  return (
    <th className={`px-3 py-2.5 text-left text-[11.5px] font-medium uppercase tracking-wider text-muted-foreground ${className}`}>
      {children}
    </th>
  )
}

function Td({ children, className = '' }: { children?: React.ReactNode; className?: string }) {
  return <td className={`px-3 py-2.5 align-middle tabular-nums ${className}`}>{children}</td>
}

function ThCheckbox({
  checked, indeterminate, onClick,
}: { checked: boolean; indeterminate: boolean; onClick: () => void }) {
  return (
    <th className="w-10 px-3 py-2.5">
      <Checkbox checked={checked} indeterminate={indeterminate} onClick={(e) => { e.stopPropagation(); onClick() }} />
    </th>
  )
}

function TdCheckbox({
  checked, onClick,
}: { checked: boolean; onClick: (e: React.MouseEvent) => void }) {
  return (
    <td className="w-10 px-3 py-2.5">
      <Checkbox checked={checked} onClick={onClick} />
    </td>
  )
}

function Checkbox({
  checked, indeterminate = false, onClick,
}: { checked: boolean; indeterminate?: boolean; onClick: (e: React.MouseEvent) => void }) {
  const on = checked || indeterminate
  return (
    <button
      type="button"
      onClick={onClick}
      aria-checked={checked}
      role="checkbox"
      className={`inline-flex h-3.5 w-3.5 items-center justify-center rounded-sm border ${
        on ? 'border-primary bg-primary' : 'border-muted-foreground/40 bg-background'
      }`}
    >
      {indeterminate ? (
        <span className="h-[2px] w-2 bg-background" />
      ) : checked ? (
        <span className="-mt-0.5 h-[8px] w-[4px] rotate-45 border-b-2 border-r-2 border-background" />
      ) : null}
    </button>
  )
}

function RowMenu({ onAction, disabled }: { onAction: (a: 'deposit' | 'withdraw') => void; disabled: boolean }) {
  return (
    <div className="relative inline-flex" onClick={(e) => e.stopPropagation()}>
      <details className="group">
        <summary className="flex h-7 w-7 cursor-pointer list-none items-center justify-center rounded-md text-muted-foreground hover:bg-background hover:text-foreground [&::-webkit-details-marker]:hidden">
          <MoreHorizontal className="h-3.5 w-3.5" />
        </summary>
        <div className="absolute right-0 top-8 z-20 w-44 rounded-md border border-border bg-card py-1 shadow-lg">
          <button
            type="button"
            onClick={() => onAction('deposit')}
            className="block w-full px-3 py-1.5 text-left text-[12.5px] hover:bg-background"
          >
            Deposit USDC…
          </button>
          <button
            type="button"
            onClick={() => onAction('withdraw')}
            disabled={disabled}
            className="block w-full px-3 py-1.5 text-left text-[12.5px] text-red-400 hover:bg-background disabled:opacity-50"
          >
            Withdraw &amp; cancel
          </button>
        </div>
      </details>
    </div>
  )
}

function truncate(s: string, n: number): string {
  if (!s) return ''
  return s.length <= n ? s : `${s.slice(0, n - 4)}…${s.slice(-3)}`
}

function shortDate(iso: string): string {
  try {
    const d = new Date(iso)
    return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
  } catch { return '—' }
}
