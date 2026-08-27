import { useMemo, useCallback, useState, useRef, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { AlertTriangle, ChevronDown, ChevronRight, Info, Loader2, Puzzle, RefreshCw } from 'lucide-react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  LabelList,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import {
  fetchShredsEconomics,
  type ShredsEconomics,
  type ShredsEconomicsMetro,
  type ShredsEconomicsMonth,
} from '@/lib/api'
import { useTheme } from '@/hooks/use-theme'
import { cn } from '@/lib/utils'
import { PageHeader } from './page-header'

// Shreds Economics.
//
// Two revenue streams, summed: the per-epoch charge on client seats, and the
// monthly subscription invoices that started in August 2026. They are summed
// and not netted because they are separate charges to separate counterparties.
//
// Seat revenue on this page is accrued, not cash. The API spreads each epoch's
// charge across the calendar days its window covers and cuts at today, so the
// open month holds only its elapsed days and the run-rate below projects the
// rest. See api/handlers/shreds_economics.go for the derivation.

// Series colors. Seats and invoices are the page's only two series, and they
// carry that identity everywhere: chart marks, table figures, tile splits. Each
// theme's steps are picked against that theme's own card surface rather than
// flipped from the other.
//
// Both pairs clear the lightness band, the chroma floor and the colorblind
// separation check, and both are legible as text, not just as marks: measured
// against the card each sits on, light reads 4.55 (seat) and 4.59 (invoice),
// dark 5.85 and 5.24. That last part is what lets a figure wear its series
// color. Identity never rests on color alone either way, since every column is
// also headed by the stream it holds.
const SERIES = {
  light: { seat: '#007db7', invoice: '#c94e0c' },
  dark: { seat: '#0099d5', invoice: '#e15f0a' },
} as const

// Rate-card chips. One step per price point, so a metro's tier reads at a glance
// without a legend. Ordered by price, not by rank in any particular view.
const PRICE_CHIPS: { min: number; className: string }[] = [
  { min: 100, className: 'bg-amber-500/10 text-amber-600 dark:text-amber-400 border-amber-500/25' },
  { min: 60, className: 'bg-violet-500/10 text-violet-600 dark:text-violet-400 border-violet-500/25' },
  { min: 30, className: 'bg-sky-500/10 text-sky-600 dark:text-sky-400 border-sky-500/25' },
  { min: 0, className: 'bg-muted text-muted-foreground border-border' },
]

function priceChipClass(price: number): string {
  return PRICE_CHIPS.find((c) => price >= c.min)?.className ?? PRICE_CHIPS[PRICE_CHIPS.length - 1].className
}

const MONTH_SHORT = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
const MONTH_LONG = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December',
]

// Helpers

function money(n: number, dp = 0): string {
  return n.toLocaleString('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp })
}

function short(n: number): string {
  const a = Math.abs(n)
  if (a >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`
  if (a >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return n.toFixed(0)
}

function axisMoney(n: number): string {
  const a = Math.abs(n)
  if (a >= 1_000_000) return `${(n / 1_000_000).toFixed(a >= 10_000_000 ? 0 : 1)}M`
  if (a >= 1_000) return `${Math.round(n / 1_000)}K`
  return String(Math.round(n))
}

function pct(n: number, of: number): number {
  return of ? (n / of) * 100 : 0
}

// "2026-08" -> "August 2026". Parsed by hand rather than through Date so the
// month is never shifted by the viewer's timezone.
function monthName(key: string): string {
  const [year, month] = key.split('-')
  return `${MONTH_LONG[Number(month) - 1] ?? month} ${year}`
}

function monthShort(key: string, withYear = false): string {
  const [year, month] = key.split('-')
  const name = MONTH_SHORT[Number(month) - 1] ?? key
  return withYear ? `${name} ${year.slice(2)}` : name
}

// Whether the window spans more than one calendar year, in which case a bare
// "Jan" would appear twice.
function spansYears(months: { month: string }[]): boolean {
  return new Set(months.map((m) => m.month.slice(0, 4))).size > 1
}

// "2026-08-26" -> "26 Aug 2026".
function dayLabel(iso: string): string {
  if (!iso) return ''
  const [year, month, day] = iso.split('-')
  return `${Number(day)} ${MONTH_SHORT[Number(month) - 1] ?? month} ${year}`
}

// View model

interface MonthRow extends ShredsEconomicsMonth {
  total: number
  perDay: number
  cumSeat: number
  cumInvoice: number
  cum: number
}

interface EconomicsView {
  months: MonthRow[]
  recognized: MonthRow[]
  openMonth: MonthRow | undefined
  nextMonth: MonthRow | undefined
  periodSeat: number
  periodInvoiced: number
  periodTotal: number
  periodDays: number
  bookedAhead: number
  metros: ShredsEconomicsMetro[]
  metroSeat: number
  metroInvoiced: number
  metroTotal: number
  peakSeats: number
  dailyRate: number
  daysLeft: number
  mrrSeat: number
  mrrInvoice: number
  mrr: number
  arr: number
}

function buildView(data: ShredsEconomics): EconomicsView {
  let runSeat = 0
  let runInvoice = 0
  const months: MonthRow[] = data.months.map((m) => {
    runSeat += m.seat_revenue
    runInvoice += m.invoiced
    return {
      ...m,
      total: m.seat_revenue + m.invoiced,
      perDay: m.days ? m.seat_revenue / m.days : 0,
      cumSeat: runSeat,
      cumInvoice: runInvoice,
      cum: runSeat + runInvoice,
    }
  })

  const recognized = months.filter((m) => !m.future)
  const openMonth = months.find((m) => m.open)
  const nextMonth = months.find((m) => m.future)

  const metroSeat = data.metros.reduce((sum, m) => sum + m.seat_revenue, 0)
  const metroInvoiced = data.metros.reduce((sum, m) => sum + m.invoiced, 0)

  // Run-rate: the open month projected to its end. Seats are charged per epoch,
  // so the days still to run are added at the rate the live seats imply.
  // Invoices are billed monthly and the open month is already whole.
  const dailyRate = data.epoch_days > 0 ? data.live_seat_rate / data.epoch_days : 0
  const daysLeft = openMonth ? Math.max(0, openMonth.days_in_month - openMonth.days) : 0
  const mrrSeat = (openMonth?.seat_revenue ?? 0) + daysLeft * dailyRate
  const mrrInvoice = openMonth?.invoiced ?? 0

  return {
    months,
    recognized,
    openMonth,
    nextMonth,
    periodSeat: recognized.reduce((sum, m) => sum + m.seat_revenue, 0),
    periodInvoiced: recognized.reduce((sum, m) => sum + m.invoiced, 0),
    periodTotal: recognized.reduce((sum, m) => sum + m.total, 0),
    periodDays: recognized.reduce((sum, m) => sum + m.days, 0),
    bookedAhead: months.filter((m) => m.future).reduce((sum, m) => sum + m.total, 0),
    metros: data.metros,
    metroSeat,
    metroInvoiced,
    metroTotal: metroSeat + metroInvoiced,
    peakSeats: data.epochs.reduce((peak, e) => Math.max(peak, e.seats), 0),
    dailyRate,
    daysLeft,
    mrrSeat,
    mrrInvoice,
    mrr: mrrSeat + mrrInvoice,
    arr: (mrrSeat + mrrInvoice) * 12,
  }
}

// Shared pieces

// Shimmer while a fetch is in flight, held back 200ms so a fast response never
// flashes a loading bar. It stays up until the fetch clears rather than running
// for a fixed time, so the bar's length is the wait's length.
function useDebouncedShimmer(isFetching: boolean, delayMs = 200): boolean {
  const [visible, setVisible] = useState(false)
  useEffect(() => {
    if (!isFetching) {
      setVisible(false)
      return
    }
    const t = setTimeout(() => setVisible(true), delayMs)
    return () => clearTimeout(t)
  }, [isFetching, delayMs])
  return visible
}

function useRefreshButton(refetch: () => void, isFetching: boolean, minMs = 400) {
  const [spinning, setSpinning] = useState(false)
  const timer = useRef<ReturnType<typeof setTimeout>>(undefined)
  const onClick = useCallback(() => {
    setSpinning(true)
    refetch()
    clearTimeout(timer.current)
    timer.current = setTimeout(() => setSpinning(false), minMs)
  }, [refetch, minMs])
  useEffect(() => () => clearTimeout(timer.current), [])
  return { spinning: spinning || isFetching, onClick }
}

function Panel({
  title,
  note,
  actions,
  children,
}: {
  title: string
  note: React.ReactNode
  actions?: React.ReactNode
  children: React.ReactNode
}) {
  return (
    <section className="rounded-xl border border-border bg-card overflow-hidden shadow-sm">
      <header className="flex flex-wrap items-start justify-between gap-x-6 gap-y-3 px-5 sm:px-6 py-5 border-b border-border">
        <div className="min-w-0 max-w-2xl">
          <h2 className="text-[15px] font-semibold tracking-tight">{title}</h2>
          <p className="text-xs leading-relaxed text-muted-foreground/80 mt-1.5">{note}</p>
        </div>
        {actions && <div className="flex items-center gap-3 flex-wrap shrink-0">{actions}</div>}
      </header>
      {children}
    </section>
  )
}

function Legend({ seat, invoice, labels }: { seat: string; invoice: string; labels: [string, string] }) {
  return (
    <div className="flex items-center gap-3 text-xs text-muted-foreground">
      <span className="flex items-center gap-1.5">
        <span className="inline-block h-2 w-2 rounded-sm" style={{ background: seat }} />
        {labels[0]}
      </span>
      <span className="flex items-center gap-1.5">
        <span className="inline-block h-2 w-2 rounded-sm" style={{ background: invoice }} />
        {labels[1]}
      </span>
    </div>
  )
}

function Segmented<T extends string>({
  value,
  options,
  onChange,
  label,
}: {
  value: T
  options: { value: T; label: string }[]
  onChange: (v: T) => void
  label: string
}) {
  return (
    <div className="inline-flex rounded-md border border-border overflow-hidden text-xs" role="group" aria-label={label}>
      {options.map((o, i) => (
        <button
          key={o.value}
          type="button"
          onClick={() => onChange(o.value)}
          aria-pressed={value === o.value}
          className={cn(
            'px-2.5 py-1 transition-colors',
            i > 0 && 'border-l border-border',
            value === o.value ? 'bg-accent text-accent-foreground' : 'hover:bg-muted text-muted-foreground',
          )}
        >
          {o.label}
        </button>
      ))}
    </div>
  )
}

function ChartTooltip({ title, sub, rows }: { title: string; sub?: string; rows: React.ReactNode }) {
  return (
    <div className="bg-popover border border-border rounded-lg px-3 py-2 text-xs shadow-xl min-w-44">
      <div className="font-medium mb-1.5">
        {title}
        {sub && <span className="font-normal text-muted-foreground"> {sub}</span>}
      </div>
      <div className="flex flex-col gap-1">{rows}</div>
    </div>
  )
}

function TipRow({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div className="flex items-baseline justify-between gap-6">
      <span className="text-muted-foreground">{label}</span>
      <span className="tabular-nums font-medium" style={color ? { color } : undefined}>
        {value}
      </span>
    </div>
  )
}

// A figure with the arithmetic behind it one click away. The run-rate is a
// projection and the ARR is that projection annualized; neither should be read
// as a settled number without the reader being able to see how it was reached.
let tileSeq = 0

function HeadlineTile({
  label,
  figure,
  unit,
  split,
  progress,
  explain,
}: {
  label: string
  figure: string
  unit: string
  split: { label: string; value: string; color: string }[]
  progress?: { done: number; of: number; label: string }
  explain: { title: string; rows: { label: string; value: string; emphasis?: 'rule' | 'total' }[]; note: string }
}) {
  const [open, setOpen] = useState(false)
  const wrap = useRef<HTMLDivElement>(null)
  const [panelId] = useState(() => `tile-explain-${(tileSeq += 1)}`)

  useEffect(() => {
    if (!open) return
    const onDown = (e: MouseEvent) => {
      if (!wrap.current?.contains(e.target as Node)) setOpen(false)
    }
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', onDown)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onDown)
      document.removeEventListener('keydown', onKey)
    }
  }, [open])

  return (
    <div className="rounded-xl border border-border bg-card px-5 py-5 min-w-0 shadow-sm">
      <div ref={wrap} className="relative flex items-center gap-1.5 mb-3.5">
        <span className="text-[10px] font-semibold text-muted-foreground/70 uppercase tracking-[0.14em]">{label}</span>
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          aria-expanded={open}
          aria-controls={panelId}
          aria-label={`How ${label} is calculated`}
          className="p-0.5 rounded text-muted-foreground/50 hover:text-foreground hover:bg-muted transition-colors"
        >
          <Info className="h-3 w-3" />
        </button>
        {open && (
          <div
            id={panelId}
            role="group"
            aria-label={`How ${label} is calculated`}
            className="absolute left-0 top-6 z-20 w-72 max-w-[calc(100vw-3rem)] rounded-lg border border-border bg-popover p-3 shadow-xl"
          >
            <div className="text-xs font-medium mb-2">{explain.title}</div>
            <div className="flex flex-col gap-1">
              {explain.rows.map((r) => (
                <div
                  key={r.label}
                  className={cn(
                    'flex items-baseline justify-between gap-4 text-xs',
                    r.emphasis === 'rule' && 'border-t border-border pt-1 mt-0.5',
                    r.emphasis === 'total' && 'border-t border-border pt-1 mt-0.5 font-medium',
                  )}
                >
                  <span className="text-muted-foreground">{r.label}</span>
                  <span className="tabular-nums shrink-0">{r.value}</span>
                </div>
              ))}
            </div>
            <p className="text-[11px] text-muted-foreground mt-2.5 leading-relaxed">{explain.note}</p>
          </div>
        )}
      </div>

      <div className="flex items-baseline gap-1.5">
        <span className="text-3xl sm:text-[2rem] font-semibold tabular-nums tracking-tight leading-none">{figure}</span>
        <span className="text-xs text-muted-foreground/70">{unit}</span>
      </div>

      {progress && (
        <div className="mt-3.5 flex items-center gap-2.5">
          <span className="h-1 flex-1 rounded-full bg-muted overflow-hidden">
            <span
              className="block h-full rounded-full bg-muted-foreground/40 transition-[width] duration-700"
              style={{ width: `${pct(progress.done, progress.of)}%` }}
            />
          </span>
          <span className="text-[11px] text-muted-foreground/70 tabular-nums shrink-0">{progress.label}</span>
        </div>
      )}

      <div className="mt-4 pt-3 border-t border-border/70 flex flex-col gap-1.5">
        {split.map((s) => (
          <div key={s.label} className="flex items-baseline justify-between gap-3 text-xs">
            <span className="flex items-center gap-1.5 text-muted-foreground">
              <span className="inline-block h-2 w-2 rounded-sm" style={{ background: s.color }} />
              {s.label}
            </span>
            <span className="tabular-nums font-medium" style={{ color: s.color }}>
              {s.value}
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}

// Revenue by month

function RevenueByMonth({ view, colors }: { view: EconomicsView; colors: { seat: string; invoice: string } }) {
  const [mode, setMode] = useState<'monthly' | 'cumulative'>('monthly')
  const cumulative = mode === 'cumulative'

  const withYear = spansYears(view.months)
  const data = view.months.map((m) => {
    const total = cumulative ? m.cum : m.total
    return {
      key: m.month,
      label: monthShort(m.month, withYear) + (m.open ? ' (open)' : m.future ? ' (booked)' : ''),
      seat: cumulative ? m.cumSeat : m.seat_revenue,
      invoice: cumulative ? m.cumInvoice : m.invoiced,
      total,
      totalLabel: short(total),
      row: m,
    }
  })

  // A month that has not started is dimmed hardest, the open one less so: a
  // solid bar reads as a settled total, and neither of those is one.
  const opacityFor = (m: MonthRow) => (m.future ? 0.42 : m.open ? 0.62 : 1)

  return (
    <Panel
      title="Revenue by month"
      note="Two streams, summed. Seats is the epoch charge on shred client seats; invoices are the monthly subscription billing, which began August 2026."
      actions={
        <>
          <Legend seat={colors.seat} invoice={colors.invoice} labels={['Seats', 'Invoices']} />
          <Segmented
            label="Revenue view"
            value={mode}
            onChange={setMode}
            options={[
              { value: 'monthly', label: 'Monthly' },
              { value: 'cumulative', label: 'Cumulative' },
            ]}
          />
        </>
      }
    >
      <div className="px-2 pt-5 pb-1">
        <ResponsiveContainer width="100%" height={232}>
          <BarChart data={data} barCategoryGap="28%" margin={{ top: 20, right: 16, left: 4, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
            <XAxis
              dataKey="label"
              tickLine={false}
              axisLine={false}
              tick={{ fontSize: 11, fill: 'var(--muted-foreground)' }}
              dy={6}
            />
            <YAxis
              tickLine={false}
              axisLine={false}
              tick={{ fontSize: 11, fill: 'var(--muted-foreground)' }}
              tickFormatter={axisMoney}
              width={52}
            />
            <Tooltip
              cursor={{ fill: 'var(--muted)', opacity: 0.4 }}
              content={({ active, payload }) => {
                if (!active || !payload?.length) return null
                const d = payload[0].payload as (typeof data)[number]
                const m = d.row
                return (
                  <ChartTooltip
                    title={monthName(m.month)}
                    sub={m.open ? '(open)' : m.future ? '(booked ahead)' : undefined}
                    rows={
                      <>
                        <TipRow
                          label={cumulative ? 'Seats to date' : 'Seats'}
                          value={d.seat !== 0 ? `${money(d.seat)} USDC` : 'not charged yet'}
                          color={d.seat < 0 ? 'var(--destructive)' : colors.seat}
                        />
                        <TipRow
                          label={cumulative ? 'Invoices to date' : 'Invoices'}
                          value={d.invoice !== 0 ? `${money(d.invoice)} USDC` : 'not billed yet'}
                          color={colors.invoice}
                        />
                        <TipRow
                          label={cumulative ? 'Total to date' : 'Total revenue'}
                          value={`${money(d.total)} USDC`}
                        />
                        {m.future ? (
                          <TipRow label="Feeds invoiced" value={String(m.invoice_feeds)} />
                        ) : (
                          <>
                            <TipRow label="Days recognized" value={`${m.days} of ${m.days_in_month}`} />
                            <TipRow label="Seat revenue / day" value={`${money(m.perDay)} USDC`} />
                          </>
                        )}
                      </>
                    }
                  />
                )
              }}
            />
            {/* Seats on the bottom, invoices on top, so the whole bar is the
                month's revenue. */}
            <Bar dataKey="seat" stackId="rev" fill={colors.seat}>
              {data.map((d) => (
                <Cell key={d.key} fillOpacity={opacityFor(d.row)} />
              ))}
            </Bar>
            <Bar dataKey="invoice" stackId="rev" fill={colors.invoice} radius={[3, 3, 0, 0]}>
              {data.map((d) => (
                <Cell key={d.key} fillOpacity={opacityFor(d.row)} />
              ))}
              {/* The stack's total, drawn off the top segment. A month with no
                  invoices gives that segment zero height at the top of the seat
                  bar, so the label still lands on top of the stack. */}
              <LabelList
                dataKey="totalLabel"
                position="top"
                offset={8}
                fontSize={10}
                fill="var(--foreground)"
                className="tabular-nums"
              />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="overflow-x-auto border-t border-border">
        <table className="min-w-160 w-full text-sm">
          <thead className="bg-muted/40">
            <tr className="text-[10px] text-muted-foreground border-b border-border">
              <th className="px-4 py-3 text-left font-semibold uppercase tracking-[0.12em]">Month</th>
              <th className="px-4 py-3 text-right font-semibold uppercase tracking-[0.12em]">Days</th>
              <th className="px-4 py-3 text-right font-semibold uppercase tracking-[0.12em]">Seat revenue</th>
              <th className="px-4 py-3 text-right font-semibold uppercase tracking-[0.12em]">Per day</th>
              <th className="px-4 py-3 text-right font-semibold uppercase tracking-[0.12em]">Invoices</th>
              <th className="px-4 py-3 text-right font-semibold uppercase tracking-[0.12em]">Total revenue</th>
              <th
                className="px-4 py-3 text-right font-semibold uppercase tracking-[0.12em]"
                title="Running total of the column to its left, so a month billed ahead carries its booked revenue too. The footer totals recognized months only."
              >
                Cumulative
              </th>
            </tr>
          </thead>
          <tbody>
            {view.months
              .slice()
              .reverse()
              .map((m) => (
                <tr
                  key={m.month}
                  className={cn(
                    'border-b border-border/50 last:border-0 transition-colors hover:bg-muted/30 align-top',
                    m.future && 'opacity-60',
                  )}
                >
                  <td className="px-4 py-3 font-medium whitespace-nowrap">
                    {monthName(m.month)}
                    {m.open && <Tag>open</Tag>}
                    {m.future && <Tag muted>booked</Tag>}
                  </td>
                  <td className="px-4 py-3 text-right tabular-nums">
                    {m.future ? (
                      <Dash />
                    ) : (
                      <>
                        {m.days}
                        {m.open && <span className="text-muted-foreground/60"> of {m.days_in_month}</span>}
                      </>
                    )}
                  </td>
                  <td
                    className="px-4 py-3 text-right tabular-nums"
                    style={{ color: m.seat_revenue < 0 ? 'var(--destructive)' : m.seat_revenue > 0 ? colors.seat : undefined }}
                  >
                    {m.seat_revenue !== 0 ? money(m.seat_revenue) : <Dash />}
                  </td>
                  <td className="px-4 py-3 text-right tabular-nums">{m.perDay !== 0 ? money(m.perDay) : <Dash />}</td>
                  <td className="px-4 py-3 text-right tabular-nums" style={{ color: m.invoiced !== 0 ? colors.invoice : undefined }}>
                    {m.invoiced !== 0 ? (
                      <>
                        {money(m.invoiced)}
                        <span className="block text-[11px] text-muted-foreground/60">{m.invoice_feeds} feeds</span>
                      </>
                    ) : (
                      <Dash />
                    )}
                  </td>
                  <td
                    className="px-4 py-3 text-right tabular-nums font-medium"
                    style={m.total < 0 ? { color: 'var(--destructive)' } : undefined}
                  >
                    {money(m.total)}
                  </td>
                  <td className="px-4 py-3 text-right tabular-nums text-muted-foreground">{money(m.cum)}</td>
                </tr>
              ))}
          </tbody>
          <tfoot>
            <tr className="border-t-2 border-border bg-muted/30 text-sm font-medium">
              <td className="px-4 py-3.5">
                {view.recognized.length} months recognized
                {view.bookedAhead > 0 && (
                  <span className="block text-[11px] font-normal text-muted-foreground/60">
                    {money(view.bookedAhead)} booked ahead, excluded from this row
                  </span>
                )}
              </td>
              <td className="px-4 py-3.5 text-right tabular-nums align-top">{view.periodDays}</td>
              <td className="px-4 py-3.5 text-right tabular-nums align-top" style={{ color: colors.seat }}>
                {money(view.periodSeat)}
              </td>
              <td className="px-4 py-3.5 text-right tabular-nums align-top">
                {view.periodDays > 0 ? money(view.periodSeat / view.periodDays) : <Dash />}
              </td>
              <td className="px-4 py-3.5 text-right tabular-nums align-top" style={{ color: colors.invoice }}>
                {money(view.periodInvoiced)}
              </td>
              <td className="px-4 py-3.5 text-right tabular-nums align-top">{money(view.periodTotal)}</td>
              <td className="px-4 py-3.5 text-right align-top">
                <Dash />
              </td>
            </tr>
          </tfoot>
        </table>
      </div>
    </Panel>
  )
}

// Metros

const METRO_HEAD = 8

// Rows the revenue table opens with: the default five recognized months plus the
// two the subscription program is typically billed ahead into. Only the
// skeleton's height depends on it, so being a month out costs nothing.
const SKELETON_MONTH_ROWS = 7

function Metros({
  data,
  view,
  colors,
}: {
  data: ShredsEconomics
  view: EconomicsView
  colors: { seat: string; invoice: string }
}) {
  const [showAll, setShowAll] = useState(false)
  const [expanded, setExpanded] = useState<string | null>(null)
  const rows = showAll ? view.metros : view.metros.slice(0, METRO_HEAD)

  // Collapsing hides rows, so drop any drawer belonging to one that went away.
  useEffect(() => {
    if (!showAll && expanded && !view.metros.slice(0, METRO_HEAD).some((m) => m.metro === expanded)) {
      setExpanded(null)
    }
  }, [showAll, expanded, view.metros])

  const firstMonth = view.recognized[0]
  const lastMonth = view.recognized[view.recognized.length - 1]
  const span =
    firstMonth && lastMonth
      ? `${monthShort(firstMonth.month)} to ${monthShort(lastMonth.month)}`
      : 'the window'

  return (
    <Panel
      title="Metros"
      note={
        <>
          Both streams. Invoices reach a metro through the feed they bill, so a feed's metro carries its revenue. Live
          is epoch <span className="font-mono">{data.current_epoch}</span>; revenue is {span}, charged per epoch rather
          than spread across days, so it runs slightly ahead of the monthly figures above. Subscription seats do not sum
          to the live total the way payers would: one payer holding seats in three metros counts in all three.
        </>
      }
    >
      <div className="overflow-x-auto">
        <table className="min-w-180 w-full text-sm">
          <thead className="bg-muted/40">
            {/* Two header rows, one band. A group label is centred over the pair
                of columns it covers: right-aligned it sat above the last column
                of its group and read as that column's heading. */}
            <tr className="text-[10px] text-muted-foreground/70">
              <th colSpan={2} />
              <th colSpan={2} className="px-4 pt-3 pb-1 text-center font-semibold uppercase tracking-[0.14em] border-l border-border/70">
                Live, epoch {data.current_epoch}
              </th>
              <th colSpan={3} className="px-4 pt-3 pb-1 text-center font-semibold uppercase tracking-[0.14em] border-l border-border/70">
                Revenue {span}
              </th>
            </tr>
            <tr className="text-[10px] text-muted-foreground border-b border-border">
              <th className="px-4 pb-2.5 pt-1 text-left font-semibold uppercase tracking-[0.12em]">Metro</th>
              <th className="px-4 pb-2.5 pt-1 text-left font-semibold uppercase tracking-[0.12em]">Price</th>
              <th className="px-4 pb-2.5 pt-1 text-right font-semibold uppercase tracking-[0.12em] border-l border-border/70">Seats</th>
              <th className="px-4 pb-2.5 pt-1 text-right font-semibold uppercase tracking-[0.12em]">Subscriptions</th>
              <th className="px-4 pb-2.5 pt-1 text-right font-semibold uppercase tracking-[0.12em] border-l border-border/70">Seats</th>
              <th className="px-4 pb-2.5 pt-1 text-right font-semibold uppercase tracking-[0.12em]">Invoices</th>
              <th className="px-4 pb-2.5 pt-1 text-right font-semibold uppercase tracking-[0.12em]">Total</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((m) => (
              <MetroRow
                key={m.metro}
                metro={m}
                view={view}
                colors={colors}
                epochsPerMonth={data.epochs_per_month}
                open={expanded === m.metro}
                onToggle={() => setExpanded((cur) => (cur === m.metro ? null : m.metro))}
              />
            ))}
            {rows.length === 0 && (
              <tr>
                <td colSpan={7} className="px-4 py-8 text-center text-sm text-muted-foreground">
                  No metro revenue in the window
                </td>
              </tr>
            )}
          </tbody>
          {view.metros.length > 0 && (
            <tfoot>
              <tr className="border-t-2 border-border bg-muted/30 font-medium">
                <td className="px-4 py-3.5">
                  {view.metros.length} metros
                  <span className="block text-[11px] font-normal text-muted-foreground/60">
                    of {data.metros_priced} priced
                  </span>
                </td>
                <td />
                <td className="px-4 py-3.5 text-right tabular-nums border-l border-border/70" style={{ color: colors.seat }}>
                  {data.live_seats}
                  <span className="block text-[11px] font-normal text-muted-foreground/60">
                    {money(data.live_seat_rate)} / epoch
                  </span>
                </td>
                <td className="px-4 py-3.5 text-right tabular-nums align-top" style={{ color: colors.invoice }}>
                  {data.live_subscriptions}
                </td>
                <td className="px-4 py-3.5 text-right tabular-nums align-top border-l border-border/70" style={{ color: colors.seat }}>
                  {short(view.metroSeat)}
                </td>
                <td className="px-4 py-3.5 text-right tabular-nums align-top" style={{ color: colors.invoice }}>
                  {short(view.metroInvoiced)}
                </td>
                <td className="px-4 py-3.5 text-right tabular-nums align-top">{short(view.metroTotal)}</td>
              </tr>
            </tfoot>
          )}
        </table>
      </div>

      {view.metros.length > METRO_HEAD && (
        <div className="border-t border-border">
          <button
            type="button"
            onClick={() => setShowAll((v) => !v)}
            aria-expanded={showAll}
            className="w-full px-4 py-3 flex items-center justify-center gap-1.5 text-xs font-medium text-muted-foreground hover:text-foreground hover:bg-muted/40 transition-colors"
          >
            {showAll ? `Show top ${METRO_HEAD}` : `Show all ${view.metros.length} metros`}
            <ChevronDown className={cn('h-3.5 w-3.5 transition-transform', showAll && 'rotate-180')} />
          </button>
        </div>
      )}
    </Panel>
  )
}

function MetroRow({
  metro,
  view,
  colors,
  epochsPerMonth,
  open,
  onToggle,
}: {
  metro: ShredsEconomicsMetro
  view: EconomicsView
  colors: { seat: string; invoice: string }
  epochsPerMonth: number
  open: boolean
  onToggle: () => void
}) {
  const total = metro.seat_revenue + metro.invoiced
  const rate = metro.price * metro.live_seats
  // Share of all metro revenue: the same quantity the figure beside the bar
  // reports, so the two cannot disagree. A refunded metro nets out below zero;
  // a negative width is not a valid declaration and a dropped one renders full
  // width, so it is floored here and the figure is labelled instead.
  const share = Math.max(0, pct(total, view.metroTotal))
  const seatShare = total > 0 ? Math.max(0, pct(metro.seat_revenue, total)) : 0
  const invoiceShare = total > 0 ? Math.max(0, pct(metro.invoiced, total)) : 0

  return (
    <>
      <tr
        onClick={onToggle}
        onKeyDown={(e) => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault()
            onToggle()
          }
        }}
        tabIndex={0}
        role="button"
        aria-expanded={open}
        className={cn(
          'border-b border-border/50 cursor-pointer transition-colors align-top',
          'hover:bg-muted/40 focus:outline-none focus-visible:bg-muted/50',
          open && 'bg-muted/40',
          metro.live_seats === 0 && metro.subscriptions === 0 && 'text-muted-foreground',
        )}
      >
        <td className="px-4 py-3 whitespace-nowrap">
          {/* Flex, not an inline icon: an inline SVG sits on the baseline and
              inflates the line box, dropping the code below the numeric columns. */}
          <span className="flex items-center gap-1.5 font-mono text-sm font-medium uppercase tracking-wide">
            <ChevronRight
              className={cn(
                'h-3 w-3 shrink-0 text-muted-foreground/50 transition-transform duration-200',
                open && 'rotate-90 text-muted-foreground',
              )}
            />
            {metro.metro}
          </span>
        </td>
        <td className="px-4 py-3 whitespace-nowrap">
          <span
            className={cn(
              'inline-flex items-center px-2 py-0.5 rounded-full border text-xs font-medium tabular-nums',
              priceChipClass(metro.price),
            )}
          >
            {money(metro.price)}
          </span>
          <span className="block text-[11px] text-muted-foreground/60 tabular-nums mt-0.5">
            {money(metro.price * epochsPerMonth)} / month
          </span>
        </td>
        <td className="px-4 py-3 text-right tabular-nums border-l border-border/70" style={{ color: metro.live_seats > 0 ? colors.seat : undefined }}>
          {metro.live_seats > 0 ? metro.live_seats : <Dash />}
        </td>
        <td className="px-4 py-3 text-right tabular-nums" style={{ color: metro.subscriptions > 0 ? colors.invoice : undefined }}>
          {metro.subscriptions > 0 ? metro.subscriptions : <Dash />}
        </td>
        <td
          className="px-4 py-3 text-right tabular-nums border-l border-border/70"
          style={{ color: metro.seat_revenue < 0 ? 'var(--destructive)' : colors.seat }}
        >
          {short(metro.seat_revenue)}
        </td>
        <td className="px-4 py-3 text-right tabular-nums" style={{ color: metro.invoiced > 0 ? colors.invoice : undefined }}>
          {metro.invoiced > 0 ? short(metro.invoiced) : <Dash />}
        </td>
        <td className="px-4 py-3 text-right">
          <span className="tabular-nums font-medium">{short(total)}</span>
          <span className="mt-1.5 flex items-center justify-end gap-2">
            <span className="text-[11px] text-muted-foreground/60 tabular-nums">
              {total < 0 ? 'refunded' : `${pct(total, view.metroTotal).toFixed(0)}%`}
            </span>
            <span className="h-1 w-16 shrink-0 rounded-full bg-muted overflow-hidden">
              {/* A sliver still reads as present; rounding it to nothing would
                  make a small metro look like it earned none at all. */}
              <span
                className="block h-full rounded-full transition-[width] duration-700"
                style={{ width: share > 0 ? `max(2px, ${share}%)` : 0, background: colors.seat }}
              />
            </span>
          </span>
        </td>
      </tr>
      <tr className="border-b border-border/50">
        <td colSpan={7} className="p-0">
          {/* A fixed cap, not a measured height: inside a table cell the
              grid-rows and scrollHeight approaches both collapse to zero. The
              drawer measures 66px wide open and 112px at the table's 720px
              minimum, where the stats wrap to a second row, so 192px clears its
              tallest state with room for another line. */}
          <div
            className={cn(
              'overflow-hidden transition-[max-height] duration-300 ease-out',
              open ? 'max-h-48' : 'max-h-0',
            )}
          >
            <div className="px-4 py-4 bg-muted/20 flex flex-wrap items-start gap-x-10 gap-y-4">
              <DrawerStat label="Devices" value={String(metro.devices)} sub={metro.devices === 1 ? 'device' : 'devices'} />
              <DrawerStat
                label="Live seats"
                value={String(metro.live_seats)}
                sub={rate > 0 ? `${money(rate)} / epoch` : 'none charged'}
              />
              <DrawerStat
                label="Rate"
                value={rate > 0 ? money(rate * epochsPerMonth) : '0'}
                sub="per month"
              />
              <DrawerStat
            label="Subscription seats"
            value={String(metro.subscriptions)}
            sub={metro.subscriptions > 0 ? undefined : 'none'}
          />
              <div className="flex-1 min-w-56">
                <div className="flex h-1.5 gap-0.5 rounded-full overflow-hidden bg-muted mb-2">
                  <span style={{ width: `${seatShare}%`, background: colors.seat }} />
                  <span style={{ width: `${invoiceShare}%`, background: colors.invoice }} />
                </div>
                <div className="flex flex-wrap gap-x-5 gap-y-1 text-[11px] tabular-nums">
                  <span style={{ color: colors.seat }}>
                    Seats <span className="font-medium">{money(metro.seat_revenue)}</span>
                  </span>
                  <span style={{ color: colors.invoice }}>
                    Invoices <span className="font-medium">{money(metro.invoiced)}</span>
                  </span>
                  <span className="text-muted-foreground">
                    Total <span className="font-medium text-foreground">{money(total)}</span>
                  </span>
                </div>
              </div>
            </div>
          </div>
        </td>
      </tr>
    </>
  )
}

function DrawerStat({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="flex flex-col">
      <span className="text-[10px] text-muted-foreground/60 uppercase tracking-widest">{label}</span>
      <span className="text-sm tabular-nums font-medium">
        {value}
        {sub && <span className="ml-1.5 text-[11px] font-normal text-muted-foreground/60">{sub}</span>}
      </span>
    </div>
  )
}

// Small bits

function Dash() {
  return <span className="text-muted-foreground/40">&ndash;</span>
}

function Tag({ children, muted }: { children: React.ReactNode; muted?: boolean }) {
  return (
    <span
      className={cn(
        'ml-2 inline-flex items-center px-1.5 py-px rounded text-[10px] font-medium uppercase tracking-wider border',
        muted ? 'border-border text-muted-foreground/70' : 'border-border text-muted-foreground',
      )}
    >
      {children}
    </span>
  )
}

function Bone({ className = '', style }: { className?: string; style?: React.CSSProperties }) {
  return <div className={cn('rounded bg-muted animate-pulse', className)} style={style} />
}

// A panel outline with its header and a body of the given height, so the shape
// on screen during the wait is the shape that arrives after it.
function PanelSkeleton({ children }: { children: React.ReactNode }) {
  return (
    <section className="rounded-xl border border-border bg-card overflow-hidden shadow-sm">
      <header className="px-5 sm:px-6 py-5 border-b border-border flex items-start justify-between gap-6">
        <div className="flex flex-col gap-2 w-full max-w-md">
          <Bone className="h-4 w-40" />
          <Bone className="h-3 w-full max-w-sm" />
        </div>
        <Bone className="h-6 w-36 shrink-0" />
      </header>
      {children}
    </section>
  )
}

// twoLine matches the metro rows, whose Metro and Price cells carry a sub-line
// and so stand about half again as tall as the revenue table's.
function TableRowsSkeleton({ rows, twoLine = false }: { rows: number; twoLine?: boolean }) {
  return (
    <div className="divide-y divide-border/50">
      {Array.from({ length: rows }, (_, i) => (
        <div key={i} className={cn('flex items-start gap-4 px-4', twoLine ? 'py-3.5' : 'py-3.5')}>
          <div className="flex flex-col gap-1.5 w-24">
            <Bone className="h-3.5 w-full" />
            {twoLine && <Bone className="h-2.5 w-3/4" />}
          </div>
          <div className="flex flex-col gap-1.5 w-20">
            <Bone className="h-3.5 w-full" />
            {twoLine && <Bone className="h-2.5 w-2/3" />}
          </div>
          <Bone className="h-3.5 flex-1" />
          <Bone className="h-3.5 w-16" />
          <Bone className="h-3.5 w-16" />
        </div>
      ))}
    </div>
  )
}

function PageSkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        {[0, 1, 2].map((i) => (
          <div key={i} className="rounded-xl border border-border bg-card shadow-sm px-5 py-5 flex flex-col gap-3.5">
            <Bone className="h-2.5 w-28" />
            <Bone className="h-8 w-40" />
            <Bone className="h-1 w-full" />
            <div className="pt-3 border-t border-border/70 flex flex-col gap-2">
              <Bone className="h-3 w-full" />
              <Bone className="h-3 w-2/3" />
            </div>
          </div>
        ))}
      </div>

      <PanelSkeleton>
        <div className="px-5 sm:px-6 pt-6 pb-2">
          {/* Uneven heights, so the wait reads as a chart rather than a slab.
              The proportions are arbitrary and deliberately not derived from any
              real month: nothing is known yet when this renders. */}
          <div className="flex items-end gap-6 h-56">
            {[62, 96, 78, 74, 70, 44, 18].map((h, i) => (
              <div key={i} className="flex-1 flex items-end h-full">
                <Bone className="w-full" style={{ height: `${h}%` }} />
              </div>
            ))}
          </div>
        </div>
        <div className="border-t border-border">
          <TableRowsSkeleton rows={SKELETON_MONTH_ROWS} />
        </div>
      </PanelSkeleton>

      <PanelSkeleton>
        <TableRowsSkeleton rows={METRO_HEAD} twoLine />
        <div className="border-t border-border px-4 py-3">
          <Bone className="h-3 w-40 mx-auto" />
        </div>
      </PanelSkeleton>
    </div>
  )
}

// Page

export function ShredsEconomicsPage() {
  const { resolvedTheme } = useTheme()
  const colors = resolvedTheme === 'dark' ? SERIES.dark : SERIES.light

  const { data, isFetching, error, refetch } = useQuery({
    queryKey: ['shreds-economics'],
    queryFn: () => fetchShredsEconomics(),
    refetchInterval: 60_000,
  })

  const refresh = useRefreshButton(refetch, isFetching)
  const shimmer = useDebouncedShimmer(isFetching)
  const view = useMemo(() => (data ? buildView(data) : null), [data])

  if (error && !data) {
    return (
      <div className="flex flex-col items-center justify-center py-24 text-center">
        <AlertTriangle className="h-8 w-8 text-muted-foreground mb-3" />
        <p className="text-sm text-muted-foreground">Failed to load economics data.</p>
      </div>
    )
  }

  const openMonth = view?.openMonth
  const lastEpochDay = data?.epochs[data.epochs.length - 1]?.day
  // The first load has its own empty state above; this covers a poll that fails
  // afterwards, which would otherwise leave stale figures reading as current.
  const stale = Boolean(error && data)

  return (
    <div className="flex-1 overflow-y-auto overflow-x-hidden">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Puzzle}
          title="Shreds Economics"
          subtitle={
            <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
              <span>Revenue by month and metro</span>
              {data ? (
                <>
                  <span className="text-muted-foreground/40">&middot;</span>
                  <span>
                    epoch {data.current_epoch} in flight
                    {lastEpochDay && <> since {dayLabel(lastEpochDay)}</>}
                  </span>
                  <span className="text-muted-foreground/40">&middot;</span>
                  <span>recognized through {dayLabel(data.as_of)} UTC</span>
                </>
              ) : (
                <span className="text-muted-foreground/60">loading the current epoch and window</span>
              )}
            </div>
          }
          actions={
            <>
              {stale && (
                <span
                  className="flex items-center gap-1.5 text-xs text-amber-600 dark:text-amber-400"
                  title="The last refresh failed. These figures are the most recent that loaded."
                >
                  <AlertTriangle className="h-3.5 w-3.5" />
                  Showing last loaded figures
                </span>
              )}
              <button
                onClick={refresh.onClick}
                className="p-1.5 rounded text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
                title="Refresh"
              >
                {refresh.spinning ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
              </button>
            </>
          }
        />

        {/* Loading shimmer */}
        <div className="h-0.5 w-full overflow-hidden rounded-full mb-4">
          {shimmer && (
            <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
          )}
        </div>

        {!data || !view ? (
          <PageSkeleton />
        ) : (
          <div className="space-y-6">
            <div className={cn('grid grid-cols-1 gap-3', openMonth ? 'sm:grid-cols-3' : 'sm:grid-cols-2')}>
              {openMonth && (
                <HeadlineTile
                  label={`${monthName(openMonth.month)} revenue`}
                  figure={money(openMonth.total)}
                  unit="USDC"
                  progress={{
                    done: openMonth.days,
                    of: openMonth.days_in_month,
                    label: `${openMonth.days} of ${openMonth.days_in_month} days`,
                  }}
                  split={[
                    { label: 'Seats', value: money(openMonth.seat_revenue), color: colors.seat },
                    { label: 'Invoices', value: money(openMonth.invoiced), color: colors.invoice },
                  ]}
                  explain={{
                    title: `${monthName(openMonth.month)} to date`,
                    rows: [
                      {
                        label: `Seats, ${openMonth.days} of ${openMonth.days_in_month} days`,
                        value: money(openMonth.seat_revenue, 2),
                      },
                      {
                        label: `Invoices, ${openMonth.invoice_feeds} feeds`,
                        value: `+ ${money(openMonth.invoiced, 2)}`,
                      },
                      { label: 'Total', value: money(openMonth.total, 2), emphasis: 'total' },
                    ],
                    note: data.subscriptions_opened_on
                      ? `Subscriptions opened ${dayLabel(data.subscriptions_opened_on)}, covering ${data.live_subscriptions} seats held by ${data.live_subscription_payers} payers. Every earlier month is seats only.`
                      : 'No subscription has been sold yet, so every month here is seats only.',
                  }}
                />
              )}

              <HeadlineTile
                label="Run-rate MRR"
                figure={money(view.mrr)}
                unit="USDC"
                split={[
                  { label: 'Seats', value: money(view.mrrSeat), color: colors.seat },
                  { label: 'Invoices', value: money(view.mrrInvoice), color: colors.invoice },
                ]}
                explain={{
                  title: 'How run-rate is calculated',
                  rows: [
                    { label: `Seats earned, ${openMonth?.days ?? 0} days`, value: money(openMonth?.seat_revenue ?? 0, 2) },
                    {
                      label: `${view.daysLeft} days left at ${money(view.dailyRate, 2)} a day`,
                      value: `+ ${money(view.daysLeft * view.dailyRate, 2)}`,
                    },
                    { label: 'Seats, projected to month end', value: money(view.mrrSeat, 2), emphasis: 'rule' },
                    { label: 'Invoices, billed in full', value: `+ ${money(view.mrrInvoice, 2)}` },
                    { label: 'Run-rate MRR', value: money(view.mrr, 2), emphasis: 'total' },
                  ],
                  note: `The daily rate is what the ${data.live_seats} live seats are charged: ${money(
                    data.live_seat_rate,
                  )} an epoch over ${data.epoch_days.toFixed(2)} days.${
                    view.nextMonth && view.nextMonth.invoiced > 0
                      ? ` It understates the invoice side: ${monthName(view.nextMonth.month)} is already booked at ${money(
                          view.nextMonth.invoiced,
                        )}.`
                      : ''
                  }`,
                }}
              />

              <HeadlineTile
                label="ARR"
                figure={short(view.arr)}
                unit="USDC"
                split={[
                  { label: 'Seats', value: short(view.mrrSeat * 12), color: colors.seat },
                  { label: 'Invoices', value: short(view.mrrInvoice * 12), color: colors.invoice },
                ]}
                explain={{
                  title: 'How ARR is calculated',
                  rows: [
                    { label: 'Run-rate MRR', value: money(view.mrr, 2) },
                    { label: 'Months', value: '× 12' },
                    { label: 'ARR', value: money(view.arr), emphasis: 'total' },
                  ],
                  note: `Run-rate MRR annualized. The seat base ran at ${view.peakSeats} seats at its peak in the window against ${data.live_seats} today, while subscription seats went from nothing to ${data.live_subscriptions}.`,
                }}
              />
            </div>

            <RevenueByMonth view={view} colors={colors} />
            <Metros data={data} view={view} colors={colors} />
          </div>
        )}
      </div>
    </div>
  )
}
