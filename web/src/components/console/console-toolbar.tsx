import { Search, ChevronDown, X, Filter } from 'lucide-react'
import type { SeatStatus } from './subscription-status'

export interface ConsoleFilters {
  query: string
  status: SeatStatus | 'all'
  metro: string | 'all'
  lowEscrowOnly: boolean
}

interface ConsoleToolbarProps {
  filters: ConsoleFilters
  metros: string[]
  onChange: (next: ConsoleFilters) => void
}

export function ConsoleToolbar({ filters, metros, onChange }: ConsoleToolbarProps) {
  return (
    <div className="flex flex-wrap items-center gap-2 rounded-t-xl border border-b-0 border-border bg-card p-2 pl-3">
      <div className="flex min-w-[260px] flex-1 items-center gap-1.5 rounded-md border border-border bg-background px-2.5 py-1 text-[12.5px] text-muted-foreground">
        <Search className="h-3.5 w-3.5" />
        <input
          value={filters.query}
          onChange={(e) => onChange({ ...filters, query: e.target.value })}
          placeholder="Filter by ID, device, IP…"
          className="w-full bg-transparent text-foreground outline-none placeholder:text-muted-foreground"
        />
      </div>

      <SelectChip
        label="Status"
        value={filters.status === 'all' ? 'All' : labelForStatus(filters.status)}
        options={[
          { value: 'all',    label: 'All' },
          { value: 'active', label: 'Active' },
          { value: 'low',    label: 'Low escrow' },
          { value: 'pending',label: 'Pending' },
          { value: 'expired',label: 'Expired' },
        ]}
        onChange={(v) => onChange({ ...filters, status: v as SeatStatus | 'all' })}
      />

      <SelectChip
        label="Metro"
        value={filters.metro === 'all' ? 'All' : filters.metro}
        options={[{ value: 'all', label: 'All' }, ...metros.map((m) => ({ value: m, label: m }))]}
        onChange={(v) => onChange({ ...filters, metro: v })}
      />

      <button
        type="button"
        onClick={() => onChange({ ...filters, lowEscrowOnly: !filters.lowEscrowOnly })}
        className={`inline-flex items-center gap-1.5 rounded-md border px-2.5 py-1 text-[12.5px] transition-colors ${
          filters.lowEscrowOnly
            ? 'border-primary bg-primary/10 text-primary'
            : 'border-border bg-background text-foreground hover:border-muted-foreground/40'
        }`}
      >
        <Filter className="h-3 w-3" /> Escrow &lt; 1 epoch
        {filters.lowEscrowOnly ? <X className="h-3 w-3" /> : null}
      </button>
    </div>
  )
}

function labelForStatus(s: SeatStatus): string {
  switch (s) {
    case 'active': return 'Active'
    case 'low': return 'Low escrow'
    case 'pending': return 'Pending'
    case 'expired': return 'Expired'
  }
}

interface SelectChipProps {
  label: string
  value: string
  options: { value: string; label: string }[]
  onChange: (value: string) => void
}

function SelectChip({ label, value, options, onChange }: SelectChipProps) {
  return (
    <label className="relative inline-flex items-center gap-1.5 rounded-md border border-border bg-background px-2.5 py-1 text-[12.5px] text-muted-foreground hover:border-muted-foreground/40">
      <span>{label}</span>
      <span className="text-foreground">{value}</span>
      <ChevronDown className="h-3 w-3" />
      <select
        value={value === 'All' ? 'all' : options.find((o) => o.label === value)?.value ?? value}
        onChange={(e) => onChange(e.target.value)}
        className="absolute inset-0 cursor-pointer opacity-0"
      >
        {options.map((o) => (
          <option key={o.value} value={o.value}>{o.label}</option>
        ))}
      </select>
    </label>
  )
}
