import { useState, useEffect, useRef } from 'react'
import {
  Server,
  ChevronDown,
  ChevronUp,
  RefreshCw,
  CheckCircle2,
  Plus,
  Trash2,
  Pencil,
  AlertTriangle,
  AlertOctagon,
  GitCommit,
  Wifi,
  WifiOff,
  Link2,
  MapPin,
  Building2,
  Users,
  Landmark,
  Radio,
  Calendar,
  Search,
  RotateCw,
  Check,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import type { TimeRange, ActionFilter } from '@/lib/api'
import {
  type Category,
  type EntityType,
  type DZFilter,
  type MinStakeOption,
  ALL_ACTIONS,
  ALL_DZ_ENTITIES,
  ALL_SOLANA_ENTITIES,
  ALL_CATEGORIES,
  DEFAULT_ENTITY_TYPES,
  presets,
} from './timeline-constants'

const minStakeOptions: { value: MinStakeOption; label: string }[] = [
  { value: '0', label: 'Any' },
  { value: '0.01', label: '>0.01%' },
  { value: '0.05', label: '>0.05%' },
  { value: '0.1', label: '>0.1%' },
  { value: '0.5', label: '>0.5%' },
  { value: '1', label: '>1%' },
  { value: '1.5', label: '>1.5%' },
  { value: '2', label: '>2%' },
]

const timeRangeOptions: { value: TimeRange | 'custom'; label: string }[] = [
  { value: '1h', label: '1h' },
  { value: '6h', label: '6h' },
  { value: '12h', label: '12h' },
  { value: '24h', label: '24h' },
  { value: '3d', label: '3d' },
  { value: '7d', label: '7d' },
  { value: 'custom', label: 'Custom' },
]

const actionOptions: { value: ActionFilter; label: string; icon: typeof Plus }[] = [
  { value: 'added', label: 'Added', icon: Plus },
  { value: 'removed', label: 'Removed', icon: Trash2 },
  { value: 'changed', label: 'Changed', icon: Pencil },
  { value: 'alerting', label: 'Alerting', icon: AlertTriangle },
  { value: 'resolved', label: 'Resolved', icon: CheckCircle2 },
]

const categoryOptions: { value: Category; label: string; icon: typeof Server }[] = [
  { value: 'state_change', label: 'State Changes', icon: GitCommit },
  { value: 'packet_loss', label: 'Packet Loss', icon: Wifi },
  { value: 'interface_carrier', label: 'Carrier Transitions', icon: WifiOff },
  { value: 'interface_errors', label: 'Errors', icon: AlertOctagon },
  { value: 'interface_discards', label: 'Discards', icon: AlertTriangle },
]

const dzEntityOptions: { value: EntityType; label: string; icon: typeof Server }[] = [
  { value: 'device', label: 'Devices', icon: Server },
  { value: 'link', label: 'Links', icon: Link2 },
  { value: 'metro', label: 'Metros', icon: MapPin },
  { value: 'contributor', label: 'Contributors', icon: Building2 },
  { value: 'user', label: 'Users', icon: Users },
]

const solanaEntityOptions: { value: EntityType; label: string; icon: typeof Server }[] = [
  { value: 'validator', label: 'Validators', icon: Landmark },
  { value: 'gossip_node', label: 'Gossip Nodes', icon: Radio },
]

const dzFilterOptions: { value: DZFilter; label: string }[] = [
  { value: 'on_dz', label: 'On DZ' },
  { value: 'off_dz', label: 'Off DZ' },
  { value: 'all', label: 'All' },
]

// Compact dropdown for multi-select filters
function FilterDropdown<T extends string>({
  label,
  options,
  selected,
  onToggle,
  allValues,
}: {
  label: string
  options: { value: T; label: string; icon?: typeof Server }[]
  selected: Set<T>
  onToggle: (value: T) => void
  allValues: T[]
}) {
  const [open, setOpen] = useState(false)
  const dropdownRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    if (open) {
      document.addEventListener('mousedown', handleClickOutside)
      return () => document.removeEventListener('mousedown', handleClickOutside)
    }
  }, [open])

  const allSelected = allValues.every(v => selected.has(v))
  const noneSelected = allValues.every(v => !selected.has(v))
  const selectedCount = allValues.filter(v => selected.has(v)).length

  return (
    <div className="relative" ref={dropdownRef}>
      <button
        onClick={() => setOpen(!open)}
        className={cn(
          'flex items-center gap-1.5 px-2 py-1 text-xs rounded-md border transition-colors',
          open || (!allSelected && !noneSelected)
            ? 'bg-background border-border text-foreground'
            : 'border-transparent text-muted-foreground hover:text-foreground hover:bg-muted/50'
        )}
      >
        <span className="uppercase tracking-wide">{label}</span>
        {!allSelected && (
          <span className="bg-primary/10 text-primary px-1 rounded text-[10px] font-medium">
            {selectedCount}
          </span>
        )}
        <ChevronDown className={cn('h-3 w-3 transition-transform', open && 'rotate-180')} />
      </button>

      {open && (
        <div className="absolute top-full left-0 mt-1 z-50 min-w-[160px] bg-popover border border-border rounded-md shadow-lg py-1 whitespace-nowrap">
          {options.map(option => {
            const Icon = option.icon
            const isSelected = selected.has(option.value)
            return (
              <button
                key={option.value}
                onClick={() => onToggle(option.value)}
                className="w-full flex items-center gap-2 px-3 py-1.5 text-xs hover:bg-muted transition-colors"
              >
                <div className={cn(
                  'w-3.5 h-3.5 rounded border flex items-center justify-center',
                  isSelected ? 'bg-primary border-primary' : 'border-muted-foreground/30'
                )}>
                  {isSelected && <CheckCircle2 className="h-2.5 w-2.5 text-primary-foreground" />}
                </div>
                {Icon && <Icon className="h-3 w-3 text-muted-foreground" />}
                <span className={isSelected ? 'text-foreground' : 'text-muted-foreground'}>{option.label}</span>
              </button>
            )
          })}
        </div>
      )}
    </div>
  )
}

// Presets dropdown menu
function PresetsDropdown({
  searchParams,
  onApplyPreset,
  onResetAll,
}: {
  searchParams: URLSearchParams
  onApplyPreset: (preset: typeof presets[0]) => void
  onResetAll: () => void
}) {
  const [open, setOpen] = useState(false)
  const dropdownRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    if (open) {
      document.addEventListener('mousedown', handleClickOutside)
      return () => document.removeEventListener('mousedown', handleClickOutside)
    }
  }, [open])

  const activePreset = presets.find(preset =>
    Object.entries(preset.params).every(([k, v]) => searchParams.get(k) === v) &&
    [...searchParams.keys()].filter(k => k !== 'search' && k !== 'internal' && k !== 'start' && k !== 'end').every(k => k in preset.params)
  )

  return (
    <div className="relative" ref={dropdownRef}>
      <button
        onClick={() => setOpen(!open)}
        className={cn(
          'flex items-center gap-1.5 px-2.5 py-1 text-sm rounded-md border transition-colors',
          activePreset
            ? 'bg-primary text-primary-foreground border-primary'
            : 'border-border text-muted-foreground hover:text-foreground bg-background'
        )}
      >
        <span>{activePreset ? activePreset.label : 'Presets'}</span>
        <ChevronDown className={cn('h-3.5 w-3.5 transition-transform', open && 'rotate-180')} />
      </button>
      {open && (
        <div className="absolute top-full left-0 mt-1 z-50 min-w-[200px] bg-popover border border-border rounded-md shadow-lg py-1 whitespace-nowrap">
          {presets.map(preset => {
            const isActive = preset === activePreset
            return (
              <button
                key={preset.label}
                onClick={() => {
                  if (isActive) {
                    onResetAll()
                  } else {
                    onApplyPreset(preset)
                  }
                  setOpen(false)
                }}
                className={cn(
                  'w-full flex items-center gap-2 px-3 py-1.5 text-sm hover:bg-muted transition-colors',
                  isActive ? 'text-foreground font-medium' : 'text-muted-foreground'
                )}
              >
                {isActive && <Check className="h-3.5 w-3.5 text-primary" />}
                {!isActive && <div className="w-3.5" />}
                {preset.label}
              </button>
            )
          })}
        </div>
      )}
    </div>
  )
}

// Count non-default advanced filters
function countActiveAdvancedFilters({
  selectedCategories,
  selectedEntityTypes,
  selectedActions,
  dzFilter,
  minStake,
  includeInternal,
}: {
  selectedCategories: Set<Category>
  selectedEntityTypes: Set<EntityType>
  selectedActions: Set<ActionFilter>
  dzFilter: DZFilter
  minStake: MinStakeOption
  includeInternal: boolean
}): number {
  let count = 0
  if (selectedCategories.size !== ALL_CATEGORIES.length) count++
  if (selectedActions.size !== ALL_ACTIONS.length) count++
  const defaultEntitySet = new Set(DEFAULT_ENTITY_TYPES)
  if (selectedEntityTypes.size !== defaultEntitySet.size || !DEFAULT_ENTITY_TYPES.every(e => selectedEntityTypes.has(e))) count++
  if (dzFilter !== 'on_dz') count++
  if (minStake !== '0') count++
  if (includeInternal) count++
  return count
}

export interface TimelineFiltersProps {
  searchParams: URLSearchParams
  timeRange: TimeRange | 'custom'
  selectedCategories: Set<Category>
  selectedEntityTypes: Set<EntityType>
  selectedActions: Set<ActionFilter>
  dzFilter: DZFilter
  minStake: MinStakeOption
  includeInternal: boolean
  customStart: string
  customEnd: string
  isFetching: boolean
  hasActiveFilters: boolean
  onTimeRangeChange: (range: TimeRange | 'custom') => void
  onToggleCategory: (category: Category) => void
  onToggleEntityType: (entityType: EntityType) => void
  onToggleAction: (action: ActionFilter) => void
  onDzFilterChange: (dz: DZFilter) => void
  onMinStakeChange: (value: MinStakeOption) => void
  onIncludeInternalChange: (value: boolean) => void
  onCustomStartChange: (value: string) => void
  onCustomEndChange: (value: string) => void
  onRefetch: () => void
  onResetAll: () => void
  onApplyPreset: (preset: typeof presets[0]) => void
}

export function TimelineFilters({
  searchParams,
  timeRange,
  selectedCategories,
  selectedEntityTypes,
  selectedActions,
  dzFilter,
  minStake,
  includeInternal,
  customStart,
  customEnd,
  isFetching,
  hasActiveFilters,
  onTimeRangeChange,
  onToggleCategory,
  onToggleEntityType,
  onToggleAction,
  onDzFilterChange,
  onMinStakeChange,
  onIncludeInternalChange,
  onCustomStartChange,
  onCustomEndChange,
  onRefetch,
  onResetAll,
  onApplyPreset,
}: TimelineFiltersProps) {
  const [advancedOpen, setAdvancedOpen] = useState(false)

  const hasSolanaEntities = ALL_SOLANA_ENTITIES.some(e => selectedEntityTypes.has(e))
  const maxDate = new Date().toISOString().slice(0, 16)

  const activeAdvancedCount = countActiveAdvancedFilters({
    selectedCategories,
    selectedEntityTypes,
    selectedActions,
    dzFilter,
    minStake,
    includeInternal,
  })

  // Auto-expand if there are active advanced filters
  useEffect(() => {
    if (activeAdvancedCount > 0) {
      setAdvancedOpen(true)
    }
  }, [activeAdvancedCount])

  return (
    <div className="mb-4 space-y-2 relative z-20">
      {/* Primary bar */}
      <div className="flex items-center gap-3">
        {/* Time range segmented control */}
        <div className="inline-flex rounded-md border border-border bg-background p-0.5">
          {timeRangeOptions.map(option => (
            <button
              key={option.value}
              onClick={() => onTimeRangeChange(option.value)}
              className={cn(
                'px-2.5 py-1 text-sm rounded transition-colors',
                timeRange === option.value
                  ? 'bg-primary text-primary-foreground'
                  : 'text-muted-foreground hover:text-foreground'
              )}
            >
              {option.label}
            </button>
          ))}
        </div>

        {/* Custom date range picker */}
        {timeRange === 'custom' && (
          <div className="inline-flex items-center gap-2">
            <Calendar className="h-4 w-4 text-muted-foreground" />
            <input
              type="datetime-local"
              value={customStart}
              max={maxDate}
              onChange={(e) => onCustomStartChange(e.target.value)}
              className="px-2 py-1 text-sm border border-border rounded-md bg-background"
            />
            <span className="text-muted-foreground">to</span>
            <input
              type="datetime-local"
              value={customEnd}
              max={maxDate}
              onChange={(e) => onCustomEndChange(e.target.value)}
              className="px-2 py-1 text-sm border border-border rounded-md bg-background"
            />
          </div>
        )}

        <div className="flex-1" />

        {/* Presets dropdown */}
        <PresetsDropdown searchParams={searchParams} onApplyPreset={onApplyPreset} onResetAll={onResetAll} />

        {/* Search button */}
        <button
          onClick={() => window.dispatchEvent(new CustomEvent('open-search'))}
          className="flex items-center gap-1.5 px-2.5 py-1 text-sm text-muted-foreground hover:text-foreground border border-border rounded-md bg-background hover:bg-muted transition-colors"
          title="Search (Cmd+K)"
        >
          <Search className="h-3.5 w-3.5" />
          <kbd className="font-mono text-[10px] text-muted-foreground">⌘K</kbd>
        </button>

        {/* Refresh */}
        <button
          onClick={onRefetch}
          disabled={isFetching}
          className="text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
          title="Refresh"
        >
          <RefreshCw className={cn('h-4 w-4', isFetching && 'animate-spin')} />
        </button>
      </div>

      {/* Advanced filters toggle */}
      <div>
        <button
          onClick={() => setAdvancedOpen(!advancedOpen)}
          className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors"
        >
          {advancedOpen ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
          <span>Filters</span>
          {activeAdvancedCount > 0 && (
            <span className="bg-primary/10 text-primary px-1.5 rounded text-[10px] font-medium">
              {activeAdvancedCount} active
            </span>
          )}
        </button>

        {/* Collapsible advanced filters */}
        <div
          className={cn(
            'overflow-hidden transition-[max-height,opacity] duration-200',
            advancedOpen ? 'max-h-[200px] opacity-100 mt-2' : 'max-h-0 opacity-0'
          )}
        >
          <div className="flex flex-wrap items-center gap-x-3 gap-y-2 rounded-lg border border-border bg-muted/20 p-3">
            <FilterDropdown
              label="Event Type"
              options={categoryOptions}
              selected={selectedCategories}
              onToggle={onToggleCategory}
              allValues={ALL_CATEGORIES}
            />

            <FilterDropdown
              label="Action"
              options={actionOptions}
              selected={selectedActions}
              onToggle={onToggleAction}
              allValues={ALL_ACTIONS}
            />

            <div className="h-4 w-px bg-border" />

            <FilterDropdown
              label="DoubleZero"
              options={dzEntityOptions}
              selected={selectedEntityTypes}
              onToggle={onToggleEntityType}
              allValues={ALL_DZ_ENTITIES}
            />

            <FilterDropdown
              label="Solana"
              options={solanaEntityOptions}
              selected={selectedEntityTypes}
              onToggle={onToggleEntityType}
              allValues={ALL_SOLANA_ENTITIES}
            />

            {hasSolanaEntities && (
              <div className="inline-flex rounded-md border border-border bg-background p-0.5 gap-0.5">
                {dzFilterOptions.map(option => (
                  <button
                    key={option.value}
                    onClick={() => onDzFilterChange(option.value)}
                    className={cn(
                      'px-2 py-0.5 text-xs rounded transition-colors',
                      dzFilter === option.value
                        ? 'bg-primary text-primary-foreground'
                        : 'text-muted-foreground hover:text-foreground'
                    )}
                  >
                    {option.label}
                  </button>
                ))}
              </div>
            )}

            {hasSolanaEntities && (
              <div className="flex items-center gap-1.5">
                <span className="text-xs text-muted-foreground uppercase tracking-wide">DZ Stake Change</span>
                <select
                  value={minStake}
                  onChange={(e) => onMinStakeChange(e.target.value as MinStakeOption)}
                  className="px-2 py-0.5 text-xs rounded-md border border-border bg-background text-foreground cursor-pointer"
                >
                  {minStakeOptions.map(option => (
                    <option key={option.value} value={option.value}>{option.label}</option>
                  ))}
                </select>
              </div>
            )}

            <div className="flex-1" />

            <button
              onClick={() => onIncludeInternalChange(!includeInternal)}
              className="inline-flex items-center gap-2 text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              <span>Internal users</span>
              <div className={cn(
                'relative w-7 h-4 rounded-full transition-colors',
                includeInternal ? 'bg-primary' : 'bg-muted-foreground/30'
              )}>
                <div className={cn(
                  'absolute top-0.5 w-3 h-3 rounded-full bg-white shadow transition-transform',
                  includeInternal ? 'translate-x-3.5' : 'translate-x-0.5'
                )} />
              </div>
            </button>

            <button
              onClick={onResetAll}
              disabled={!hasActiveFilters}
              className={cn(
                'inline-flex items-center gap-1 text-xs transition-colors',
                hasActiveFilters
                  ? 'text-muted-foreground hover:text-foreground cursor-pointer'
                  : 'text-muted-foreground/40 cursor-not-allowed'
              )}
            >
              <RotateCw className="h-3 w-3" />
              Reset
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
