import { useState, useMemo, useEffect, useRef, useCallback } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import {
  fetchRewardsLiveNetwork,
  fetchRewardsSimulate,
  fetchRewardsCompare,
  fetchRewardsLinkEstimate,
  type RewardsNetwork,
  type OperatorValue,
  type OperatorDelta,
  type RewardsLinkResult,
  type RewardsCompareResponse,
} from '@/lib/api'
import { Loader2, Play, GitCompare, Unlink, Plus, RefreshCw, TrendingUp, TrendingDown, Coins, ChevronUp, ChevronDown, X } from 'lucide-react'
import { useDelayedLoading } from '@/hooks/use-delayed-loading'
import { Link, useSearchParams } from 'react-router-dom'
import { PageHeader } from './page-header'

type Mode = 'simulate' | 'compare' | 'link-estimate'
type SortDir = 'asc' | 'desc'

const STORAGE_KEY = 'rewards-network'

function formatPercent(value: number, decimals = 2): string {
  return `${(value * 100).toFixed(decimals)}%`
}

function formatValue(value: number): string {
  if (value === 0) return '0'
  const abs = Math.abs(value)
  if (abs >= 0.0001) return value.toFixed(4)
  if (abs >= 0.000001) return value.toFixed(6)
  return value.toExponential(2)
}

// Operator color palette
const COLORS = [
  '#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6',
  '#EC4899', '#06B6D4', '#84CC16', '#F97316', '#6366F1',
  '#14B8A6', '#D946EF', '#0EA5E9', '#22C55E', '#E11D48',
]

function getColor(index: number): string {
  return COLORS[index % COLORS.length]
}

function getStoredNetwork(): RewardsNetwork | null {
  if (typeof window === 'undefined') return null
  try {
    const stored = localStorage.getItem(STORAGE_KEY)
    if (stored) return JSON.parse(stored) as RewardsNetwork
  } catch { /* ignore */ }
  return null
}

// Build lookup maps from network devices
function buildNameMaps(network: RewardsNetwork) {
  const operatorNames = new Map<string, string>()
  const operatorPks = new Map<string, string>()
  const cityNames = new Map<string, string>()
  for (const d of network.devices) {
    if (d.operator && d.operator_name) operatorNames.set(d.operator, d.operator_name)
    if (d.operator && d.operator_pk) operatorPks.set(d.operator, d.operator_pk)
    if (d.city && d.city_name) cityNames.set(d.city, d.city_name)
  }
  return { operatorNames, operatorPks, cityNames }
}

// Contributor label — code as link if pk is known, plain text otherwise
function ContributorLabel({ operator, operatorNames, operatorPks }: {
  operator: string
  operatorNames: Map<string, string>
  operatorPks: Map<string, string>
}) {
  const pk = operatorPks.get(operator)
  const name = operatorNames.get(operator) || operator
  if (pk) {
    return (
      <Link
        to={`/dz/contributors/${pk}`}
        className="hover:underline text-blue-500"
        title={name}
      >
        {operator}
      </Link>
    )
  }
  return <span title={name}>{operator}</span>
}

// Sortable column header button
function SortHeader({
  label,
  field,
  sortField,
  sortDir,
  onSort,
  align = 'right',
}: {
  label: string
  field: string
  sortField: string
  sortDir: SortDir
  onSort: (field: string) => void
  align?: 'left' | 'right'
}) {
  const active = sortField === field
  return (
    <th className={`py-2 px-4 font-medium ${align === 'right' ? 'text-right' : 'text-left'}`}>
      <button
        onClick={() => onSort(field)}
        className={`inline-flex items-center gap-1 hover:text-foreground transition-colors ${active ? 'text-foreground' : ''}`}
      >
        {label}
        {active
          ? sortDir === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />
          : <ChevronDown className="h-3 w-3 opacity-30" />}
      </button>
    </th>
  )
}

function useSort<T extends string>(defaultField: T, defaultDir: SortDir = 'desc') {
  const [sortField, setSortField] = useState<T>(defaultField)
  const [sortDir, setSortDir] = useState<SortDir>(defaultDir)

  const handleSort = (field: string) => {
    const f = field as T
    if (sortField === f) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortField(f)
      setSortDir('desc')
    }
  }

  return { sortField, sortDir, handleSort }
}

// Results table for operator values
function OperatorTable({ results, title, operatorNames, operatorPks }: { results: OperatorValue[]; title?: string; operatorNames: Map<string, string>; operatorPks: Map<string, string> }) {
  const { sortField, sortDir, handleSort } = useSort<'operator' | 'value' | 'proportion'>('value')

  const sorted = useMemo(() => {
    return [...results].sort((a, b) => {
      let cmp = 0
      if (sortField === 'operator') {
        cmp = a.operator.localeCompare(b.operator)
      } else if (sortField === 'value') {
        cmp = a.value - b.value
      } else {
        cmp = a.proportion - b.proportion
      }
      return sortDir === 'asc' ? cmp : -cmp
    })
  }, [results, sortField, sortDir, operatorNames])

  const total = results.reduce((s, r) => s + r.value, 0)

  return (
    <div className="bg-card border border-border rounded-lg overflow-hidden">
      {title && (
        <div className="px-4 py-2 border-b border-border bg-muted/30">
          <h3 className="text-sm font-medium">{title}</h3>
        </div>
      )}
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border text-muted-foreground">
            <SortHeader label="Contributor" field="operator" sortField={sortField} sortDir={sortDir} onSort={handleSort} align="left" />
            <SortHeader label="Value" field="value" sortField={sortField} sortDir={sortDir} onSort={handleSort} />
            <SortHeader label="Share" field="proportion" sortField={sortField} sortDir={sortDir} onSort={handleSort} />
            <th className="text-left py-2 px-4 font-medium w-40">Distribution</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((r, i) => (
            <tr key={r.operator} className="border-b border-border/50 hover:bg-muted/30">
              <td className="py-2 px-4 font-medium flex items-center gap-2">
                <span
                  className="w-3 h-3 rounded-full inline-block flex-shrink-0"
                  style={{ backgroundColor: getColor(i) }}
                />
                <ContributorLabel operator={r.operator} operatorNames={operatorNames} operatorPks={operatorPks} />
              </td>
              <td className="py-2 px-4 text-right tabular-nums">{formatValue(r.value)}</td>
              <td className="py-2 px-4 text-right tabular-nums">{formatPercent(r.proportion)}</td>
              <td className="py-2 px-4">
                <div className="w-full bg-muted rounded-full h-2">
                  <div
                    className="h-2 rounded-full transition-all"
                    style={{
                      width: `${Math.max(r.proportion * 100, 0.5)}%`,
                      backgroundColor: getColor(i),
                    }}
                  />
                </div>
              </td>
            </tr>
          ))}
        </tbody>
        <tfoot>
          <tr className="border-t border-border font-medium">
            <td className="py-2 px-4">Total</td>
            <td className="py-2 px-4 text-right tabular-nums">{formatValue(total)}</td>
            <td className="py-2 px-4 text-right tabular-nums">100%</td>
            <td className="py-2 px-4" />
          </tr>
        </tfoot>
      </table>
    </div>
  )
}

// Delta table for comparison results
function DeltaTable({ deltas, operatorNames, operatorPks }: { deltas: OperatorDelta[]; operatorNames: Map<string, string>; operatorPks: Map<string, string> }) {
  const { sortField, sortDir, handleSort } = useSort<'operator' | 'baseline_value' | 'modified_value' | 'value_delta' | 'proportion_delta'>('value_delta')

  const sorted = useMemo(() => {
    return [...deltas].sort((a, b) => {
      let cmp = 0
      if (sortField === 'operator') {
        cmp = a.operator.localeCompare(b.operator)
      } else if (sortField === 'baseline_value') {
        cmp = a.baseline_value - b.baseline_value
      } else if (sortField === 'modified_value') {
        cmp = a.modified_value - b.modified_value
      } else if (sortField === 'value_delta') {
        cmp = Math.abs(a.value_delta) - Math.abs(b.value_delta)
      } else {
        cmp = Math.abs(a.proportion_delta) - Math.abs(b.proportion_delta)
      }
      return sortDir === 'asc' ? cmp : -cmp
    })
  }, [deltas, sortField, sortDir, operatorNames])

  return (
    <div className="bg-card border border-border rounded-lg overflow-hidden">
      <div className="px-4 py-2 border-b border-border bg-muted/30">
        <h3 className="text-sm font-medium">Changes</h3>
      </div>
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border text-muted-foreground">
            <SortHeader label="Contributor" field="operator" sortField={sortField} sortDir={sortDir} onSort={handleSort} align="left" />
            <SortHeader label="Baseline" field="baseline_value" sortField={sortField} sortDir={sortDir} onSort={handleSort} />
            <SortHeader label="Modified" field="modified_value" sortField={sortField} sortDir={sortDir} onSort={handleSort} />
            <SortHeader label="Value Delta" field="value_delta" sortField={sortField} sortDir={sortDir} onSort={handleSort} />
            <SortHeader label="Share Delta" field="proportion_delta" sortField={sortField} sortDir={sortDir} onSort={handleSort} />
          </tr>
        </thead>
        <tbody>
          {sorted.map((d) => (
            <tr key={d.operator} className="border-b border-border/50 hover:bg-muted/30">
              <td className="py-2 px-4 font-medium"><ContributorLabel operator={d.operator} operatorNames={operatorNames} operatorPks={operatorPks} /></td>
              <td className="py-2 px-4 text-right tabular-nums">{formatValue(d.baseline_value)}</td>
              <td className="py-2 px-4 text-right tabular-nums">{formatValue(d.modified_value)}</td>
              <td className={`py-2 px-4 text-right tabular-nums ${d.value_delta > 0 ? 'text-green-600 dark:text-green-400' : d.value_delta < 0 ? 'text-red-600 dark:text-red-400' : ''}`}>
                <span className="inline-flex items-center gap-1">
                  {d.value_delta > 0 && <TrendingUp className="h-3.5 w-3.5" />}
                  {d.value_delta < 0 && <TrendingDown className="h-3.5 w-3.5" />}
                  {d.value_delta > 0 ? '+' : ''}{formatValue(d.value_delta)}
                </span>
              </td>
              <td className={`py-2 px-4 text-right tabular-nums ${d.proportion_delta > 0 ? 'text-green-600 dark:text-green-400' : d.proportion_delta < 0 ? 'text-red-600 dark:text-red-400' : ''}`}>
                <span className="inline-flex items-center gap-1">
                  {d.proportion_delta > 0 && <TrendingUp className="h-3.5 w-3.5" />}
                  {d.proportion_delta < 0 && <TrendingDown className="h-3.5 w-3.5" />}
                  {d.proportion_delta > 0 ? '+' : ''}{formatPercent(d.proportion_delta)}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// Link estimate results table
function LinkTable({ results }: { results: RewardsLinkResult[] }) {
  const { sortField, sortDir, handleSort } = useSort<'device1' | 'device2' | 'bandwidth' | 'latency' | 'value' | 'percent'>('value')

  const sorted = useMemo(() => {
    return [...results].sort((a, b) => {
      let cmp = 0
      if (sortField === 'device1') cmp = a.device1.localeCompare(b.device1)
      else if (sortField === 'device2') cmp = a.device2.localeCompare(b.device2)
      else if (sortField === 'bandwidth') cmp = a.bandwidth - b.bandwidth
      else if (sortField === 'latency') cmp = a.latency - b.latency
      else if (sortField === 'value') cmp = a.value - b.value
      else cmp = a.percent - b.percent
      return sortDir === 'asc' ? cmp : -cmp
    })
  }, [results, sortField, sortDir])

  const maxValue = sorted.length > 0 ? Math.max(...sorted.map(r => r.value)) : 1

  return (
    <div className="bg-card border border-border rounded-lg overflow-hidden">
      <div className="px-4 py-2 border-b border-border bg-muted/30">
        <h3 className="text-sm font-medium">Per-Link Values</h3>
      </div>
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border text-muted-foreground">
            <SortHeader label="Device 1" field="device1" sortField={sortField} sortDir={sortDir} onSort={handleSort} align="left" />
            <SortHeader label="Device 2" field="device2" sortField={sortField} sortDir={sortDir} onSort={handleSort} align="left" />
            <SortHeader label="Bandwidth" field="bandwidth" sortField={sortField} sortDir={sortDir} onSort={handleSort} />
            <SortHeader label="Latency" field="latency" sortField={sortField} sortDir={sortDir} onSort={handleSort} />
            <SortHeader label="Value" field="value" sortField={sortField} sortDir={sortDir} onSort={handleSort} />
            <SortHeader label="Share" field="percent" sortField={sortField} sortDir={sortDir} onSort={handleSort} />
            <th className="text-left py-2 px-4 font-medium w-32">Distribution</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((r, i) => (
            <tr key={`${r.device1}-${r.device2}-${i}`} className="border-b border-border/50 hover:bg-muted/30">
              <td className="py-2 px-4 font-mono text-xs">{r.device1}</td>
              <td className="py-2 px-4 font-mono text-xs">{r.device2}</td>
              <td className="py-2 px-4 text-right tabular-nums">{r.bandwidth} Gbps</td>
              <td className="py-2 px-4 text-right tabular-nums">{r.latency.toFixed(2)} ms</td>
              <td className="py-2 px-4 text-right tabular-nums">{formatValue(r.value)}</td>
              <td className="py-2 px-4 text-right tabular-nums">{formatPercent(r.percent)}</td>
              <td className="py-2 px-4">
                <div className="w-full bg-muted rounded-full h-1.5">
                  <div
                    className="h-1.5 rounded-full bg-blue-500 transition-all"
                    style={{ width: `${Math.max((r.value / maxValue) * 100, 0.5)}%` }}
                  />
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// Quick add link form
function QuickAddLink({
  network,
  onAdd,
}: {
  network: RewardsNetwork
  onAdd: (network: RewardsNetwork) => void
}) {
  const { cityNames, operatorNames } = useMemo(() => buildNameMaps(network), [network])

  const cities = useMemo(() => {
    const set = new Set<string>()
    for (const d of network.devices) {
      if (d.city) set.add(d.city)
    }
    return [...set].sort()
  }, [network])

  const operators = useMemo(() => {
    const set = new Set<string>()
    for (const d of network.devices) {
      if (d.operator) set.add(d.operator)
    }
    return [...set].sort()
  }, [network])

  const [city1, setCity1] = useState('')
  const [city2, setCity2] = useState('')
  const [operator, setOperator] = useState('')
  const [latency, setLatency] = useState('10')
  const bandwidth = '10'

  const handleAdd = () => {
    if (!city1 || !city2 || !operator || city1 === city2) return

    const existingDevices = new Set(network.devices.map(d => d.device))
    let dev1 = `${city1}99`
    let dev2 = `${city2}99`
    let counter = 99
    while (existingDevices.has(dev1)) { counter++; dev1 = `${city1}${counter}` }
    counter = 99
    while (existingDevices.has(dev2)) { counter++; dev2 = `${city2}${counter}` }

    const newNetwork: RewardsNetwork = {
      ...network,
      private_links: [
        ...network.private_links,
        {
          device1: dev1,
          device2: dev2,
          latency: parseFloat(latency) || 10,
          bandwidth: parseFloat(bandwidth) || 10,
          uptime: 0.99,
          shared: 'NA',
        },
      ],
      devices: [
        ...network.devices,
        { device: dev1, edge: 10, operator, city: city1, city_name: cityNames.get(city1) || city1, operator_name: operatorNames.get(operator) || operator },
        { device: dev2, edge: 10, operator, city: city2, city_name: cityNames.get(city2) || city2, operator_name: operatorNames.get(operator) || operator },
      ],
    }

    onAdd(newNetwork)
  }

  return (
    <div className="bg-card border border-border rounded-lg p-4">
      <h3 className="text-sm font-medium mb-3">Add Link</h3>
      <div className="grid grid-cols-2 md:grid-cols-5 gap-2">
        <select
          value={city1}
          onChange={(e) => setCity1(e.target.value)}
          className="px-2 py-1.5 text-sm bg-background border border-border rounded"
        >
          <option value="">City 1</option>
          {cities.map(c => <option key={c} value={c}>{cityNames.get(c) || c}</option>)}
        </select>
        <select
          value={city2}
          onChange={(e) => setCity2(e.target.value)}
          className="px-2 py-1.5 text-sm bg-background border border-border rounded"
        >
          <option value="">City 2</option>
          {cities.map(c => <option key={c} value={c}>{cityNames.get(c) || c}</option>)}
        </select>
        <select
          value={operator}
          onChange={(e) => setOperator(e.target.value)}
          className="px-2 py-1.5 text-sm bg-background border border-border rounded"
        >
          <option value="">Contributor</option>
          {operators.map(o => <option key={o} value={o}>{o}</option>)}
        </select>
        <input
          type="number"
          value={latency}
          onChange={(e) => setLatency(e.target.value)}
          placeholder="Latency (ms)"
          className="px-2 py-1.5 text-sm bg-background border border-border rounded"
        />
        <button
          onClick={handleAdd}
          disabled={!city1 || !city2 || !operator || city1 === city2}
          className="flex items-center justify-center gap-1 px-3 py-1.5 text-sm bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          <Plus className="h-3.5 w-3.5" />
          Add
        </button>
      </div>
    </div>
  )
}

// Elapsed time display for long-running operations
function useElapsed(running: boolean) {
  const [elapsed, setElapsed] = useState(0)
  const startRef = useRef(0)

  useEffect(() => {
    if (!running) { setElapsed(0); return }
    startRef.current = Date.now()
    const id = setInterval(() => setElapsed(Math.floor((Date.now() - startRef.current) / 1000)), 1000)
    return () => clearInterval(id)
  }, [running])

  return elapsed
}

function formatElapsed(s: number): string {
  if (s < 60) return `${s}s`
  return `${Math.floor(s / 60)}m ${s % 60}s`
}

// Contributor summary table — computed from network data, no simulation needed
function ContributorSummary({ network, operatorNames, operatorPks }: { network: RewardsNetwork; operatorNames: Map<string, string>; operatorPks: Map<string, string> }) {
  const { sortField, sortDir, handleSort } = useSort<'operator' | 'devices' | 'links' | 'metros'>('devices')

  const stats = useMemo(() => {
    const map = new Map<string, { devices: number; links: number; metros: Set<string> }>()
    for (const d of network.devices) {
      if (!d.operator) continue
      if (!map.has(d.operator)) map.set(d.operator, { devices: 0, links: 0, metros: new Set() })
      const s = map.get(d.operator)!
      s.devices++
      if (d.city) s.metros.add(d.city)
    }
    const devicesByOp = new Map<string, Set<string>>()
    for (const d of network.devices) {
      if (!d.operator) continue
      if (!devicesByOp.has(d.operator)) devicesByOp.set(d.operator, new Set())
      devicesByOp.get(d.operator)!.add(d.device)
    }
    for (const l of network.private_links) {
      for (const [op, devs] of devicesByOp) {
        if (devs.has(l.device1) || devs.has(l.device2)) {
          map.get(op)!.links++
        }
      }
    }
    return [...map.entries()]
      .map(([op, s]) => ({ operator: op, devices: s.devices, links: s.links, metros: s.metros.size }))
  }, [network])

  const sorted = useMemo(() => {
    return [...stats].sort((a, b) => {
      let cmp = 0
      if (sortField === 'operator') {
        cmp = a.operator.localeCompare(b.operator)
      } else if (sortField === 'devices') {
        cmp = a.devices - b.devices
      } else if (sortField === 'links') {
        cmp = a.links - b.links
      } else {
        cmp = a.metros - b.metros
      }
      return sortDir === 'asc' ? cmp : -cmp
    })
  }, [stats, sortField, sortDir, operatorNames])

  return (
    <div className="bg-card border border-border rounded-lg overflow-hidden">
      <div className="px-4 py-2 border-b border-border bg-muted/30">
        <h3 className="text-sm font-medium">Network Contributors</h3>
      </div>
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border text-muted-foreground">
            <SortHeader label="Contributor" field="operator" sortField={sortField} sortDir={sortDir} onSort={handleSort} align="left" />
            <SortHeader label="Devices" field="devices" sortField={sortField} sortDir={sortDir} onSort={handleSort} />
            <SortHeader label="Links" field="links" sortField={sortField} sortDir={sortDir} onSort={handleSort} />
            <SortHeader label="Metros" field="metros" sortField={sortField} sortDir={sortDir} onSort={handleSort} />
          </tr>
        </thead>
        <tbody>
          {sorted.map((s, i) => (
            <tr key={s.operator} className="border-b border-border/50 hover:bg-muted/30">
              <td className="py-2 px-4 font-medium flex items-center gap-2">
                <span
                  className="w-3 h-3 rounded-full inline-block flex-shrink-0"
                  style={{ backgroundColor: getColor(i) }}
                />
                <ContributorLabel operator={s.operator} operatorNames={operatorNames} operatorPks={operatorPks} />
              </td>
              <td className="py-2 px-4 text-right">
                <Link
                  to={`/dz/devices?search=contributor:${s.operator}`}
                  className="tabular-nums text-blue-500 hover:underline"
                >
                  {s.devices}
                </Link>
              </td>
              <td className="py-2 px-4 text-right">
                <Link
                  to={`/dz/links?search=contributor:${s.operator}`}
                  className="tabular-nums text-blue-500 hover:underline"
                >
                  {s.links}
                </Link>
              </td>
              <td className="py-2 px-4 text-right tabular-nums">{s.metros}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function RewardsPageSkeleton() {
  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-6xl mx-auto px-8 py-6 space-y-6">
        <div>
          <div className="h-8 w-64 bg-muted rounded animate-pulse" />
          <div className="h-4 w-96 bg-muted rounded animate-pulse mt-2" />
        </div>
      </div>
    </div>
  )
}

export function RewardsPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const mode = (searchParams.get('mode') as Mode | null) ?? 'simulate'
  const setMode = (m: Mode) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      next.set('mode', m)
      return next
    }, { replace: true })
  }

  const [baselineNetwork, setBaselineNetwork] = useState<RewardsNetwork | null>(() => getStoredNetwork())
  const [modifiedNetwork, setModifiedNetwork] = useState<RewardsNetwork | null>(() => getStoredNetwork())
  const [selectedOperator, setSelectedOperator] = useState<string>('')

  // Simulation results
  const [compareResults, setCompareResults] = useState<RewardsCompareResponse | null>(null)
  const [linkResults, setLinkResults] = useState<RewardsLinkResult[] | null>(null)

  // AbortControllers for cancellation
  const compareAbortRef = useRef<AbortController | null>(null)
  const linkAbortRef = useRef<AbortController | null>(null)

  // Load live network automatically (fetch once, long stale time since cache handles freshness)
  const liveNetworkQuery = useQuery({
    queryKey: ['rewardsLiveNetwork'],
    queryFn: fetchRewardsLiveNetwork,
    staleTime: 5 * 60 * 1000,
    refetchOnWindowFocus: false,
  })

  // Simulate query (auto-fetch, results come from background cache)
  const simulateQuery = useQuery({
    queryKey: ['rewardsSimulate'],
    queryFn: () => fetchRewardsSimulate(),
    retry: (failureCount, error) => {
      // Keep retrying 503 (computing) with longer intervals
      if (error instanceof Error && error.message === 'computing') return failureCount < 30
      return failureCount < 3
    },
    retryDelay: (attemptIndex, error) => {
      if (error instanceof Error && error.message === 'computing') return 10000
      return Math.min(1000 * 2 ** attemptIndex, 30000)
    },
    staleTime: 5 * 60 * 1000,
    refetchOnWindowFocus: false,
  })

  const simResults = simulateQuery.data?.results ?? null
  const simComputing = simulateQuery.error instanceof Error && simulateQuery.error.message === 'computing'

  // Update state when live network loads
  useEffect(() => {
    if (liveNetworkQuery.data) {
      setBaselineNetwork(liveNetworkQuery.data.network)
      setModifiedNetwork(liveNetworkQuery.data.network)
      try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(liveNetworkQuery.data.network))
      } catch { /* ignore quota errors */ }
    }
  }, [liveNetworkQuery.data])

  const refreshNetwork = async () => {
    const result = await liveNetworkQuery.refetch()
    if (result.data) {
      setCompareResults(null)
      setLinkResults(null)
    }
  }

  // Compare mutation
  const compareMutation = useMutation({
    mutationFn: async () => {
      if (!baselineNetwork || !modifiedNetwork) throw new Error('Networks not loaded')
      compareAbortRef.current = new AbortController()
      return fetchRewardsCompare(baselineNetwork, modifiedNetwork, compareAbortRef.current.signal)
    },
    onSuccess: (data) => {
      setCompareResults(data)
    },
  })

  // Link estimate mutation
  const linkEstimateMutation = useMutation({
    mutationFn: async () => {
      if (!baselineNetwork || !selectedOperator) throw new Error('Network or operator not selected')
      linkAbortRef.current = new AbortController()
      return fetchRewardsLinkEstimate(selectedOperator, baselineNetwork, linkAbortRef.current.signal)
    },
    onSuccess: (data) => {
      setLinkResults(data.results)
    },
  })

  const cancelCompare = useCallback(() => {
    compareAbortRef.current?.abort()
    compareMutation.reset()
  }, [compareMutation])

  const cancelLinkEstimate = useCallback(() => {
    linkAbortRef.current?.abort()
    linkEstimateMutation.reset()
  }, [linkEstimateMutation])

  // Build name lookup maps from network devices
  const { operatorNames, operatorPks } = useMemo(() => {
    if (!baselineNetwork) return { operatorNames: new Map<string, string>(), operatorPks: new Map<string, string>(), cityNames: new Map<string, string>() }
    return buildNameMaps(baselineNetwork)
  }, [baselineNetwork])

  const operators = useMemo(() => {
    if (!baselineNetwork) return []
    const set = new Set<string>()
    for (const d of baselineNetwork.devices) {
      if (d.operator) set.add(d.operator)
    }
    return [...set].sort()
  }, [baselineNetwork])

  const selectedOperatorLinkCount = useMemo(() => {
    if (!baselineNetwork || !selectedOperator) return 0
    const operatorDevices = new Set(
      baselineNetwork.devices
        .filter(d => d.operator === selectedOperator)
        .map(d => d.device)
    )
    return baselineNetwork.private_links.filter(
      l => operatorDevices.has(l.device1) || operatorDevices.has(l.device2)
    ).length
  }, [baselineNetwork, selectedOperator])

  const compareElapsed = useElapsed(compareMutation.isPending)
  const linkElapsed = useElapsed(linkEstimateMutation.isPending)

  const networkLoading = liveNetworkQuery.isFetching
  const isLoading = networkLoading || compareMutation.isPending || linkEstimateMutation.isPending
  const error = (simulateQuery.error instanceof Error && simulateQuery.error.message !== 'computing' ? simulateQuery.error : null) || compareMutation.error || linkEstimateMutation.error || liveNetworkQuery.error

  const showSkeleton = useDelayedLoading(liveNetworkQuery.isLoading && !baselineNetwork)

  const handleModifiedNetworkChange = (network: RewardsNetwork) => {
    setModifiedNetwork(network)
    setCompareResults(null)
  }

  const addedLinks = useMemo(() => {
    if (!baselineNetwork || !modifiedNetwork) return []
    return modifiedNetwork.private_links.slice(baselineNetwork.private_links.length)
  }, [baselineNetwork, modifiedNetwork])

  const [showAddedLinks, setShowAddedLinks] = useState(false)

  const networkSubtitle = liveNetworkQuery.data ? (
    <span className="text-sm text-muted-foreground">
      {liveNetworkQuery.data.device_count} devices · {liveNetworkQuery.data.link_count} links · {liveNetworkQuery.data.operator_count} contributors · {liveNetworkQuery.data.metro_count} metros
    </span>
  ) : undefined

  if (showSkeleton) {
    return <RewardsPageSkeleton />
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-6xl mx-auto px-8 py-6 space-y-6">
        <PageHeader
          icon={Coins}
          title="Rewards Simulation"
          subtitle={networkSubtitle}
          actions={
            <button
              onClick={refreshNetwork}
              disabled={isLoading}
              className="flex items-center gap-2 px-3 py-1.5 text-sm bg-card border border-border rounded-lg hover:bg-muted/50 transition-colors disabled:opacity-50"
            >
              {liveNetworkQuery.isFetching ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
              Refresh Network
            </button>
          }
        />

        {/* Mode selector */}
        <div className="flex rounded-lg border border-border overflow-hidden w-fit">
          <button
            onClick={() => setMode('simulate')}
            className={`px-3 py-1.5 text-sm transition-colors ${mode === 'simulate' ? 'bg-blue-600 text-white' : 'bg-card hover:bg-muted/50'}`}
          >
            <Play className="h-3.5 w-3.5 inline mr-1" />
            Simulate
          </button>
          <button
            onClick={() => setMode('compare')}
            className={`px-3 py-1.5 text-sm border-l border-border transition-colors ${mode === 'compare' ? 'bg-blue-600 text-white' : 'bg-card hover:bg-muted/50'}`}
          >
            <GitCompare className="h-3.5 w-3.5 inline mr-1" />
            Compare
          </button>
          <button
            onClick={() => setMode('link-estimate')}
            className={`px-3 py-1.5 text-sm border-l border-border transition-colors ${mode === 'link-estimate' ? 'bg-blue-600 text-white' : 'bg-card hover:bg-muted/50'}`}
          >
            <Unlink className="h-3.5 w-3.5 inline mr-1" />
            Link Value
          </button>
        </div>

        {/* Error */}
        {error && (
          <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-3 text-sm text-red-500">
            {error instanceof Error ? error.message : 'An error occurred'}
          </div>
        )}

        {/* Simulate mode */}
        {mode === 'simulate' && (
          <div className="space-y-4">
            {baselineNetwork && (
              <ContributorSummary network={baselineNetwork} operatorNames={operatorNames} operatorPks={operatorPks} />
            )}

            {simComputing && (
              <div className="bg-blue-500/10 border border-blue-500/20 rounded-lg p-4 flex items-center gap-3">
                <Loader2 className="h-5 w-5 animate-spin text-blue-500" />
                <div>
                  <p className="text-sm font-medium">Computing Shapley values...</p>
                  <p className="text-xs text-muted-foreground">First run takes 1–2 minutes. Results will appear automatically.</p>
                </div>
              </div>
            )}

            {simulateQuery.isLoading && !simComputing && (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                Loading simulation results...
              </div>
            )}

            {simResults && simResults.length > 0 ? (
              <>
                <OperatorTable results={simResults} title="Shapley Reward Shares" operatorNames={operatorNames} operatorPks={operatorPks} />
                {simulateQuery.data?.computed_at && (
                  <div className="flex items-center justify-between text-xs text-muted-foreground">
                    <span>
                      Last computed {new Date(simulateQuery.data.computed_at).toLocaleString()}
                      {simulateQuery.data.epoch ? ` · Epoch ${simulateQuery.data.epoch}` : ''}
                    </span>
                    <span>Refreshes automatically each epoch (~2–3 days)</span>
                  </div>
                )}
              </>
            ) : simResults ? (
              <div className="text-center text-muted-foreground py-8">
                No simulation results available
              </div>
            ) : null}
          </div>
        )}

        {/* Compare mode */}
        {mode === 'compare' && (
          <div className="space-y-4">
            {baselineNetwork && modifiedNetwork && (
              <>
                <QuickAddLink
                  network={modifiedNetwork}
                  onAdd={handleModifiedNetworkChange}
                />

                {addedLinks.length > 0 && (
                  <div className="bg-card border border-border rounded-lg p-3 space-y-2">
                    <div className="flex items-center gap-2 text-sm">
                      <button
                        onClick={() => setShowAddedLinks(!showAddedLinks)}
                        className="text-foreground font-medium hover:underline"
                      >
                        +{addedLinks.length} link{addedLinks.length > 1 ? 's' : ''} added
                      </button>
                      <button
                        onClick={() => { setModifiedNetwork(baselineNetwork); setCompareResults(null); setShowAddedLinks(false) }}
                        className="text-blue-500 hover:underline text-sm"
                      >
                        Reset
                      </button>
                    </div>
                    {showAddedLinks && modifiedNetwork && (
                      <div className="text-xs text-muted-foreground space-y-1">
                        {addedLinks.map((l, i) => {
                          const d1 = modifiedNetwork.devices.find(d => d.device === l.device1)
                          const d2 = modifiedNetwork.devices.find(d => d.device === l.device2)
                          const city1Label = d1?.city_name || d1?.city || l.device1
                          const city2Label = d2?.city_name || d2?.city || l.device2
                          return (
                            <div key={i} className="flex items-center gap-2">
                              <span>{city1Label}</span>
                              <span>↔</span>
                              <span>{city2Label}</span>
                              <span className="text-muted-foreground/60">·</span>
                              <span>{l.bandwidth} Gbps, {l.latency} ms</span>
                            </div>
                          )
                        })}
                      </div>
                    )}
                  </div>
                )}

                <div className="flex items-center gap-2">
                  <button
                    onClick={() => compareMutation.mutate()}
                    disabled={networkLoading || compareMutation.isPending || addedLinks.length === 0}
                    className="flex items-center gap-2 px-4 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50"
                  >
                    {compareMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <GitCompare className="h-4 w-4" />}
                    {compareMutation.isPending ? `Running... ${compareElapsed > 0 ? formatElapsed(compareElapsed) : ''}` : 'Run Comparison (~1 min)'}
                  </button>
                  {compareMutation.isPending && (
                    <button
                      onClick={cancelCompare}
                      className="flex items-center gap-1 px-2 py-1.5 text-xs border border-border rounded hover:bg-muted/50 transition-colors"
                    >
                      <X className="h-3 w-3" /> Cancel
                    </button>
                  )}
                </div>
              </>
            )}

            {!baselineNetwork && (
              <p className="text-sm text-muted-foreground">Load a network first to compare configurations.</p>
            )}

            {compareResults && compareResults.deltas.length > 0 ? (
              <>
                <DeltaTable deltas={compareResults.deltas} operatorNames={operatorNames} operatorPks={operatorPks} />
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                  <OperatorTable results={compareResults.baseline_results} title="Baseline" operatorNames={operatorNames} operatorPks={operatorPks} />
                  <OperatorTable results={compareResults.modified_results} title="Modified" operatorNames={operatorNames} operatorPks={operatorPks} />
                </div>
              </>
            ) : compareResults ? (
              <div className="text-center text-muted-foreground py-8">
                No changes detected between baseline and modified networks
              </div>
            ) : null}
          </div>
        )}

        {/* Link estimate mode */}
        {mode === 'link-estimate' && (
          <div className="space-y-4">
            {baselineNetwork ? (
              <>
                <div className="flex items-center gap-3">
                  <select
                    value={selectedOperator}
                    onChange={(e) => { setSelectedOperator(e.target.value); setLinkResults(null) }}
                    className="px-3 py-2 text-sm bg-background border border-border rounded-lg"
                  >
                    <option value="">Select contributor...</option>
                    {operators.map(o => <option key={o} value={o}>{o}</option>)}
                  </select>
                  <button
                    onClick={() => linkEstimateMutation.mutate()}
                    disabled={networkLoading || linkEstimateMutation.isPending || !selectedOperator}
                    className="flex items-center gap-2 px-4 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50"
                  >
                    {linkEstimateMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Unlink className="h-4 w-4" />}
                    Estimate Link Values
                  </button>
                  {linkEstimateMutation.isPending && (
                    <button
                      onClick={cancelLinkEstimate}
                      className="flex items-center gap-1 px-2 py-1.5 text-xs border border-border rounded hover:bg-muted/50 transition-colors"
                    >
                      <X className="h-3 w-3" /> Cancel
                    </button>
                  )}
                </div>
                {linkEstimateMutation.isPending && (
                  <p className="text-sm text-muted-foreground">
                    Estimating link values ({selectedOperatorLinkCount + 1} simulations, may take a few minutes)... {linkElapsed > 0 && formatElapsed(linkElapsed)}
                  </p>
                )}
                {!linkEstimateMutation.isPending && (
                  <p className="text-xs text-muted-foreground">
                    Link values are approximations. They may not sum exactly to the contributor's total Shapley value.
                  </p>
                )}
              </>
            ) : (
              <p className="text-sm text-muted-foreground">Load a network first to estimate link values.</p>
            )}

            {linkResults && linkResults.length > 0 ? (
              <>
                {selectedOperatorLinkCount > 15 && (
                  <p className="text-xs text-muted-foreground">
                    Values are approximated using leave-one-out method
                  </p>
                )}
                <LinkTable results={linkResults} />
              </>
            ) : linkResults ? (
              <div className="text-center text-muted-foreground py-8">
                No link value results available
              </div>
            ) : null}
          </div>
        )}
      </div>
    </div>
  )
}
