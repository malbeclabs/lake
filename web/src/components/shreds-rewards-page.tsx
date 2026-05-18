import { useCallback, useMemo, useState } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useSearchParams, Link, useNavigate } from 'react-router-dom'
import {
  Loader2,
  AlertCircle,
  Search,
  X,
  ChevronUp,
  ChevronDown,
  Trophy,
} from 'lucide-react'
import {
  fetchShredsRewards,
  type ShredsRewardsParams,
  type ShredsRewardsRow,
} from '@/lib/api'
import { cn, handleRowClick } from '@/lib/utils'
import { PageHeader } from './page-header'
import { Pagination } from './pagination'
import { format2Z } from './shreds-rewards-format'

const PAGE_SIZE = 100

type SortField =
  | 'validator_name'
  | 'activated_stake'
  | 'total_earned_2z'
  | 'immediately_claimable_2z'
type SortDirection = 'asc' | 'desc'

const SORT_FIELDS: ReadonlySet<string> = new Set<SortField>([
  'validator_name',
  'activated_stake',
  'total_earned_2z',
  'immediately_claimable_2z',
])

function truncatePK(pk: string): string {
  if (!pk) return ''
  if (pk.length <= 12) return pk
  return `${pk.slice(0, 6)}...${pk.slice(-4)}`
}

function formatStake(lamports: number): string {
  if (!lamports || lamports <= 0) return '—'
  const sol = lamports / 1e9
  if (sol >= 1e6) return `${(sol / 1e6).toFixed(2)}M`
  if (sol >= 1e3) return `${(sol / 1e3).toFixed(0)}K`
  return sol.toLocaleString(undefined, { maximumFractionDigits: 0 })
}

function formatStakeExact(lamports: number): string {
  if (!lamports || lamports <= 0) return '0 SOL'
  return `${(lamports / 1e9).toLocaleString(undefined, { maximumFractionDigits: 2 })} SOL`
}


function parseSearchFilters(searchParam: string): string[] {
  if (!searchParam) return []
  return searchParam
    .split(',')
    .map((f) => f.trim())
    .filter(Boolean)
}

function CopyableTrunc({ value }: { value: string }) {
  if (!value) return <>—</>
  return (
    <button
      type="button"
      className="font-mono hover:text-foreground transition-colors cursor-pointer"
      title={`Click to copy ${value}`}
      onClick={(e) => {
        e.stopPropagation()
        navigator.clipboard.writeText(value)
      }}
    >
      {truncatePK(value)}
    </button>
  )
}

export function ShredsRewardsPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const navigate = useNavigate()

  const page = Math.max(1, parseInt(searchParams.get('page') || '1') || 1)
  const offset = (page - 1) * PAGE_SIZE
  const rawSort = searchParams.get('sort') || 'total_earned_2z'
  const sortField: SortField = (SORT_FIELDS.has(rawSort) ? rawSort : 'total_earned_2z') as SortField
  const sortDirection: SortDirection =
    searchParams.get('order') === 'asc' ? 'asc' : 'desc'

  const searchParam = searchParams.get('search') || ''
  const [filterInput, setFilterInput] = useState(searchParam)

  const searchFilters = useMemo(() => parseSearchFilters(searchParam), [searchParam])

  const queryParams: ShredsRewardsParams = useMemo(
    () => ({
      search: searchParam || undefined,
      sort: sortField,
      order: sortDirection,
      limit: PAGE_SIZE,
      offset,
    }),
    [searchParam, sortField, sortDirection, offset],
  )

  const { data, isLoading, error } = useQuery({
    queryKey: ['shreds-rewards', queryParams],
    queryFn: () => fetchShredsRewards(queryParams),
    placeholderData: keepPreviousData,
    refetchInterval: 60_000,
  })

  const handleSort = useCallback(
    (field: SortField) => {
      setSearchParams((prev) => {
        const p = new URLSearchParams(prev)
        if (p.get('sort') === field || (!p.get('sort') && field === 'total_earned_2z')) {
          const nextOrder = (p.get('order') || 'desc') === 'asc' ? 'desc' : 'asc'
          if (nextOrder === 'desc') p.delete('order')
          else p.set('order', nextOrder)
          if (field === 'total_earned_2z') p.delete('sort')
          else p.set('sort', field)
        } else {
          if (field === 'total_earned_2z') p.delete('sort')
          else p.set('sort', field)
          p.delete('order')
        }
        p.delete('page')
        return p
      })
    },
    [setSearchParams],
  )

  const setOffset = useCallback(
    (newOffset: number) => {
      setSearchParams((prev) => {
        const p = new URLSearchParams(prev)
        const newPage = Math.floor(newOffset / PAGE_SIZE) + 1
        if (newPage <= 1) p.delete('page')
        else p.set('page', String(newPage))
        return p
      })
    },
    [setSearchParams],
  )

  const handleSearchSubmit = useCallback(
    (e: React.FormEvent) => {
      e.preventDefault()
      setSearchParams((prev) => {
        const p = new URLSearchParams(prev)
        if (filterInput.trim()) p.set('search', filterInput.trim())
        else p.delete('search')
        p.delete('page')
        return p
      })
    },
    [filterInput, setSearchParams],
  )

  const handleClearSearch = useCallback(() => {
    setFilterInput('')
    setSearchParams((prev) => {
      const p = new URLSearchParams(prev)
      p.delete('search')
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  const SortIcon = ({ field }: { field: SortField }) => {
    if (sortField !== field) return null
    return sortDirection === 'asc' ? (
      <ChevronUp className="inline h-3 w-3 ml-0.5" />
    ) : (
      <ChevronDown className="inline h-3 w-3 ml-0.5" />
    )
  }

  const validators = data?.validators ?? []
  const epochColumns = data?.epoch_columns ?? []
  // We don't know total count from the API; derive whether there's another page by
  // checking whether the response is a full page.
  const hasMore = validators.length === PAGE_SIZE
  const fauxTotal = hasMore ? offset + PAGE_SIZE + 1 : offset + validators.length

  const thClass =
    'px-4 py-3 font-medium cursor-pointer select-none hover:text-foreground transition-colors whitespace-nowrap'
  const thStatic = 'px-4 py-3 font-medium whitespace-nowrap'
  const thRight = `${thClass} text-right`
  const thStaticRight = `${thStatic} text-right`

  if (isLoading && !data) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error && !data) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Unable to load rewards data</div>
          <div className="text-sm text-muted-foreground">
            {(error as Error)?.message || 'Unknown error'}
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Trophy}
          title="Edge Rewards"
          subtitle={
            data?.current_solana_epoch ? (
              <span className="text-sm text-muted-foreground">
                Epoch {data.current_solana_epoch}
                {data.latest_finalized_epoch != null && (
                  <span className="text-muted-foreground/50">
                    {' '}
                    · last finalized {data.latest_finalized_epoch}
                  </span>
                )}
              </span>
            ) : undefined
          }
          actions={
            <form
              onSubmit={handleSearchSubmit}
              className="w-full sm:w-96 flex items-stretch gap-2"
            >
              <div className="relative flex-1">
                <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                <input
                  type="text"
                  placeholder="Search by name, vote: or node:"
                  value={filterInput}
                  onChange={(e) => setFilterInput(e.target.value)}
                  className="w-full pl-8 pr-3 py-1.5 text-sm border border-border rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-accent"
                />
              </div>
              <button
                type="submit"
                className="shrink-0 px-2 flex items-center bg-accent text-accent-foreground rounded-md hover:bg-accent/90"
                title="Search"
              >
                <Search className="h-4 w-4" />
              </button>
              {searchFilters.length > 0 && (
                <button
                  type="button"
                  onClick={handleClearSearch}
                  className="shrink-0 px-2 flex items-center border border-border rounded-md hover:bg-muted"
                  title="Clear"
                >
                  <X className="h-4 w-4" />
                </button>
              )}
            </form>
          }
        />

        <div className="mb-4 rounded-lg bg-muted/50 px-4 py-3 text-xs xxs:text-sm text-muted-foreground">
          Per-epoch $2Z earnings for validators publishing shreds via DoubleZero. The
          newest epoch is rightmost. Claimable rewards are those that have not yet been
          paid out by the on-chain claim journal.
        </div>

        <div className="border border-border rounded-lg overflow-hidden bg-card">
          <div className="overflow-x-auto">
            <table className="min-w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <th
                    className={cn(thClass, 'sticky left-0 bg-card z-10')}
                    onClick={() => handleSort('validator_name')}
                  >
                    Validator
                    <SortIcon field="validator_name" />
                  </th>
                  <th className={thStatic}>Vote</th>
                  <th
                    className={thRight}
                    onClick={() => handleSort('activated_stake')}
                  >
                    Stake
                    <SortIcon field="activated_stake" />
                  </th>
                  <th className={thStatic}>DZ IP</th>
                  <th
                    className={thRight}
                    onClick={() => handleSort('total_earned_2z')}
                  >
                    All-time
                    <SortIcon field="total_earned_2z" />
                  </th>
                  <th
                    className={thRight}
                    onClick={() => handleSort('immediately_claimable_2z')}
                  >
                    Claimable
                    <SortIcon field="immediately_claimable_2z" />
                  </th>
                  {epochColumns.map((ep) => (
                    <th key={ep} className={thStaticRight}>
                      <span className="text-[10px] uppercase tracking-wider text-muted-foreground/70 block">
                        Epoch
                      </span>
                      <span className="tabular-nums">{ep}</span>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {validators.length === 0 ? (
                  <tr>
                    <td
                      colSpan={6 + epochColumns.length}
                      className="px-4 py-12 text-center text-muted-foreground"
                    >
                      {searchFilters.length > 0
                        ? 'No validators match this search'
                        : 'No validators have earned rewards yet'}
                    </td>
                  </tr>
                ) : (
                  validators.map((v: ShredsRewardsRow) => {
                    const url = `/dz/shreds/rewards/${encodeURIComponent(v.node_id)}`
                    const displayName =
                      v.validator_name?.trim() || truncatePK(v.node_id)
                    return (
                      <tr
                        key={v.node_id}
                        onClick={(e) => handleRowClick(e, url, navigate)}
                        className="border-b border-border last:border-b-0 hover:bg-muted/40 transition-colors cursor-pointer"
                      >
                        <td
                          className="sticky left-0 bg-card px-4 py-3 text-sm max-w-[240px] truncate"
                          title={v.validator_name || v.node_id}
                        >
                          <Link
                            to={url}
                            className="hover:text-foreground transition-colors font-medium"
                            onClick={(e) => e.stopPropagation()}
                          >
                            {displayName}
                          </Link>
                        </td>
                        <td className="px-4 py-3 text-sm">
                          <CopyableTrunc value={v.vote_pubkey} />
                        </td>
                        <td
                          className="px-4 py-3 text-sm tabular-nums text-right"
                          title={formatStakeExact(v.activated_stake)}
                        >
                          {formatStake(v.activated_stake)}
                        </td>
                        <td className="px-4 py-3 text-sm font-mono">
                          {v.dz_user_ip || (
                            <span className="text-muted-foreground/50">—</span>
                          )}
                        </td>
                        <td className="px-4 py-3 text-sm tabular-nums text-right">
                          {format2Z(v.total_earned_2z)}
                        </td>
                        <td
                          className={cn(
                            'px-4 py-3 text-sm tabular-nums text-right',
                            v.immediately_claimable_2z > 0
                              ? 'text-amber-500 dark:text-amber-400 font-medium'
                              : 'text-muted-foreground/50',
                          )}
                        >
                          {format2Z(v.immediately_claimable_2z)}
                        </td>
                        {epochColumns.map((ep) => {
                          const amt = v.epoch_earnings?.[String(ep)] ?? 0
                          return (
                            <td
                              key={ep}
                              className={cn(
                                'px-4 py-3 text-sm tabular-nums text-right',
                                amt > 0 ? '' : 'text-muted-foreground/40',
                              )}
                            >
                              {format2Z(amt)}
                            </td>
                          )
                        })}
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>
          {(hasMore || offset > 0) && (
            <Pagination
              total={fauxTotal}
              limit={PAGE_SIZE}
              offset={offset}
              onOffsetChange={setOffset}
            />
          )}
        </div>
      </div>
    </div>
  )
}
