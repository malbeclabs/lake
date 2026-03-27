import { useMemo, useState, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import { Loader2, AlertCircle, Search, ShieldCheck, ChevronDown, ChevronUp, Info } from 'lucide-react'
import { fetchPublisherCheck, type PublisherCheckItem } from '@/lib/api'
import { cn } from '@/lib/utils'
import { PageHeader } from './page-header'
import { Pagination } from './pagination'
import { StatusIcon } from './status-icon'

const PAGE_SIZE = 100

type SortField =
  | 'publishing'
  | 'publisher_ip'
  | 'client_ip'
  | 'dz_user_pubkey'
  | 'vote_pubkey'
  | 'validator_name'
  | 'activated_stake'
  | 'dz_device_code'
  | 'dz_metro_code'
  | 'publishing_leader_shreds'
  | 'publishing_retransmitted'
  | 'leader_slots'
  | 'validator_client'

type SortDirection = 'asc' | 'desc'


function formatStake(lamports: number): string {
  if (lamports === 0) return ''
  const sol = lamports / 1e9
  if (sol >= 1e6) return `${(sol / 1e6).toFixed(2)}M`
  if (sol >= 1e3) return `${(sol / 1e3).toFixed(0)}K`
  return sol.toLocaleString(undefined, { maximumFractionDigits: 0 })
}

function formatStakeExact(lamports: number): string {
  if (lamports === 0) return '0 SOL'
  return `${(lamports / 1e9).toLocaleString(undefined, { maximumFractionDigits: 2 })} SOL`
}

export function PublisherCheckPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [filterInput, setFilterInput] = useState(searchParams.get('q') || '')

  // URL-driven state
  const activeFilter = searchParams.get('q') || ''
  const page = parseInt(searchParams.get('page') || '1')
  const offset = (page - 1) * PAGE_SIZE
  const sortField = (searchParams.get('sort') || 'activated_stake') as SortField
  const sortDirection = (searchParams.get('dir') || 'desc') as SortDirection
  const activeFilters = useMemo(() => {
    const f = searchParams.get('filter')
    if (!f) return new Set<string>()
    return new Set(f.split(',').filter(Boolean))
  }, [searchParams])
  const epochs = parseInt(searchParams.get('epochs') || '2')
  const showBackups = searchParams.get('backups') === 'true'
  const activeTab = (searchParams.get('tab') || 'epoch') as 'epoch' | 'slots'
  const slots = parseInt(searchParams.get('slots') || '500')

  const setOffset = useCallback((newOffset: number) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      const newPage = Math.floor(newOffset / PAGE_SIZE) + 1
      if (newPage <= 1) p.delete('page'); else p.set('page', String(newPage))
      return p
    })
  }, [setSearchParams])

  const handleSort = useCallback((field: SortField) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (p.get('sort') === field) {
        p.set('dir', p.get('dir') === 'asc' ? 'desc' : 'asc')
      } else {
        p.set('sort', field)
        p.set('dir', 'asc')
      }
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  const handleFilterSubmit = useCallback((e: React.FormEvent) => {
    e.preventDefault()
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (filterInput) p.set('q', filterInput); else p.delete('q')
      p.delete('page')
      return p
    })
  }, [filterInput, setSearchParams])

  const handleClearFilter = useCallback(() => {
    setFilterInput('')
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      p.delete('q')
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  const handleToggleFilter = useCallback((filter: string) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      const current = new Set((p.get('filter') || '').split(',').filter(Boolean))
      if (current.has(filter)) current.delete(filter); else current.add(filter)
      if (current.size === 0) p.delete('filter'); else p.set('filter', [...current].join(','))
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  const handleEpochs = useCallback((n: number) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (n === 2) p.delete('epochs'); else p.set('epochs', String(n))
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  const handleToggleBackups = useCallback(() => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (p.get('backups') === 'true') p.delete('backups'); else p.set('backups', 'true')
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  const handleTabChange = useCallback((tab: 'epoch' | 'slots') => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (tab === 'epoch') p.delete('tab'); else p.set('tab', tab)
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  const handleSlots = useCallback((n: number) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (n === 500) p.delete('slots'); else p.set('slots', String(n))
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  const { data, isLoading, error } = useQuery({
    queryKey: ['publisher-check', activeTab, activeTab === 'epoch' ? epochs : slots],
    queryFn: () =>
      activeTab === 'slots'
        ? fetchPublisherCheck(undefined, undefined, slots)
        : fetchPublisherCheck(undefined, epochs),
    refetchInterval: 30000,
  })

  const isPublishing = useCallback((pub: PublisherCheckItem) =>
    pub.publishing_leader_shreds && !pub.publishing_retransmitted,
  [])

  // Sort
  const sortedPublishers = useMemo(() => {
    if (!data?.publishers) return []
    return [...data.publishers].sort((a, b) => {
      let cmp: number
      switch (sortField) {
        case 'publishing': cmp = Number(isPublishing(a)) - Number(isPublishing(b)); break
        case 'publisher_ip': cmp = a.publisher_ip.localeCompare(b.publisher_ip); break
        case 'client_ip': cmp = a.client_ip.localeCompare(b.client_ip); break
        case 'dz_user_pubkey': cmp = a.dz_user_pubkey.localeCompare(b.dz_user_pubkey); break
        case 'vote_pubkey': cmp = a.vote_pubkey.localeCompare(b.vote_pubkey); break
        case 'validator_name': cmp = a.validator_name.localeCompare(b.validator_name); break
        case 'activated_stake': cmp = Number(a.activated_stake) - Number(b.activated_stake); break
        case 'dz_device_code': cmp = a.dz_device_code.localeCompare(b.dz_device_code); break
        case 'dz_metro_code': cmp = a.dz_metro_code.localeCompare(b.dz_metro_code); break
        case 'publishing_leader_shreds': cmp = Number(a.publishing_leader_shreds) - Number(b.publishing_leader_shreds); break
        case 'publishing_retransmitted': cmp = Number(a.publishing_retransmitted) - Number(b.publishing_retransmitted); break
        case 'leader_slots': cmp = a.leader_slots - b.leader_slots; break
        case 'validator_client': cmp = `${a.validator_client} ${a.validator_version}`.localeCompare(`${b.validator_client} ${b.validator_version}`); break
        default: cmp = 0
      }
      return sortDirection === 'asc' ? cmp : -cmp
    })
  }, [data?.publishers, sortField, sortDirection, isPublishing])

  const searchFilteredPublishers = useMemo(() => {
    if (!activeFilter) return sortedPublishers
    const q = activeFilter.toLowerCase()
    return sortedPublishers.filter(pub =>
      pub.publisher_ip.toLowerCase().includes(q) ||
      pub.client_ip.toLowerCase().includes(q) ||
      pub.dz_user_pubkey.toLowerCase().includes(q) ||
      pub.vote_pubkey.toLowerCase().includes(q) ||
      pub.validator_name.toLowerCase().includes(q)
    )
  }, [sortedPublishers, activeFilter])

  const nonBackupPublishers = useMemo(() =>
    showBackups ? searchFilteredPublishers : searchFilteredPublishers.filter(pub => !pub.is_backup),
  [searchFilteredPublishers, showBackups])

  const filteredPublishers = useMemo(() => {
    if (activeFilters.size === 0) return nonBackupPublishers
    return nonBackupPublishers.filter(pub => {
      if (activeFilters.has('retransmit') && pub.publishing_retransmitted) return true
      if (activeFilters.has('leader') && pub.publishing_leader_shreds) return true
      if (activeFilters.has('not-publishing') && !pub.publishing_leader_shreds && !pub.publishing_retransmitted) return true
      return false
    })
  }, [nonBackupPublishers, activeFilters])

  const publishingCount = useMemo(() =>
    nonBackupPublishers.filter(p => p.publishing_leader_shreds).length,
  [nonBackupPublishers])

  const publishingStake = useMemo(() =>
    nonBackupPublishers.filter(p => p.publishing_leader_shreds).reduce((sum, p) => sum + p.activated_stake, 0),
  [nonBackupPublishers])

  const totalNetworkStake = data?.total_network_stake ?? 0

  const formatStakePct = (stake: number) => {
    if (!totalNetworkStake) return formatStake(stake)
    const pct = (stake / totalNetworkStake) * 100
    return `${pct.toFixed(1)}%`
  }

  const pagedPublishers = useMemo(
    () => filteredPublishers.slice(offset, offset + PAGE_SIZE),
    [filteredPublishers, offset]
  )

  const SortIcon = ({ field }: { field: string }) => {
    if (sortField !== field) return null
    return sortDirection === 'asc'
      ? <ChevronUp className="inline h-3 w-3 ml-0.5" />
      : <ChevronDown className="inline h-3 w-3 ml-0.5" />
  }

  const thClass = 'px-4 py-3 font-medium cursor-pointer select-none hover:text-foreground transition-colors'
  const thCenter = thClass + ' text-center'

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Unable to load publisher data</div>
          <div className="text-sm text-muted-foreground">{error?.message || 'Unknown error'}</div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={ShieldCheck}
          title="Publisher Check"
          subtitle={
            activeTab === 'slots'
              ? data?.max_slot
                ? `Last ${slots.toLocaleString()} slots (up to #${data.max_slot.toLocaleString()})`
                : undefined
              : data?.epoch
                ? `Epoch ${data.epoch}`
                : undefined
          }
          actions={
            <form onSubmit={handleFilterSubmit} className="flex items-center gap-2">
              <div className="relative">
                <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                <input
                  type="text"
                  placeholder="Search by name, Vote ID, IP, or DZ ID..."
                  value={filterInput}
                  onChange={(e) => setFilterInput(e.target.value)}
                  className="pl-8 pr-3 py-1.5 text-sm border border-border rounded-md bg-background w-64 focus:outline-none focus:ring-1 focus:ring-accent"
                />
              </div>
              <button
                type="submit"
                className="px-3 py-1.5 text-sm bg-accent text-accent-foreground rounded-md hover:bg-accent/90"
              >
                Search
              </button>
              {activeFilter && (
                <button
                  type="button"
                  onClick={handleClearFilter}
                  className="px-3 py-1.5 text-sm border border-border rounded-md hover:bg-muted"
                >
                  Clear
                </button>
              )}
            </form>
          }
        />

        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-4">
            <div className="rounded-md bg-muted px-3 py-1.5 text-sm">
              <span className="text-muted-foreground">Total Publishers</span>{' '}
              <span className="font-medium">{data?.total_publishers ?? 0}</span>
              {(data?.total_publisher_stake ?? 0) > 0 && (
                <span className="ml-1.5 text-muted-foreground" title={formatStakeExact(data?.total_publisher_stake ?? 0)}>
                  ({formatStakePct(data?.total_publisher_stake ?? 0)} of stake)
                </span>
              )}
            </div>
            <div className="rounded-md bg-muted px-3 py-1.5 text-sm">
              <span className="text-muted-foreground">Publishing Shreds</span>{' '}
              <span className="font-medium">{publishingCount}</span>
              {publishingStake > 0 && (
                <span className="ml-1.5 text-muted-foreground" title={formatStakeExact(publishingStake)}>
                  ({formatStakePct(publishingStake)} of stake)
                </span>
              )}
            </div>
          </div>
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2">
              <div className="flex items-center rounded-md border border-border text-sm">
                {([
                  ['epoch', 'Epoch'],
                  ['slots', 'Recent Slots'],
                ] as const).map(([value, label]) => (
                  <button
                    key={value}
                    type="button"
                    onClick={() => handleTabChange(value)}
                    className={cn(
                      'px-3 py-1.5 transition-colors',
                      activeTab === value
                        ? 'bg-accent text-accent-foreground'
                        : 'hover:bg-muted'
                    )}
                  >
                    {label}
                  </button>
                ))}
              </div>
              {activeTab === 'epoch' ? (
                <div className="flex items-center rounded-md border border-border text-sm">
                  {([
                    [1, 'Current Epoch'],
                    [2, 'Last 2 Epochs'],
                    [5, 'Last 5 Epochs'],
                  ] as const).map(([n, label]) => (
                    <button
                      key={n}
                      type="button"
                      onClick={() => handleEpochs(n)}
                      className={cn(
                        'px-3 py-1.5 transition-colors',
                        epochs === n
                          ? 'bg-accent text-accent-foreground'
                          : 'hover:bg-muted'
                      )}
                    >
                      {label}
                    </button>
                  ))}
                </div>
              ) : (
                <div className="flex items-center rounded-md border border-border text-sm">
                  {([100, 500, 1000, 5000] as const).map((n) => (
                    <button
                      key={n}
                      type="button"
                      onClick={() => handleSlots(n)}
                      className={cn(
                        'px-3 py-1.5 transition-colors',
                        slots === n
                          ? 'bg-accent text-accent-foreground'
                          : 'hover:bg-muted'
                      )}
                    >
                      {n.toLocaleString()}
                    </button>
                  ))}
                </div>
              )}
            </div>
            <div className="flex items-center gap-2">
              {([
                ['retransmit', 'Retransmitting', 'bg-red-500/10 border-red-500/50 text-red-400'],
                ['leader', 'Publishing', ''],
                ['not-publishing', 'Not Publishing', ''],
              ] as const).map(([value, label, activeClass]) => (
                <button
                  key={value}
                  type="button"
                  onClick={() => handleToggleFilter(value)}
                  className={cn(
                    'px-3 py-1.5 text-sm rounded-full border transition-colors',
                    activeFilters.has(value)
                      ? activeClass || 'bg-accent text-accent-foreground border-accent'
                      : 'border-border text-muted-foreground hover:bg-muted'
                  )}
                >
                  {label}
                </button>
              ))}
            </div>
            <button
              type="button"
              onClick={handleToggleBackups}
              className={cn(
                'px-3 py-1.5 text-sm rounded-md border transition-colors',
                showBackups
                  ? 'bg-accent text-accent-foreground border-accent'
                  : 'border-border hover:bg-muted'
              )}
            >
              Show Backups
            </button>
          </div>
        </div>

        <div className="mb-6 rounded-lg bg-muted/50 px-4 py-3 text-sm text-muted-foreground">
          <div className="flex items-start gap-2">
            <Info className="h-4 w-4 mt-0.5 shrink-0" />
            <ul className="space-y-1">
              <li><span className="font-medium text-foreground">Publishing Leader Shreds</span> — Leader shreds have been sent by the validator in the selected epoch range.</li>
              <li><span className="font-medium text-foreground">No Retransmit Shreds</span> — No retransmit shreds have been sent by the validator. Retransmit shreds are undesirable.</li>
            </ul>
          </div>
        </div>

        <div className="border border-border rounded-lg overflow-hidden bg-card">
          <div className="overflow-x-auto">
            <table className="min-w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <th className={thCenter} onClick={() => handleSort('publishing')}>
                    Healthy<SortIcon field="publishing" />
                  </th>
                  <th className={thClass} onClick={() => handleSort('publisher_ip')}>
                    Publisher IP<SortIcon field="publisher_ip" />
                  </th>
                  <th className={thClass} onClick={() => handleSort('client_ip')}>
                    Client IP<SortIcon field="client_ip" />
                  </th>
                  <th className={thClass} onClick={() => handleSort('dz_user_pubkey')}>
                    DZ ID<SortIcon field="dz_user_pubkey" />
                  </th>
                  <th className={thClass} onClick={() => handleSort('vote_pubkey')}>
                    Vote ID<SortIcon field="vote_pubkey" />
                  </th>
                  <th className={thClass} onClick={() => handleSort('validator_name')}>
                    Name<SortIcon field="validator_name" />
                  </th>
                  <th className={thClass} onClick={() => handleSort('activated_stake')}>
                    Stake<SortIcon field="activated_stake" />
                  </th>
                  <th className={thClass} onClick={() => handleSort('dz_device_code')}>
                    Device<SortIcon field="dz_device_code" />
                  </th>
                  <th className={thClass} onClick={() => handleSort('dz_metro_code')}>
                    Metro<SortIcon field="dz_metro_code" />
                  </th>
                  <th className={thCenter} onClick={() => handleSort('publishing_leader_shreds')}>
                    Publishing Leader Shreds<SortIcon field="publishing_leader_shreds" />
                  </th>
                  <th className={thCenter} onClick={() => handleSort('publishing_retransmitted')}>
                    No Retransmit Shreds<SortIcon field="publishing_retransmitted" />
                  </th>
                  <th className={thCenter} onClick={() => handleSort('leader_slots')}>
                    Leader Slots<SortIcon field="leader_slots" />
                  </th>
                  <th className={thClass} onClick={() => handleSort('validator_client')}>
                    Validator Client<SortIcon field="validator_client" />
                  </th>
                </tr>
              </thead>
              <tbody>
                {pagedPublishers.length === 0 ? (
                  <tr>
                    <td colSpan={13} className="px-4 py-12 text-center text-muted-foreground">
                      {activeFilter ? 'No publishers found for this filter' : 'No publishers found'}
                    </td>
                  </tr>
                ) : (
                  pagedPublishers.map((pub) => (
                    <tr
                      key={`${pub.publisher_ip}-${pub.dz_user_pubkey}`}
                      className="border-b border-border last:border-b-0 hover:bg-muted transition-colors"
                    >
                      <td className="px-4 py-3 text-center">
                        {pub.is_backup
                          ? <span className="inline-block rounded bg-muted px-1.5 py-0.5 text-xs text-muted-foreground">Backup</span>
                          : <StatusIcon ok={isPublishing(pub)} />}
                      </td>
                      <td className="px-4 py-3 font-mono text-sm">{pub.publisher_ip}</td>
                      <td className="px-4 py-3 font-mono text-sm">{pub.client_ip}</td>
                      <td className="px-4 py-3 font-mono text-sm">
                        {pub.dz_user_pubkey ? (
                          <button
                            type="button"
                            className="hover:text-foreground transition-colors cursor-pointer"
                            title="Click to copy"
                            onClick={() => navigator.clipboard.writeText(pub.dz_user_pubkey)}
                          >
                            {pub.dz_user_pubkey.slice(0, 12)}...
                          </button>
                        ) : '—'}
                      </td>
                      <td className="px-4 py-3 font-mono text-sm">
                        {pub.vote_pubkey ? (
                          <button
                            type="button"
                            className="hover:text-foreground transition-colors cursor-pointer"
                            title="Click to copy"
                            onClick={() => navigator.clipboard.writeText(pub.vote_pubkey)}
                          >
                            {pub.vote_pubkey.slice(0, 12)}...
                          </button>
                        ) : '—'}
                      </td>
                      <td className="px-4 py-3 text-sm max-w-[200px] truncate" title={pub.validator_name}>
                        {pub.validator_name || '\u2014'}
                      </td>
                      <td className="px-4 py-3 text-sm tabular-nums" title={formatStakeExact(pub.activated_stake)}>
                        {pub.activated_stake ? formatStake(pub.activated_stake) : '\u2014'}
                      </td>
                      <td className="px-4 py-3 text-sm">{pub.dz_device_code || '—'}</td>
                      <td className="px-4 py-3 text-sm">{pub.dz_metro_code || '—'}</td>
                      <td className="px-4 py-3 text-center">
                        <StatusIcon ok={pub.publishing_leader_shreds} />
                      </td>
                      <td className="px-4 py-3 text-center">
                        <StatusIcon ok={!pub.publishing_retransmitted} />
                      </td>
                      <td className="px-4 py-3 text-sm tabular-nums text-center">
                        {pub.leader_slots.toLocaleString()}
                      </td>
                      <td className="px-4 py-3 text-sm">
                        {pub.validator_client} {pub.validator_version || '?'}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
          {filteredPublishers.length > PAGE_SIZE && (
            <Pagination
              total={filteredPublishers.length}
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
