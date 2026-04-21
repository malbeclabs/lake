import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Loader2, MapPin, AlertCircle, ArrowLeft, Info, ChevronUp, ChevronDown } from 'lucide-react'
import { fetchMetro, fetchDevicesByMetro } from '@/lib/api'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { useBackLink } from '@/hooks/use-back-link'
import { handleRowClick } from '@/lib/utils'
import { useState, useMemo } from 'react'

type DeviceSortField = 'code' | 'type' | 'contributor' | 'status' | 'users' | 'unicast' | 'subscribers' | 'publishers' | 'in' | 'out'
type SortDir = 'asc' | 'desc'

function formatBps(bps: number): string {
  if (bps === 0) return '—'
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(1)} Tbps`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(1)} Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)} Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(1)} Kbps`
  return `${bps.toFixed(0)} bps`
}

function formatStake(sol: number): string {
  if (sol === 0) return '—'
  if (sol >= 1e6) return `${(sol / 1e6).toFixed(2)}M SOL`
  if (sol >= 1e3) return `${(sol / 1e3).toFixed(1)}K SOL`
  return `${sol.toFixed(0)} SOL`
}

const statusColors: Record<string, string> = {
  active: 'text-green-600 dark:text-green-400',
  activated: 'text-green-600 dark:text-green-400',
  drained: 'text-amber-500',
  'soft-drained': 'text-amber-600 dark:text-amber-400',
  inactive: 'text-muted-foreground',
  deactivated: 'text-muted-foreground',
}

export function MetroDetailPage() {
  const { pk } = useParams<{ pk: string }>()
  const navigate = useNavigate()
  const back = useBackLink({ to: '/dz/metros', label: 'metros' })
  const [sortField, setSortField] = useState<DeviceSortField>('code')
  const [sortDir, setSortDir] = useState<SortDir>('asc')

  const { data: metro, isLoading, error } = useQuery({
    queryKey: ['metro', pk],
    queryFn: () => fetchMetro(pk!),
    enabled: !!pk,
  })

  const { data: devicesData } = useQuery({
    queryKey: ['metro-devices', pk],
    queryFn: () => fetchDevicesByMetro(pk!),
    enabled: !!pk,
  })

  useDocumentTitle(metro?.code || metro?.name || 'Metro')

  const rawDevices = devicesData?.items ?? []
  const devices = useMemo(() => {
    return [...rawDevices].sort((a, b) => {
      let cmp = 0
      switch (sortField) {
        case 'code': cmp = a.code.localeCompare(b.code); break
        case 'type': cmp = (a.device_type || '').localeCompare(b.device_type || ''); break
        case 'contributor': cmp = (a.contributor_code || '').localeCompare(b.contributor_code || ''); break
        case 'status': cmp = a.status.localeCompare(b.status); break
        case 'users': {
          if (!a.max_users && !b.max_users) break
          if (!a.max_users) return 1
          if (!b.max_users) return -1
          cmp = a.current_users / a.max_users - b.current_users / b.max_users
          break
        }
        case 'unicast': {
          const effA = Math.max(a.unicast_users, a.max_unicast_users > 0 ? a.max_unicast_users : Math.max(0, a.max_users - a.max_multicast_subscribers - a.max_multicast_publishers))
          const effB = Math.max(b.unicast_users, b.max_unicast_users > 0 ? b.max_unicast_users : Math.max(0, b.max_users - b.max_multicast_subscribers - b.max_multicast_publishers))
          if (!effA && !effB) break
          if (!effA) return 1
          if (!effB) return -1
          cmp = a.unicast_users / effA - b.unicast_users / effB
          break
        }
        case 'subscribers': {
          const effA = Math.max(a.multicast_subscribers_count, a.max_multicast_subscribers > 0 ? a.max_multicast_subscribers : Math.max(0, a.max_users - a.max_unicast_users - a.max_multicast_publishers))
          const effB = Math.max(b.multicast_subscribers_count, b.max_multicast_subscribers > 0 ? b.max_multicast_subscribers : Math.max(0, b.max_users - b.max_unicast_users - b.max_multicast_publishers))
          if (!effA && !effB) break
          if (!effA) return 1
          if (!effB) return -1
          cmp = a.multicast_subscribers_count / effA - b.multicast_subscribers_count / effB
          break
        }
        case 'publishers': {
          const effA = Math.max(a.multicast_publishers_count, a.max_multicast_publishers > 0 ? a.max_multicast_publishers : Math.max(0, a.max_users - a.max_unicast_users - a.max_multicast_subscribers))
          const effB = Math.max(b.multicast_publishers_count, b.max_multicast_publishers > 0 ? b.max_multicast_publishers : Math.max(0, b.max_users - b.max_unicast_users - b.max_multicast_subscribers))
          if (!effA && !effB) break
          if (!effA) return 1
          if (!effB) return -1
          cmp = a.multicast_publishers_count / effA - b.multicast_publishers_count / effB
          break
        }
        case 'in': cmp = a.in_bps - b.in_bps; break
        case 'out': cmp = a.out_bps - b.out_bps; break
      }
      return sortDir === 'asc' ? cmp : -cmp
    })
  }, [rawDevices, sortField, sortDir])

  const handleDeviceSort = (field: DeviceSortField) => {
    if (sortField === field) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortField(field); setSortDir('asc') }
  }
  const SortIcon = ({ field }: { field: DeviceSortField }) => {
    if (sortField !== field) return null
    return sortDir === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />
  }

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error || !metro) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Metro not found</div>
          <button
            onClick={() => navigate(back.to)}
            className="text-sm text-muted-foreground hover:text-foreground"
          >
            Back to {back.label}
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 py-8">
        {/* Back button */}
        <button
          onClick={() => navigate(back.to)}
          className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground mb-6"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to {back.label}
        </button>

        {/* Header */}
        <div className="flex items-center gap-3 mb-8">
          <MapPin className="h-8 w-8 text-muted-foreground" />
          <div>
            <h1 className="text-2xl font-medium">{metro.name || metro.code}</h1>
            <div className="text-sm text-muted-foreground font-mono">{metro.code}</div>
          </div>
        </div>

        {/* Info grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-10">
          {/* Location */}
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Location</h3>
            <dl className="space-y-2">
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Latitude</dt>
                <dd className="text-sm font-mono">{metro.latitude.toFixed(4)}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Longitude</dt>
                <dd className="text-sm font-mono">{metro.longitude.toFixed(4)}</dd>
              </div>
            </dl>
          </div>

          {/* Infrastructure */}
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Infrastructure</h3>
            <dl className="space-y-2">
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Devices</dt>
                <dd className="text-sm">{metro.device_count}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Users</dt>
                <dd className="text-sm">{metro.user_count}</dd>
              </div>
              {(() => {
                const effUnicast = Math.max(metro.unicast_users_count, metro.max_unicast_users)
                const effSubs = Math.max(metro.multicast_subscribers_count, metro.max_multicast_subscribers)
                const effPubs = Math.max(metro.multicast_publishers_count, metro.max_multicast_publishers)
                const isDerivedUnicast = metro.raw_max_unicast_users === 0 && effUnicast > 0
                const isDerivedSubs = metro.raw_max_multicast_subscribers === 0 && effSubs > 0
                const isDerivedPubs = metro.raw_max_multicast_publishers === 0 && effPubs > 0
                return (
                  <>
                    <div className="flex justify-between">
                      <dt className="text-sm text-muted-foreground">Unicast</dt>
                      <dd className="text-sm tabular-nums">
                        {metro.unicast_users_count}{effUnicast > 0 && <> / {isDerivedUnicast
                          ? <span className="text-muted-foreground/50 inline-flex items-center gap-0.5" title="Calculated from max_users">{effUnicast}<Info className="h-2.5 w-2.5" /></span>
                          : <span className="text-muted-foreground">{effUnicast}</span>
                        }</>}
                      </dd>
                    </div>
                    <div className="flex justify-between">
                      <dt className="text-sm text-muted-foreground">Subscribers</dt>
                      <dd className="text-sm tabular-nums">
                        {metro.multicast_subscribers_count}{effSubs > 0 && <> / {isDerivedSubs
                          ? <span className="text-muted-foreground/50 inline-flex items-center gap-0.5" title="Calculated from max_users">{effSubs}<Info className="h-2.5 w-2.5" /></span>
                          : <span className="text-muted-foreground">{effSubs}</span>
                        }</>}
                      </dd>
                    </div>
                    <div className="flex justify-between">
                      <dt className="text-sm text-muted-foreground">Publishers</dt>
                      <dd className="text-sm tabular-nums">
                        {metro.multicast_publishers_count}{effPubs > 0 && <> / {isDerivedPubs
                          ? <span className="text-muted-foreground/50 inline-flex items-center gap-0.5" title="Calculated from max_users">{effPubs}<Info className="h-2.5 w-2.5" /></span>
                          : <span className="text-muted-foreground">{effPubs}</span>
                        }</>}
                      </dd>
                    </div>
                  </>
                )
              })()}
            </dl>
          </div>

          {/* Traffic */}
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Traffic</h3>
            <dl className="space-y-2">
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Inbound</dt>
                <dd className="text-sm">{formatBps(metro.in_bps)}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Outbound</dt>
                <dd className="text-sm">{formatBps(metro.out_bps)}</dd>
              </div>
            </dl>
          </div>

          {/* Validators */}
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Validators</h3>
            <dl className="space-y-2">
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Count</dt>
                <dd className="text-sm">{metro.validator_count}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Total Stake</dt>
                <dd className="text-sm">{formatStake(metro.stake_sol)}</dd>
              </div>
            </dl>
          </div>
        </div>

        {/* Devices table */}
        {devices.length > 0 && (
          <div>
            <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider mb-3">
              Devices ({devices.length}{devicesData && devicesData.total > rawDevices.length ? ` of ${devicesData.total}` : ''})
            </h2>
            <div className="border border-border rounded-lg overflow-hidden">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border bg-muted/30 text-xs text-muted-foreground uppercase tracking-wider">
                      {(['code', 'type', 'contributor', 'status'] as DeviceSortField[]).map(f => (
                        <th key={f} className="px-4 py-3 font-medium text-left">
                          <button className="inline-flex items-center gap-1" type="button" onClick={() => handleDeviceSort(f)}>
                            {f.charAt(0).toUpperCase() + f.slice(1)}
                            <SortIcon field={f} />
                          </button>
                        </th>
                      ))}
                      {(['users', 'unicast', 'subscribers', 'publishers', 'in', 'out'] as DeviceSortField[]).map(f => (
                        <th key={f} className="px-4 py-3 font-medium text-right">
                          <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleDeviceSort(f)}>
                            {f.charAt(0).toUpperCase() + f.slice(1)}
                            <SortIcon field={f} />
                          </button>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {devices.map((device) => {
                      const effUnicast = Math.max(device.unicast_users, device.max_unicast_users > 0 ? device.max_unicast_users : Math.max(0, device.max_users - device.max_multicast_subscribers - device.max_multicast_publishers))
                      const effSubs = Math.max(device.multicast_subscribers_count, device.max_multicast_subscribers > 0 ? device.max_multicast_subscribers : Math.max(0, device.max_users - device.max_unicast_users - device.max_multicast_publishers))
                      const effPubs = Math.max(device.multicast_publishers_count, device.max_multicast_publishers > 0 ? device.max_multicast_publishers : Math.max(0, device.max_users - device.max_unicast_users - device.max_multicast_subscribers))
                      const derivedFlags = [device.max_unicast_users === 0, device.max_multicast_subscribers === 0, device.max_multicast_publishers === 0]
                      return (
                        <tr
                          key={device.pk}
                          className="border-b border-border last:border-b-0 hover:bg-muted cursor-pointer transition-colors"
                          onClick={(e) => handleRowClick(e, `/dz/devices/${device.pk}`, navigate)}
                        >
                          <td className="px-4 py-3">
                            <span className="font-mono text-sm">{device.code}</span>
                          </td>
                          <td className="px-4 py-3 text-sm text-muted-foreground capitalize">
                            {device.device_type?.replace(/_/g, ' ')}
                          </td>
                          <td className="px-4 py-3 text-sm text-muted-foreground">
                            {device.contributor_code || '—'}
                          </td>
                          <td className={`px-4 py-3 text-sm capitalize ${statusColors[device.status] || ''}`}>
                            {device.status}
                          </td>
                          <td className="px-4 py-3 text-sm tabular-nums text-right relative">
                            {(() => {
                              const pct = device.max_users > 0 ? Math.min(100, (device.current_users / device.max_users) * 100) : 0
                              const fillColor = pct >= 90 ? 'bg-red-500/25' : pct >= 70 ? 'bg-amber-500/20' : 'bg-blue-500/15'
                              return (
                                <>
                                  {device.max_users > 0 && <div className="absolute inset-y-0 left-0 right-0 pointer-events-none bg-muted/30 border-r border-muted-foreground/20" />}
                                  {pct > 0 && <div className={`absolute inset-y-0 left-0 pointer-events-none ${fillColor}`} style={{ width: `${pct}%` }} />}
                                  <span className="relative">
                                    {device.current_users > 0 || device.max_users > 0 ? (
                                      <>{device.current_users}{device.max_users > 0 && <span className="text-muted-foreground">/{device.max_users}</span>}</>
                                    ) : <span className="text-muted-foreground">—</span>}
                                  </span>
                                </>
                              )
                            })()}
                          </td>
                          {[
                            { count: device.unicast_users, effectiveMax: effUnicast },
                            { count: device.multicast_subscribers_count, effectiveMax: effSubs },
                            { count: device.multicast_publishers_count, effectiveMax: effPubs },
                          ].map(({ count, effectiveMax }, i) => {
                            const isDerived = derivedFlags[i]
                            const pct = effectiveMax > 0 ? Math.min(100, (count / effectiveMax) * 100) : 0
                            const fillColor = pct >= 90 ? 'bg-red-500/25' : pct >= 70 ? 'bg-amber-500/20' : 'bg-blue-500/15'
                            return (
                              <td key={i} className="px-4 py-3 text-sm tabular-nums text-right relative">
                                {effectiveMax > 0 && <div className="absolute inset-y-0 left-0 right-0 pointer-events-none bg-muted/30 border-r border-muted-foreground/20" />}
                                {pct > 0 && <div className={`absolute inset-y-0 left-0 pointer-events-none ${fillColor}`} style={{ width: `${pct}%` }} />}
                                <span className="relative">
                                  {count > 0 || effectiveMax > 0 ? (
                                    <>{count}{effectiveMax > 0 && (isDerived
                                      ? <span className="text-muted-foreground/50 inline-flex items-center gap-0.5" title="Calculated from max_users">/{effectiveMax}<Info className="h-2.5 w-2.5" /></span>
                                      : <span className="text-muted-foreground">/{effectiveMax}</span>
                                    )}</>
                                  ) : <span className="text-muted-foreground">—</span>}
                                </span>
                              </td>
                            )
                          })}
                          <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground">
                            {formatBps(device.in_bps)}
                          </td>
                          <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground">
                            {formatBps(device.out_bps)}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
