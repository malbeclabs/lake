import { useParams, useNavigate, Link } from 'react-router-dom'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useState, useMemo } from 'react'
import { Loader2, Building2, AlertCircle, ArrowLeft, ChevronUp, ChevronDown } from 'lucide-react'
import { fetchFacility, fetchDevices, fetchPeeringDBFacility } from '@/lib/api'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { useBackLink } from '@/hooks/use-back-link'
import { handleRowClick } from '@/lib/utils'
import { MiniMap } from '@/components/mini-map'
import { CopyableText } from '@/components/copyable-text'

type DeviceSortField = 'code' | 'type' | 'contributor' | 'metro' | 'status' | 'users' | 'unicast' | 'subscribers' | 'publishers' | 'in' | 'out'
type SortDir = 'asc' | 'desc'

function formatBps(bps: number): string {
  if (bps === 0) return '—'
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(1)} Tbps`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(1)} Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)} Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(1)} Kbps`
  return `${bps.toFixed(0)} bps`
}

const statusDotColor: Record<string, string> = {
  activated: 'bg-green-500',
  active: 'bg-green-500',
  provisioning: 'bg-blue-500',
  'soft-drained': 'bg-amber-500',
  drained: 'bg-amber-500',
  suspended: 'bg-red-500',
  pending: 'bg-amber-500',
  inactive: 'bg-muted-foreground',
  deactivated: 'bg-muted-foreground',
}

function statusPill(status: string) {
  const colors: Record<string, string> = {
    activated: 'bg-green-500/10 text-green-600 dark:text-green-400 border-green-500/20',
    pending: 'bg-yellow-500/10 text-yellow-600 dark:text-yellow-400 border-yellow-500/20',
    suspended: 'bg-red-500/10 text-red-600 dark:text-red-400 border-red-500/20',
  }
  const cls = colors[status] ?? 'bg-muted text-muted-foreground border-border'
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium border ${cls}`}>
      {status}
    </span>
  )
}

export function FacilityDetailPage() {
  const { pk } = useParams<{ pk: string }>()
  const navigate = useNavigate()
  const back = useBackLink({ to: '/dz/facilities', label: 'facilities' })
  const [sortField, setSortField] = useState<DeviceSortField>('code')
  const [sortDir, setSortDir] = useState<SortDir>('asc')

  const { data: facility, isLoading, error } = useQuery({
    queryKey: ['facility', pk],
    queryFn: () => fetchFacility(pk!),
    enabled: !!pk,
  })

  const { data: devicesResponse } = useQuery({
    queryKey: ['devices', 'facility', pk],
    queryFn: () => fetchDevices(100, 0, 'code', 'asc', [`location_pk:${pk}`]),
    enabled: !!pk,
    placeholderData: keepPreviousData,
  })

  const { data: peeringdb } = useQuery({
    queryKey: ['peeringdb', facility?.loc_id],
    queryFn: () => fetchPeeringDBFacility(facility!.loc_id),
    enabled: !!facility && facility.loc_id > 0,
    staleTime: 1000 * 60 * 60,
  })

  useDocumentTitle(facility?.code || 'Facility')

  const rawDevices = devicesResponse?.items ?? []
  const devices = useMemo(() => {
    return [...rawDevices].sort((a, b) => {
      let cmp = 0
      switch (sortField) {
        case 'code': cmp = a.code.localeCompare(b.code); break
        case 'type': cmp = (a.device_type || '').localeCompare(b.device_type || ''); break
        case 'contributor': cmp = (a.contributor_code || '').localeCompare(b.contributor_code || ''); break
        case 'metro': cmp = (a.metro_code || '').localeCompare(b.metro_code || ''); break
        case 'status': cmp = a.status.localeCompare(b.status); break
        case 'users': {
          const noA = !a.max_users, noB = !b.max_users
          if (noA !== noB) return noA ? 1 : -1
          if (noA && noB) break
          const fracA = a.current_users / a.max_users
          const fracB = b.current_users / b.max_users
          if (fracA !== fracB) { cmp = fracA - fracB; break }
          return b.max_users - a.max_users
        }
        case 'unicast': {
          const effA = a.max_unicast_users > 0 ? a.max_unicast_users : a.unicast_users + Math.max(0, a.max_users - a.current_users)
          const effB = b.max_unicast_users > 0 ? b.max_unicast_users : b.unicast_users + Math.max(0, b.max_users - b.current_users)
          const noA = effA === 0 && a.unicast_users === 0, noB = effB === 0 && b.unicast_users === 0
          if (noA !== noB) return noA ? 1 : -1
          cmp = Math.max(0, effA - a.unicast_users) - Math.max(0, effB - b.unicast_users)
          break
        }
        case 'subscribers': {
          const effA = a.max_multicast_subscribers > 0 ? a.max_multicast_subscribers : a.multicast_subscribers_count + Math.max(0, a.max_users - a.current_users)
          const effB = b.max_multicast_subscribers > 0 ? b.max_multicast_subscribers : b.multicast_subscribers_count + Math.max(0, b.max_users - b.current_users)
          const noA = effA === 0 && a.multicast_subscribers_count === 0, noB = effB === 0 && b.multicast_subscribers_count === 0
          if (noA !== noB) return noA ? 1 : -1
          cmp = Math.max(0, effA - a.multicast_subscribers_count) - Math.max(0, effB - b.multicast_subscribers_count)
          break
        }
        case 'publishers': {
          const effA = a.max_multicast_publishers > 0 ? a.max_multicast_publishers : a.multicast_publishers_count + Math.max(0, a.max_users - a.current_users)
          const effB = b.max_multicast_publishers > 0 ? b.max_multicast_publishers : b.multicast_publishers_count + Math.max(0, b.max_users - b.current_users)
          const noA = effA === 0 && a.multicast_publishers_count === 0, noB = effB === 0 && b.multicast_publishers_count === 0
          if (noA !== noB) return noA ? 1 : -1
          cmp = Math.max(0, effA - a.multicast_publishers_count) - Math.max(0, effB - b.multicast_publishers_count)
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

  if (error || !facility) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Facility not found</div>
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
        <button
          onClick={() => navigate(back.to)}
          className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground mb-6"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to {back.label}
        </button>

        {/* Header */}
        <div className="flex items-start gap-4 mb-8">
          {peeringdb?.logoUrl ? (
            <img
              src={peeringdb.logoUrl}
              alt=""
              className="h-12 w-auto object-contain flex-shrink-0"
              onError={(e) => { (e.target as HTMLImageElement).style.display = 'none' }}
            />
          ) : (
            <Building2 className="h-8 w-8 text-muted-foreground mt-1 flex-shrink-0" />
          )}
          <div>
            <div className="flex items-center gap-3">
              <h1 className="text-2xl font-medium">{facility.name || facility.code}</h1>
              {statusPill(facility.status)}
            </div>
            <div className="text-sm text-muted-foreground font-mono mt-0.5">{facility.code}</div>
            {peeringdb?.orgName && (
              <div className="text-sm text-muted-foreground mt-1">{peeringdb.orgName}</div>
            )}
          </div>
        </div>

        {/* Info grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-10">
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Details</h3>
            <dl className="space-y-2">
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Country</dt>
                <dd className="text-sm">{facility.country || '—'}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Metro</dt>
                <dd className="text-sm font-mono">
                  {facility.metro_pk
                    ? <Link to={`/dz/metros/${facility.metro_pk}`} className="font-mono text-foreground/85 hover:text-foreground hover:underline">{facility.metro_code}</Link>
                    : (facility.metro_code || '—')}
                </dd>
              </div>
              <div className="flex justify-between items-center">
                <dt className="text-sm text-muted-foreground">Pubkey</dt>
                <dd className="text-sm font-mono">
                  <CopyableText text={facility.pk} className="font-mono text-sm">
                    {facility.pk.slice(0, 4)}...{facility.pk.slice(-4)}
                  </CopyableText>
                </dd>
              </div>
              <div className="flex justify-between items-center">
                <dt className="text-sm text-muted-foreground">Code</dt>
                <dd className="text-sm font-mono">
                  <CopyableText text={facility.code} className="font-mono text-sm" />
                </dd>
              </div>
              {facility.loc_id > 0 && (
                <div className="flex justify-between">
                  <dt className="text-sm text-muted-foreground">PeeringDB ID</dt>
                  <dd className="text-sm font-mono">
                    <a
                      href={`https://www.peeringdb.com/fac/${facility.loc_id}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-blue-600 dark:text-blue-400 hover:underline"
                    >
                      {facility.loc_id}
                    </a>
                  </dd>
                </div>
              )}
            </dl>
          </div>

          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Infrastructure</h3>
            <dl className="space-y-2">
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Devices</dt>
                <dd className="text-sm">{facility.device_count}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Users</dt>
                <dd className="text-sm tabular-nums">
                  {facility.user_count}
                  {facility.max_users > 0 && <span className="text-muted-foreground">/{facility.max_users}</span>}
                </dd>
              </div>
              {(() => {
                const effUnicastMax = facility.max_unicast_users > 0 ? facility.max_unicast_users : facility.max_users
                const effSubsMax = facility.max_multicast_subscribers > 0 ? facility.max_multicast_subscribers : facility.max_users
                const effPubsMax = facility.max_multicast_publishers > 0 ? facility.max_multicast_publishers : facility.max_users
                return (
                  <>
                    {(facility.unicast_users_count > 0 || effUnicastMax > 0) && (
                      <div className="flex justify-between">
                        <dt className="text-sm text-muted-foreground pl-3">Unicast</dt>
                        <dd className="text-sm tabular-nums">
                          {facility.unicast_users_count}
                          {effUnicastMax > 0 && <span className="text-muted-foreground">/{effUnicastMax}</span>}
                        </dd>
                      </div>
                    )}
                    {(facility.multicast_subscribers_count > 0 || effSubsMax > 0) && (
                      <div className="flex justify-between">
                        <dt className="text-sm text-muted-foreground pl-3">Subscribers</dt>
                        <dd className="text-sm tabular-nums">
                          {facility.multicast_subscribers_count}
                          {effSubsMax > 0 && <span className="text-muted-foreground">/{effSubsMax}</span>}
                        </dd>
                      </div>
                    )}
                    {(facility.multicast_publishers_count > 0 || effPubsMax > 0) && (
                      <div className="flex justify-between">
                        <dt className="text-sm text-muted-foreground pl-3">Publishers</dt>
                        <dd className="text-sm tabular-nums">
                          {facility.multicast_publishers_count}
                          {effPubsMax > 0 && <span className="text-muted-foreground">/{effPubsMax}</span>}
                        </dd>
                      </div>
                    )}
                  </>
                )
              })()}
            </dl>
          </div>

          {facility.lat !== 0 && facility.lng !== 0 ? (
            <div className="border border-border rounded-lg overflow-hidden bg-card" style={{ height: '160px' }}>
              <MiniMap
                lat={facility.lat}
                lng={facility.lng}
                googleMapsHref={`https://www.google.com/maps?q=${facility.lat},${facility.lng}`}
              />
            </div>
          ) : null}
        </div>

        {/* Devices table */}
        <div>
            <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider mb-3">
              Devices ({devices.length}{devicesResponse && devicesResponse.total > rawDevices.length ? ` of ${devicesResponse.total}` : ''})
            </h2>
            <div className="border border-border rounded-lg overflow-hidden">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border bg-muted/30 text-xs text-muted-foreground uppercase tracking-wider">
                      {(['code', 'type', 'contributor', 'metro', 'status'] as DeviceSortField[]).map(f => (
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
                      const remaining = device.max_users > 0 ? Math.max(0, device.max_users - device.current_users) : 0
                      const effUnicast = Math.max(device.unicast_users, device.max_unicast_users > 0 ? device.max_unicast_users : device.unicast_users + remaining)
                      const effSubs = Math.max(device.multicast_subscribers_count, device.max_multicast_subscribers > 0 ? device.max_multicast_subscribers : device.multicast_subscribers_count + remaining)
                      const effPubs = Math.max(device.multicast_publishers_count, device.max_multicast_publishers > 0 ? device.max_multicast_publishers : device.multicast_publishers_count + remaining)
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
                          <td className="px-4 py-3 text-sm">
                            {device.contributor_pk
                              ? <Link to={`/dz/contributors/${device.contributor_pk}`} className="text-foreground/85 hover:text-foreground hover:underline" onClick={e => e.stopPropagation()}>{device.contributor_code}</Link>
                              : <span className="text-muted-foreground">—</span>}
                          </td>
                          <td className="px-4 py-3 text-sm">
                            {device.metro_pk
                              ? <Link to={`/dz/metros/${device.metro_pk}`} className="font-mono text-foreground/85 hover:text-foreground hover:underline" onClick={e => e.stopPropagation()}>{device.metro_code}</Link>
                              : <span className="text-muted-foreground">—</span>}
                          </td>
                          <td className="px-4 py-3">
                            <span
                              className={`inline-block h-2.5 w-2.5 rounded-full ${statusDotColor[device.status] ?? 'bg-muted-foreground'}`}
                              title={device.status}
                            />
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
                            const available = effectiveMax > count ? effectiveMax - count : 0
                            return (
                              <td key={i} className="px-4 py-3 text-sm tabular-nums text-right">
                                {count === 0 && effectiveMax === 0
                                  ? <span className="text-muted-foreground">—</span>
                                  : <span>{available}</span>
                                }
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
                    {devices.length === 0 && (
                      <tr>
                        <td colSpan={11} className="px-4 py-8 text-center text-muted-foreground text-sm">
                          No devices
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
      </div>
    </div>
  )
}
