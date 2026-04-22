import { useParams, useNavigate, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { useState, useMemo } from 'react'
import { Loader2, Building2, AlertCircle, ArrowLeft, Info, ChevronUp, ChevronDown } from 'lucide-react'
import { fetchContributor, fetchDevicesByContributor, fetchLinksByContributor } from '@/lib/api'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { useBackLink } from '@/hooks/use-back-link'
import { handleRowClick } from '@/lib/utils'

type DeviceSortField = 'code' | 'type' | 'metro' | 'location' | 'status' | 'users' | 'unicast' | 'subscribers' | 'publishers' | 'in' | 'out'
type LinkSortField = 'code' | 'type' | 'side_a' | 'side_z' | 'status' | 'bandwidth' | 'in' | 'out' | 'latency'
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

export function ContributorDetailPage() {
  const { pk } = useParams<{ pk: string }>()
  const navigate = useNavigate()
  const back = useBackLink({ to: '/dz/contributors', label: 'contributors' })
  const [sortField, setSortField] = useState<DeviceSortField>('code')
  const [sortDir, setSortDir] = useState<SortDir>('asc')
  const [linkSortField, setLinkSortField] = useState<LinkSortField>('code')
  const [linkSortDir, setLinkSortDir] = useState<SortDir>('asc')

  const { data: contributor, isLoading, error } = useQuery({
    queryKey: ['contributor', pk],
    queryFn: () => fetchContributor(pk!),
    enabled: !!pk,
  })

  const { data: devicesData } = useQuery({
    queryKey: ['contributor-devices', pk],
    queryFn: () => fetchDevicesByContributor(pk!),
    enabled: !!pk,
  })

  const { data: linksData } = useQuery({
    queryKey: ['contributor-links', pk],
    queryFn: () => fetchLinksByContributor(pk!),
    enabled: !!pk,
  })

  useDocumentTitle(contributor?.code || 'Contributor')

  const rawDevices = devicesData?.items ?? []
  const devices = useMemo(() => {
    return [...rawDevices].sort((a, b) => {
      let cmp = 0
      switch (sortField) {
        case 'code': cmp = a.code.localeCompare(b.code); break
        case 'type': cmp = (a.device_type || '').localeCompare(b.device_type || ''); break
        case 'metro': cmp = (a.metro_code || '').localeCompare(b.metro_code || ''); break
        case 'location': cmp = (a.location_code || '').localeCompare(b.location_code || ''); break
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

  const rawLinks = linksData?.items ?? []
  const links = useMemo(() => {
    return [...rawLinks].sort((a, b) => {
      let cmp = 0
      switch (linkSortField) {
        case 'code': cmp = a.code.localeCompare(b.code); break
        case 'type': cmp = a.link_type.localeCompare(b.link_type); break
        case 'side_a': cmp = a.side_a_code.localeCompare(b.side_a_code); break
        case 'side_z': cmp = a.side_z_code.localeCompare(b.side_z_code); break
        case 'status': cmp = a.status.localeCompare(b.status); break
        case 'bandwidth': cmp = a.bandwidth_bps - b.bandwidth_bps; break
        case 'in': cmp = a.in_bps - b.in_bps; break
        case 'out': cmp = a.out_bps - b.out_bps; break
        case 'latency': cmp = a.latency_us - b.latency_us; break
      }
      return linkSortDir === 'asc' ? cmp : -cmp
    })
  }, [rawLinks, linkSortField, linkSortDir])

  const handleLinkSort = (field: LinkSortField) => {
    if (linkSortField === field) setLinkSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setLinkSortField(field); setLinkSortDir('asc') }
  }
  const LinkSortIcon = ({ field }: { field: LinkSortField }) => {
    if (linkSortField !== field) return null
    return linkSortDir === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />
  }

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error || !contributor) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Contributor not found</div>
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
          <Building2 className="h-8 w-8 text-muted-foreground" />
          <div>
            <h1 className="text-2xl font-medium">{contributor.name || contributor.code}</h1>
            <div className="text-sm text-muted-foreground font-mono">{contributor.code}</div>
          </div>
        </div>

        {/* Info grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-10">
          {/* Devices */}
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Devices</h3>
            <dl className="space-y-2">
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Total Devices</dt>
                <dd className="text-sm">{contributor.device_count}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Side A Devices</dt>
                <dd className="text-sm">{contributor.side_a_devices}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Side Z Devices</dt>
                <dd className="text-sm">{contributor.side_z_devices}</dd>
              </div>
            </dl>
          </div>

          {/* Links & Users */}
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Links & Users</h3>
            <dl className="space-y-2">
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Links</dt>
                <dd className="text-sm">{contributor.link_count}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Users</dt>
                <dd className="text-sm">{contributor.user_count}</dd>
              </div>
              {(() => {
                const effUnicast = Math.max(contributor.unicast_users_count, contributor.max_unicast_users)
                const effSubs = Math.max(contributor.multicast_subscribers_count, contributor.max_multicast_subscribers)
                const effPubs = Math.max(contributor.multicast_publishers_count, contributor.max_multicast_publishers)
                const isDerivedUnicast = contributor.raw_max_unicast_users === 0 && effUnicast > 0
                const isDerivedSubs = contributor.raw_max_multicast_subscribers === 0 && effSubs > 0
                const isDerivedPubs = contributor.raw_max_multicast_publishers === 0 && effPubs > 0
                return (
                  <>
                    <div className="flex justify-between">
                      <dt className="text-sm text-muted-foreground">Unicast</dt>
                      <dd className="text-sm tabular-nums">
                        {contributor.unicast_users_count}{effUnicast > 0 && <> / {isDerivedUnicast
                          ? <span className="text-muted-foreground/50 inline-flex items-center gap-0.5" title="Calculated from max_users">{effUnicast}<Info className="h-2.5 w-2.5" /></span>
                          : <span className="text-muted-foreground">{effUnicast}</span>
                        }</>}
                      </dd>
                    </div>
                    <div className="flex justify-between">
                      <dt className="text-sm text-muted-foreground">Subscribers</dt>
                      <dd className="text-sm tabular-nums">
                        {contributor.multicast_subscribers_count}{effSubs > 0 && <> / {isDerivedSubs
                          ? <span className="text-muted-foreground/50 inline-flex items-center gap-0.5" title="Calculated from max_users">{effSubs}<Info className="h-2.5 w-2.5" /></span>
                          : <span className="text-muted-foreground">{effSubs}</span>
                        }</>}
                      </dd>
                    </div>
                    <div className="flex justify-between">
                      <dt className="text-sm text-muted-foreground">Publishers</dt>
                      <dd className="text-sm tabular-nums">
                        {contributor.multicast_publishers_count}{effPubs > 0 && <> / {isDerivedPubs
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
                <dd className="text-sm">{formatBps(contributor.in_bps)}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Outbound</dt>
                <dd className="text-sm">{formatBps(contributor.out_bps)}</dd>
              </div>
            </dl>
          </div>
        </div>

        {/* Devices table */}
        {devices.length > 0 && (
          <div className="mb-8">
            <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider mb-3">
              Devices ({devices.length}{devicesData && devicesData.total > rawDevices.length ? ` of ${devicesData.total}` : ''})
            </h2>
            <div className="border border-border rounded-lg overflow-hidden">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border bg-muted/30 text-xs text-muted-foreground uppercase tracking-wider">
                      {(['code', 'type', 'metro', 'location', 'status'] as DeviceSortField[]).map(f => (
                        <th key={f} className="px-4 py-3 font-medium text-left">
                          <button className="inline-flex items-center gap-1" type="button" onClick={() => handleDeviceSort(f)}>
                            {f === 'location' ? 'Location' : f.charAt(0).toUpperCase() + f.slice(1)}
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
                            {device.metro_pk
                              ? <Link to={`/dz/metros/${device.metro_pk}`} className="font-mono text-foreground/85 hover:text-foreground hover:underline" onClick={e => e.stopPropagation()}>{device.metro_code}</Link>
                              : <span className="text-muted-foreground">—</span>}
                          </td>
                          <td className="px-4 py-3 text-sm">
                            {device.location_pk
                              ? <Link to={`/dz/locations/${encodeURIComponent(device.location_pk)}`} className="font-mono text-foreground/85 hover:text-foreground hover:underline" onClick={e => e.stopPropagation()}>{device.location_code}</Link>
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
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {/* Links table */}
        {links.length > 0 && (
          <div>
            <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider mb-3">
              Links ({links.length}{linksData && linksData.total > rawLinks.length ? ` of ${linksData.total}` : ''})
            </h2>
            <div className="border border-border rounded-lg overflow-hidden">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border bg-muted/30 text-xs text-muted-foreground uppercase tracking-wider">
                      {([
                        { field: 'code', label: 'Code' },
                        { field: 'type', label: 'Type' },
                        { field: 'status', label: 'Status' },
                        { field: 'side_a', label: 'Side A' },
                        { field: 'side_z', label: 'Side Z' },
                      ] as { field: LinkSortField; label: string }[]).map(({ field, label }) => (
                        <th key={field} className="px-4 py-3 font-medium text-left">
                          <button className="inline-flex items-center gap-1" type="button" onClick={() => handleLinkSort(field)}>
                            {label}
                            <LinkSortIcon field={field} />
                          </button>
                        </th>
                      ))}
                      {([
                        { field: 'bandwidth', label: 'Bandwidth' },
                        { field: 'in', label: 'In' },
                        { field: 'out', label: 'Out' },
                        { field: 'latency', label: 'Latency' },
                      ] as { field: LinkSortField; label: string }[]).map(({ field, label }) => (
                        <th key={field} className="px-4 py-3 font-medium text-right">
                          <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleLinkSort(field)}>
                            {label}
                            <LinkSortIcon field={field} />
                          </button>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {links.map((link) => (
                      <tr
                        key={link.pk}
                        className="border-b border-border last:border-b-0 hover:bg-muted cursor-pointer transition-colors"
                        onClick={(e) => handleRowClick(e, `/dz/links/${link.pk}`, navigate)}
                      >
                        <td className="px-4 py-3 font-mono text-sm whitespace-nowrap">{link.code}</td>
                        <td className="px-4 py-3 text-sm text-muted-foreground">{link.link_type}</td>
                        <td className="px-4 py-3">
                          <span
                            className={`inline-block h-2.5 w-2.5 rounded-full ${statusDotColor[link.status] ?? 'bg-muted-foreground'}`}
                            title={link.status}
                          />
                        </td>
                        <td className="px-4 py-3 text-sm whitespace-nowrap">
                          {link.side_a_pk
                            ? <Link to={`/dz/devices/${link.side_a_pk}`} className="font-mono text-foreground/85 hover:text-foreground hover:underline" onClick={e => e.stopPropagation()}>{link.side_a_code}</Link>
                            : <span className="text-muted-foreground">—</span>}
                          {link.side_a_metro && <span className="text-xs text-muted-foreground ml-1">
                            (<Link to={`/dz/metros/${link.side_a_metro_pk}`} className="hover:text-foreground hover:underline" onClick={e => e.stopPropagation()}>{link.side_a_metro}</Link>)
                          </span>}
                        </td>
                        <td className="px-4 py-3 text-sm whitespace-nowrap">
                          {link.side_z_pk
                            ? <Link to={`/dz/devices/${link.side_z_pk}`} className="font-mono text-foreground/85 hover:text-foreground hover:underline" onClick={e => e.stopPropagation()}>{link.side_z_code}</Link>
                            : <span className="text-muted-foreground">—</span>}
                          {link.side_z_metro && <span className="text-xs text-muted-foreground ml-1">
                            (<Link to={`/dz/metros/${link.side_z_metro_pk}`} className="hover:text-foreground hover:underline" onClick={e => e.stopPropagation()}>{link.side_z_metro}</Link>)
                          </span>}
                        </td>
                        <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground">{formatBps(link.bandwidth_bps)}</td>
                        <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground">{formatBps(link.in_bps)}</td>
                        <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground">{formatBps(link.out_bps)}</td>
                        <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground">
                          {link.latency_us > 0 ? `${(link.latency_us / 1000).toFixed(1)} ms` : '—'}
                        </td>
                      </tr>
                    ))}
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
