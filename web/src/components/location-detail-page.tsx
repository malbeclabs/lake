import { useParams, useNavigate, Link } from 'react-router-dom'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { Loader2, Building2, AlertCircle, ArrowLeft } from 'lucide-react'
import { fetchLocation, fetchDevices, fetchPeeringDBFacility } from '@/lib/api'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { useBackLink } from '@/hooks/use-back-link'

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

export function LocationDetailPage() {
  const { pk } = useParams<{ pk: string }>()
  const navigate = useNavigate()
  const back = useBackLink({ to: '/dz/locations', label: 'locations' })

  const { data: location, isLoading, error } = useQuery({
    queryKey: ['location', pk],
    queryFn: () => fetchLocation(pk!),
    enabled: !!pk,
  })

  const { data: devicesResponse } = useQuery({
    queryKey: ['devices', 'location', pk],
    queryFn: () => fetchDevices(100, 0, 'code', 'asc', [`location_pk:${pk}`]),
    enabled: !!pk,
    placeholderData: keepPreviousData,
  })

  const { data: peeringdb } = useQuery({
    queryKey: ['peeringdb', location?.loc_id],
    queryFn: () => fetchPeeringDBFacility(location!.loc_id),
    enabled: !!location && location.loc_id > 0,
    staleTime: 1000 * 60 * 60,
  })

  useDocumentTitle(location?.code || 'Location')

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error || !location) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Location not found</div>
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

  const devices = devicesResponse?.items ?? []

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
              <h1 className="text-2xl font-medium">{location.name || location.code}</h1>
              {statusPill(location.status)}
            </div>
            <div className="text-sm text-muted-foreground font-mono mt-0.5">{location.code}</div>
            {peeringdb?.orgName && (
              <div className="text-sm text-muted-foreground mt-1">{peeringdb.orgName}</div>
            )}
          </div>
        </div>

        {/* Info grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Details</h3>
            <dl className="space-y-2">
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Country</dt>
                <dd className="text-sm">{location.country || '—'}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Metro</dt>
                <dd className="text-sm font-mono">{location.metro_code || '—'}</dd>
              </div>
              {location.loc_id > 0 && (
                <div className="flex justify-between">
                  <dt className="text-sm text-muted-foreground">PeeringDB ID</dt>
                  <dd className="text-sm font-mono">{location.loc_id}</dd>
                </div>
              )}
            </dl>
          </div>

          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Coordinates</h3>
            <dl className="space-y-2">
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Latitude</dt>
                <dd className="text-sm font-mono">{location.lat !== 0 ? location.lat.toFixed(4) : '—'}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Longitude</dt>
                <dd className="text-sm font-mono">{location.lng !== 0 ? location.lng.toFixed(4) : '—'}</dd>
              </div>
            </dl>
          </div>

          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Infrastructure</h3>
            <dl className="space-y-2">
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Devices</dt>
                <dd className="text-sm">{location.device_count}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Active Users</dt>
                <dd className="text-sm">{location.user_count}</dd>
              </div>
            </dl>
          </div>
        </div>

        {/* Devices table */}
        <div>
          <h2 className="text-base font-medium mb-4">
            Devices
            {devices.length > 0 && (
              <span className="ml-2 text-sm font-normal text-muted-foreground">({devices.length})</span>
            )}
          </h2>
          <div className="border border-border rounded-lg overflow-hidden bg-card">
            <table className="w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <th className="px-4 py-3 font-medium">Code</th>
                  <th className="px-4 py-3 font-medium">Type</th>
                  <th className="px-4 py-3 font-medium">Status</th>
                  <th className="px-4 py-3 font-medium">Contributor</th>
                  <th className="px-4 py-3 font-medium text-right">Users</th>
                  <th className="px-4 py-3 font-medium">Public IP</th>
                </tr>
              </thead>
              <tbody>
                {devices.map((device) => (
                  <tr
                    key={device.pk}
                    className="border-b border-border last:border-b-0 hover:bg-muted transition-colors"
                  >
                    <td className="px-4 py-3 whitespace-nowrap">
                      <Link
                        to={`/dz/devices/${encodeURIComponent(device.pk)}`}
                        className="font-mono text-sm text-blue-600 dark:text-blue-400 hover:underline"
                      >
                        {device.code}
                      </Link>
                    </td>
                    <td className="px-4 py-3 text-sm text-muted-foreground">{device.device_type || '—'}</td>
                    <td className="px-4 py-3">
                      {statusPill(device.status)}
                    </td>
                    <td className="px-4 py-3 text-sm font-mono">{device.contributor_code || '—'}</td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {device.current_users > 0 ? device.current_users : <span className="text-muted-foreground">—</span>}
                    </td>
                    <td className="px-4 py-3 text-sm font-mono text-muted-foreground">{device.public_ip || '—'}</td>
                  </tr>
                ))}
                {devices.length === 0 && (
                  <tr>
                    <td colSpan={6} className="px-4 py-8 text-center text-muted-foreground text-sm">
                      No devices at this location
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  )
}
