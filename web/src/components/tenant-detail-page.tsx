import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Loader2, Layers, AlertCircle, ArrowLeft } from 'lucide-react'
import { fetchTenant } from '@/lib/api'
import { useDocumentTitle } from '@/hooks/use-document-title'

export function TenantDetailPage() {
  const { pk } = useParams<{ pk: string }>()
  const navigate = useNavigate()

  const { data: tenant, isLoading, error } = useQuery({
    queryKey: ['tenant', pk],
    queryFn: () => fetchTenant(pk!),
    enabled: !!pk,
  })

  useDocumentTitle(tenant?.code || 'Tenant')

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error || !tenant) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Tenant not found</div>
          <button
            onClick={() => navigate('/dz/tenants')}
            className="text-sm text-muted-foreground hover:text-foreground"
          >
            Back to tenants
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 py-8">
        <button
          onClick={() => navigate('/dz/tenants')}
          className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground mb-6"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to tenants
        </button>

        <div className="flex items-center gap-3 mb-8">
          <Layers className="h-8 w-8 text-muted-foreground" />
          <div>
            <h1 className="text-2xl font-medium">{tenant.code || tenant.pk}</h1>
            <div className="text-sm text-muted-foreground font-mono">{tenant.pk}</div>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Identity</h3>
            <dl className="space-y-2">
              <div className="flex justify-between gap-4">
                <dt className="text-sm text-muted-foreground shrink-0">Code</dt>
                <dd className="text-sm font-mono truncate">{tenant.code || '—'}</dd>
              </div>
              <div className="flex justify-between gap-4">
                <dt className="text-sm text-muted-foreground shrink-0">Owner</dt>
                <dd className="text-sm font-mono truncate" title={tenant.owner_pubkey}>
                  {tenant.owner_pubkey ? `${tenant.owner_pubkey.slice(0, 8)}…${tenant.owner_pubkey.slice(-6)}` : '—'}
                </dd>
              </div>
              <div className="flex justify-between gap-4">
                <dt className="text-sm text-muted-foreground shrink-0">VRF ID</dt>
                <dd className="text-sm tabular-nums">{tenant.vrf_id}</dd>
              </div>
            </dl>
          </div>

          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Billing</h3>
            <dl className="space-y-2">
              <div className="flex justify-between gap-4">
                <dt className="text-sm text-muted-foreground shrink-0">Billing Rate</dt>
                <dd className="text-sm tabular-nums">{tenant.billing_rate.toLocaleString()}</dd>
              </div>
            </dl>
          </div>

          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Routing</h3>
            <dl className="space-y-2">
              <div className="flex justify-between gap-4">
                <dt className="text-sm text-muted-foreground shrink-0">Metro Routing</dt>
                <dd className="text-sm">{tenant.metro_routing ? 'enabled' : 'disabled'}</dd>
              </div>
              <div className="flex justify-between gap-4">
                <dt className="text-sm text-muted-foreground shrink-0">Route Liveness</dt>
                <dd className="text-sm">{tenant.route_liveness ? 'enabled' : 'disabled'}</dd>
              </div>
            </dl>
          </div>
        </div>
      </div>
    </div>
  )
}
