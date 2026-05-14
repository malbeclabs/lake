import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { Loader2, Network, AlertCircle, ArrowLeft } from 'lucide-react'
import { fetchTopologies, fetchLinks } from '@/lib/api'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { useBackLink } from '@/hooks/use-back-link'
import { handleRowClick } from '@/lib/utils'

export function TopologyDetailPage() {
  const { name } = useParams<{ name: string }>()
  const navigate = useNavigate()
  const back = useBackLink({ to: '/dz/links', label: 'links' })

  const { data: topologiesData, isLoading: topoLoading } = useQuery({
    queryKey: ['topologies'],
    queryFn: fetchTopologies,
    staleTime: 60_000,
  })

  const topology = topologiesData?.topologies?.find(
    t => t.name.toLowerCase() === name?.toLowerCase()
  )

  const { data: linksData } = useQuery({
    queryKey: ['topology-links', name],
    queryFn: () => fetchLinks(500, 0, 'code', 'asc', [`topology:${name}`]),
    enabled: !!name,
    placeholderData: keepPreviousData,
  })

  const links = linksData?.items ?? []

  useDocumentTitle(topology?.name || name || 'Topology')

  if (topoLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (!topology) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Topology not found</div>
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

        <div className="flex items-center gap-3 mb-8">
          <Network className="h-6 w-6 text-purple-500" />
          <h1 className="text-2xl font-bold">{topology.name}</h1>
        </div>

        {/* Metadata */}
        <div className="grid grid-cols-2 sm:grid-cols-5 gap-4 mb-8">
          <div className="p-4 bg-muted/30 rounded-lg">
            <div className="text-sm text-muted-foreground">Constraint</div>
            <div className="text-lg font-medium">{topology.constraint}</div>
          </div>
          <div className="p-4 bg-muted/30 rounded-lg">
            <div className="text-sm text-muted-foreground">Admin Group Bit</div>
            <div className="text-lg font-medium tabular-nums">{topology.admin_group_bit}</div>
          </div>
          <div className="p-4 bg-muted/30 rounded-lg">
            <div className="text-sm text-muted-foreground">Flex-Algo</div>
            <div className="text-lg font-medium tabular-nums">{topology.flex_algo_number}</div>
          </div>
          <div className="p-4 bg-muted/30 rounded-lg">
            <div className="text-sm text-muted-foreground">Color</div>
            <div className="text-lg font-medium tabular-nums">{topology.color}</div>
          </div>
          <div className="p-4 bg-muted/30 rounded-lg">
            <div className="text-sm text-muted-foreground">Links</div>
            <div className="text-lg font-medium tabular-nums">{topology.link_count}</div>
          </div>
        </div>

        {/* Member links */}
        <h2 className="text-lg font-medium mb-4">Member Links</h2>
        <div className="border border-border rounded-lg overflow-hidden">
          <table className="w-full">
            <thead>
              <tr className="text-left text-sm text-muted-foreground border-b border-border bg-muted/30">
                <th className="px-4 py-2.5 font-medium">Link</th>
                <th className="px-4 py-2.5 font-medium">Type</th>
                <th className="px-4 py-2.5 font-medium">Route</th>
                <th className="px-4 py-2.5 font-medium">Status</th>
              </tr>
            </thead>
            <tbody>
              {links.map((link) => (
                <tr
                  key={link.pk}
                  className="border-b border-border last:border-b-0 hover:bg-muted cursor-pointer transition-colors"
                  onClick={(e) => handleRowClick(e, `/dz/links/${link.pk}`, navigate)}
                >
                  <td className="px-4 py-2.5 font-mono text-sm">{link.code}</td>
                  <td className="px-4 py-2.5 text-sm text-muted-foreground">{link.link_type}</td>
                  <td className="px-4 py-2.5 text-sm text-muted-foreground">
                    {link.side_a_metro && link.side_z_metro
                      ? `${link.side_a_metro} — ${link.side_z_metro}`
                      : `${link.side_a_code} — ${link.side_z_code}`}
                  </td>
                  <td className="px-4 py-2.5 text-sm">
                    {link.unicast_drained ? (
                      <span className="inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-amber-500/15 text-amber-600 dark:text-amber-400">
                        drained
                      </span>
                    ) : (
                      <span className="text-muted-foreground">{link.status}</span>
                    )}
                  </td>
                </tr>
              ))}
              {links.length === 0 && (
                <tr>
                  <td colSpan={4} className="px-4 py-8 text-center text-muted-foreground">
                    No links in this topology
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
