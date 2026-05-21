import { useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Loader2, Network, AlertCircle } from 'lucide-react'
import { fetchTopologies } from '@/lib/api'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { handleRowClick } from '@/lib/utils'

export function TopologiesPage() {
  const navigate = useNavigate()

  const { data, isLoading, error } = useQuery({
    queryKey: ['topologies'],
    queryFn: fetchTopologies,
    staleTime: 60_000,
  })

  const topologies = data?.topologies ?? []

  useDocumentTitle('Topologies')

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
          <div className="text-lg font-medium">Failed to load topologies</div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 py-8">
        <div className="flex items-center gap-3 mb-8">
          <Network className="h-6 w-6 text-purple-500" />
          <h1 className="text-2xl font-bold">Topologies</h1>
        </div>

        <div className="border border-border rounded-lg overflow-hidden">
          <table className="w-full">
            <thead>
              <tr className="text-left text-sm text-muted-foreground border-b border-border bg-muted/30">
                <th className="px-4 py-2.5 font-medium">Name</th>
                <th className="px-4 py-2.5 font-medium">Constraint</th>
                <th className="px-4 py-2.5 font-medium text-right">Admin Group Bit</th>
                <th className="px-4 py-2.5 font-medium text-right">Flex-Algo</th>
                <th className="px-4 py-2.5 font-medium text-right">Color</th>
                <th className="px-4 py-2.5 font-medium text-right">Links</th>
              </tr>
            </thead>
            <tbody>
              {topologies.map((t) => (
                <tr
                  key={t.pk}
                  className="border-b border-border last:border-b-0 hover:bg-muted cursor-pointer transition-colors"
                  onClick={(e) => handleRowClick(e, `/dz/links/topologies/${encodeURIComponent(t.name)}`, navigate)}
                >
                  <td className="px-4 py-2.5">
                    <span className="inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-green-500/15 text-green-600 dark:text-green-400">
                      {t.name}
                    </span>
                  </td>
                  <td className="px-4 py-2.5 text-sm text-muted-foreground">{t.constraint}</td>
                  <td className="px-4 py-2.5 text-sm tabular-nums text-right">{t.admin_group_bit}</td>
                  <td className="px-4 py-2.5 text-sm tabular-nums text-right">{t.flex_algo_number}</td>
                  <td className="px-4 py-2.5 text-sm tabular-nums text-right">{t.color}</td>
                  <td className="px-4 py-2.5 text-sm tabular-nums text-right">{t.link_count}</td>
                </tr>
              ))}
              {topologies.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">
                    No topologies found
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
