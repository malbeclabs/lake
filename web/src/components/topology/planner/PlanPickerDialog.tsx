import { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { X, Loader2, Trash2 } from 'lucide-react'
import { fetchPlans, deletePlan, type PlanSummary } from '@/lib/api'

export function PlanPickerDialog({
  onPick,
  onClose,
}: {
  onPick: (id: string) => void
  onClose: () => void
}) {
  const { data: plans, isLoading } = useQuery({
    queryKey: ['plans'],
    queryFn: fetchPlans,
    staleTime: 10_000,
  })
  const queryClient = useQueryClient()
  const [deletingId, setDeletingId] = useState<string | null>(null)

  const handleDelete = async (p: PlanSummary) => {
    if (!window.confirm(`Delete plan "${p.name}"? It will be removed from your list.`)) return
    setDeletingId(p.id)
    try {
      await deletePlan(p.id)
      await queryClient.invalidateQueries({ queryKey: ['plans'] })
    } finally {
      setDeletingId(null)
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50" onClick={onClose}>
      <div
        className="bg-card border border-border rounded-lg shadow-lg w-full max-w-lg max-h-[70vh] overflow-hidden flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-4 py-3 border-b border-border">
          <h2 className="text-sm font-semibold">Open plan</h2>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground">
            <X className="h-4 w-4" />
          </button>
        </div>
        <div className="overflow-y-auto p-2">
          {isLoading ? (
            <div className="flex items-center justify-center py-8 text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
            </div>
          ) : !plans || plans.length === 0 ? (
            <div className="py-8 text-center text-sm text-muted-foreground">No plans yet.</div>
          ) : (
            <div className="space-y-1">
              {plans.map((p) => (
                <div key={p.id} className="w-full flex items-center gap-1 rounded hover:bg-muted">
                  <button
                    onClick={() => {
                      onPick(p.id)
                      onClose()
                    }}
                    className="flex-1 flex items-center justify-between px-3 py-2 text-left text-sm min-w-0"
                  >
                    <span className="font-medium truncate">{p.name}</span>
                    <span className="text-xs text-muted-foreground shrink-0">
                      {p.change_count} changes · {p.status}
                    </span>
                  </button>
                  <button
                    onClick={() => handleDelete(p)}
                    disabled={deletingId === p.id}
                    title="Delete plan"
                    className="shrink-0 mr-1 rounded p-1 text-muted-foreground hover:text-red-500 disabled:opacity-50"
                  >
                    {deletingId === p.id ? (
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    ) : (
                      <Trash2 className="h-3.5 w-3.5" />
                    )}
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
