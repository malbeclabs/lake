import { useQuery } from '@tanstack/react-query'
import { X, Loader2 } from 'lucide-react'
import { fetchPlans } from '@/lib/api'

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
                <button
                  key={p.id}
                  onClick={() => {
                    onPick(p.id)
                    onClose()
                  }}
                  className="w-full flex items-center justify-between px-3 py-2 text-left text-sm rounded hover:bg-muted"
                >
                  <span className="font-medium">{p.name}</span>
                  <span className="text-xs text-muted-foreground">
                    {p.change_count} changes · {p.status}
                  </span>
                </button>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
