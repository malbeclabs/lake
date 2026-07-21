import { useState } from 'react'
import {
  MousePointer2,
  ServerCog,
  Server,
  Link2,
  Plus,
  FolderOpen,
  Copy,
  Loader2,
  ArrowLeft,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { duplicatePlan } from '@/lib/api'
import { usePlanner } from './PlannerContext'
import { PlanPickerDialog } from './PlanPickerDialog'
import { IssuesSyncAction } from './IssuesSyncAction'
import { statusBadgeClass } from './toolbar-util'
import type { PlannerTool } from './planner-reducer'

const TOOLS: { tool: PlannerTool; label: string; icon: typeof MousePointer2 }[] = [
  { tool: 'select', label: 'Select', icon: MousePointer2 },
  { tool: 'add-device', label: 'Add device', icon: ServerCog },
  { tool: 'remove-device', label: 'Remove device', icon: Server },
  { tool: 'add-link', label: 'Add link', icon: Link2 },
]

export function PlannerToolbar() {
  const { plan, tool, setTool, saving, newPlan, openPlan, closePlan, savePlanMeta } = usePlanner()
  const [picking, setPicking] = useState(false)
  const [renaming, setRenaming] = useState(false)
  const [nameDraft, setNameDraft] = useState('')

  const handleNew = async () => {
    const name = window.prompt('New plan name')
    if (name) await newPlan(name)
  }

  const handleDuplicate = async () => {
    if (!plan) return
    const dup = await duplicatePlan(plan.id)
    openPlan(dup.id)
  }

  const commitRename = async () => {
    setRenaming(false)
    if (plan && nameDraft && nameDraft !== plan.name) {
      await savePlanMeta({ name: nameDraft })
    }
  }

  return (
    <div className="border-b border-border px-4 py-2 flex items-center gap-3">
      {plan && (
        <button
          onClick={closePlan}
          title="Back to all plans"
          className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs rounded text-muted-foreground hover:text-foreground hover:bg-muted"
        >
          <ArrowLeft className="h-3.5 w-3.5" />
          All plans
        </button>
      )}
      {/* Plan identity */}
      <div className="flex items-center gap-2 min-w-0">
        {plan ? (
          renaming ? (
            <input
              autoFocus
              value={nameDraft}
              onChange={(e) => setNameDraft(e.target.value)}
              onBlur={commitRename}
              onKeyDown={(e) => e.key === 'Enter' && commitRename()}
              className="px-2 py-1 text-sm bg-muted border border-border rounded"
            />
          ) : (
            <button
              className="text-sm font-semibold truncate hover:underline"
              onClick={() => {
                setNameDraft(plan.name)
                setRenaming(true)
              }}
            >
              {plan.name}
            </button>
          )
        ) : (
          <span className="text-sm text-muted-foreground">No plan open</span>
        )}
        {plan && <span className={statusBadgeClass(plan.status)}>{plan.status}</span>}
        {saving && <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />}
      </div>

      {/* Tools palette (only meaningful with a plan open) */}
      {plan && (
        <div className="flex items-center gap-1 ml-2">
          {TOOLS.map(({ tool: t, label, icon: Icon }) => (
            <button
              key={t}
              title={label}
              onClick={() => setTool(t)}
              className={cn(
                'flex items-center gap-1.5 px-2.5 py-1.5 text-xs rounded transition-colors',
                tool === t
                  ? 'bg-accent text-accent-foreground'
                  : 'text-muted-foreground hover:text-foreground hover:bg-muted'
              )}
            >
              <Icon className="h-3.5 w-3.5" />
              {label}
            </button>
          ))}
        </div>
      )}

      {/* Right-side actions */}
      <div className="ml-auto flex items-center gap-2">
        <button
          onClick={handleNew}
          className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs rounded text-muted-foreground hover:text-foreground hover:bg-muted"
        >
          <Plus className="h-3.5 w-3.5" />
          New
        </button>
        <button
          onClick={() => setPicking(true)}
          className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs rounded text-muted-foreground hover:text-foreground hover:bg-muted"
        >
          <FolderOpen className="h-3.5 w-3.5" />
          Open
        </button>
        {plan && (
          <button
            onClick={handleDuplicate}
            className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs rounded text-muted-foreground hover:text-foreground hover:bg-muted"
          >
            <Copy className="h-3.5 w-3.5" />
            Duplicate
          </button>
        )}
        {plan && (
          <button
            onClick={() => {
              const next = plan.status === 'draft' ? 'approved' : 'draft'
              savePlanMeta({ status: next })
            }}
            className="px-2.5 py-1.5 text-xs rounded bg-muted hover:bg-muted/80"
          >
            {plan.status === 'draft' ? 'Approve' : 'Set draft'}
          </button>
        )}
        {plan && <IssuesSyncAction planId={plan.id} changeCount={plan.change_count} />}
      </div>

      {picking && (
        <PlanPickerDialog onPick={openPlan} onClose={() => setPicking(false)} />
      )}
    </div>
  )
}
