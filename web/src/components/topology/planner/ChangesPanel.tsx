import { useEffect, useRef, useState } from 'react'
import { ChevronUp, ChevronDown, Trash2 } from 'lucide-react'
import { usePlanner, type PlannerContextValue } from './PlannerContext'
import { changeSummary } from './change-label'
import { DriftBadge } from './DriftBadge'
import type { DriftStatus, PlanChange, PlanChangeState } from '@/lib/api'

const STATES: PlanChangeState[] = ['pending', 'done', 'skipped']

function ChangeRow({
  change,
  drift,
  index,
  count,
  onMove,
  patchChange,
  removeChange,
  focusChange,
}: {
  change: PlanChange
  drift: DriftStatus
  index: number
  count: number
  onMove: (index: number, dir: -1 | 1) => void
  patchChange: PlannerContextValue['patchChange']
  removeChange: PlannerContextValue['removeChange']
  focusChange: PlannerContextValue['focusChange']
}) {
  // The free-text note keeps LOCAL state and commits on blur, so typing fires a
  // single versioned PATCH per edit session -- not one per keystroke (which would
  // race the version column and trigger a 409 conflict storm). Date and status
  // already fire once per selection, so they patch directly.
  const [note, setNote] = useState(change.assignee_note ?? '')
  // Remember the last server value we synced so an external change (collaborator
  // edit / reload) re-seeds the field, but typing (which only moves local state)
  // never does. Mirrors LinkEditForm's re-seed-on-identity-change guard (T12).
  const lastServerNote = useRef(change.assignee_note ?? '')

  useEffect(() => {
    const server = change.assignee_note ?? ''
    if (server !== lastServerNote.current) {
      lastServerNote.current = server
      setNote(server)
    }
  }, [change.assignee_note])

  const commitNote = () => {
    if ((change.assignee_note ?? '') === note) return
    lastServerNote.current = note
    patchChange(change.id, { assignee_note: note })
  }

  return (
    <div className="border border-border rounded-md p-2 bg-card">
      <div className="flex items-start justify-between gap-2">
        <span className="text-xs font-medium flex items-center gap-1.5">
          <button
            type="button"
            onClick={() => focusChange(change.id)}
            className="hover:underline text-left"
            title="Fly the map to this change"
          >
            {changeSummary(change)}
          </button>
          <DriftBadge drift={drift} />
        </span>
        <div className="flex items-center gap-0.5 shrink-0">
          <button
            onClick={() => onMove(index, -1)}
            disabled={index === 0}
            className="p-0.5 text-muted-foreground hover:text-foreground disabled:opacity-30"
            title="Move up"
          >
            <ChevronUp className="h-3.5 w-3.5" />
          </button>
          <button
            onClick={() => onMove(index, 1)}
            disabled={index === count - 1}
            className="p-0.5 text-muted-foreground hover:text-foreground disabled:opacity-30"
            title="Move down"
          >
            <ChevronDown className="h-3.5 w-3.5" />
          </button>
          <button
            onClick={() => removeChange(change.id)}
            className="p-0.5 text-muted-foreground hover:text-red-500"
            title="Remove change"
          >
            <Trash2 className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-2 mt-2">
        <label className="block text-[11px] text-muted-foreground">
          Target date
          <input
            type="date"
            value={change.target_date ?? ''}
            onChange={(e) => patchChange(change.id, { target_date: e.target.value || null })}
            className="mt-0.5 w-full px-1.5 py-1 text-xs bg-muted border border-border rounded"
          />
        </label>
        <label className="block text-[11px] text-muted-foreground">
          Status
          <select
            value={change.state}
            onChange={(e) => patchChange(change.id, { state: e.target.value as PlanChangeState })}
            className="mt-0.5 w-full px-1.5 py-1 text-xs bg-muted border border-border rounded"
          >
            {STATES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>
      </div>

      <input
        value={note}
        onChange={(e) => setNote(e.target.value)}
        onBlur={commitNote}
        placeholder="Note (OPS ticket, Slack thread…)"
        className="mt-2 w-full px-1.5 py-1 text-xs bg-muted border border-border rounded"
      />
    </div>
  )
}

export function ChangesPanel() {
  const { plan, drift, patchChange, removeChange, reorderChanges, focusChange } = usePlanner()
  if (!plan) return null

  const ordered = [...plan.changes].sort((a, b) => a.seq - b.seq)

  const move = (index: number, dir: -1 | 1) => {
    const target = index + dir
    if (target < 0 || target >= ordered.length) return
    const ids = ordered.map((c) => c.id)
    ;[ids[index], ids[target]] = [ids[target], ids[index]]
    reorderChanges(ids)
  }

  return (
    <div className="p-3">
      <h2 className="text-sm font-semibold mb-3">Changes ({ordered.length})</h2>
      {(() => {
        const broken = ordered.filter((c) => drift.get(c.id) === 'broken').length
        const done = ordered.filter((c) => drift.get(c.id) === 'already_done').length
        if (broken === 0 && done === 0) return null
        return (
          <div className="mb-3 text-[11px] text-muted-foreground">
            {broken > 0 && (
              <span className="text-red-600 dark:text-red-400">{broken} broken</span>
            )}
            {broken > 0 && done > 0 && ' · '}
            {done > 0 && <span>{done} already done vs live topology</span>}
          </div>
        )
      })()}
      {ordered.length === 0 ? (
        <p className="text-xs text-muted-foreground py-6 text-center">
          No changes yet. Use the tools to edit the map.
        </p>
      ) : (
        <div className="space-y-2">
          {ordered.map((change, i) => (
            <ChangeRow
              key={change.id}
              change={change}
              drift={drift.get(change.id) ?? 'pending'}
              index={i}
              count={ordered.length}
              onMove={move}
              patchChange={patchChange}
              removeChange={removeChange}
              focusChange={focusChange}
            />
          ))}
        </div>
      )}
    </div>
  )
}
