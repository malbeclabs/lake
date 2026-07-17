import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Loader2, Copy, Check, Users } from 'lucide-react'
import { fetchPlanActionList } from '@/lib/api'
import type { ActionList, ContributorActionGroup, ActionTask } from '@/lib/api'

function CopyButton({ text, label }: { text: string; label: string }) {
  const [copied, setCopied] = useState(false)
  const onCopy = () => {
    void navigator.clipboard.writeText(text)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }
  return (
    <button
      type="button"
      onClick={onCopy}
      className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs text-muted-foreground hover:text-foreground"
    >
      {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
      {copied ? 'Copied' : label}
    </button>
  )
}

function TaskRow({ task }: { task: ActionTask }) {
  const hasUsers = task.current_users !== undefined
  const hasStake = task.stake_sol !== undefined && task.stake_share !== undefined
  return (
    <li className="flex flex-col gap-0.5 py-1">
      <div className="flex items-center gap-2">
        <input type="checkbox" readOnly checked={task.state === 'done'} className="h-3 w-3" />
        <span className={task.state === 'done' ? 'text-muted-foreground line-through' : ''}>
          {task.title}
        </span>
      </div>
      {hasUsers && (
        <div className="ml-5 text-xs text-muted-foreground">
          Current users: {task.current_users}
          {hasStake ? `, stake: ${task.stake_sol!.toFixed(1)} SOL (${task.stake_share!.toFixed(2)}%)` : ''}
        </div>
      )}
      {task.involved_contributors && task.involved_contributors.length > 1 && (
        <div className="ml-5 text-xs text-muted-foreground">
          Coordinate with: {task.involved_contributors.join(', ')}
        </div>
      )}
      {task.target_date && (
        <div className="ml-5 text-xs text-muted-foreground">Target date: {task.target_date}</div>
      )}
      {task.note && <div className="ml-5 text-xs text-muted-foreground">Note: {task.note}</div>}
    </li>
  )
}

function GroupCard({ group }: { group: ContributorActionGroup }) {
  return (
    <div className="rounded-lg border border-border p-3">
      <div className="mb-2 flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <span className="font-medium">{group.contributor_code || 'Unknown contributor'}</span>
          {group.slack_channel && (
            <span className="text-xs text-muted-foreground">{group.slack_channel}</span>
          )}
        </div>
        <CopyButton text={group.markdown} label="Copy markdown" />
      </div>
      <ul className="text-sm">
        {group.tasks.map((t) => (
          <TaskRow key={`${t.seq}-${t.title}`} task={t} />
        ))}
      </ul>
    </div>
  )
}

export function ActionListPanel({ planId }: { planId: string }) {
  const { data, isLoading, isError } = useQuery<ActionList>({
    queryKey: ['plan-action-list', planId],
    queryFn: () => fetchPlanActionList(planId),
    enabled: !!planId,
  })

  if (isLoading) {
    return (
      <div className="flex items-center gap-2 p-4 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" /> Loading action list...
      </div>
    )
  }
  if (isError || !data) {
    return <div className="p-4 text-sm text-red-500">Failed to load action list.</div>
  }
  if (data.groups.length === 0) {
    return (
      <div className="p-4 text-sm text-muted-foreground">No contributor actions in this plan yet.</div>
    )
  }

  return (
    <div className="flex flex-col gap-3 p-2">
      <div className="flex items-center justify-between">
        <h3 className="flex items-center gap-2 text-sm font-semibold">
          <Users className="h-4 w-4" /> Per-contributor actions
        </h3>
        <CopyButton text={data.markdown} label="Copy all as markdown" />
      </div>
      {data.groups.map((g) => (
        <GroupCard key={g.contributor_pk || g.contributor_code} group={g} />
      ))}
    </div>
  )
}
