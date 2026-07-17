import { useState } from 'react'
import { useIsOpsUser } from '@/hooks/use-is-ops-user'
import { PlannerProvider, usePlanner } from '@/components/topology/planner/PlannerContext'
import { PlannerToolbar } from '@/components/topology/planner/PlannerToolbar'
import { PlannerMap } from '@/components/topology/planner/PlannerMap'
import { PlannerLanding } from '@/components/topology/planner/PlannerLanding'
import { ChangesPanel } from '@/components/topology/planner/ChangesPanel'
import { ConflictBanner } from '@/components/topology/planner/ConflictBanner'
import { ActionListPanel } from '@/components/topology/planner/ActionListPanel'
import { PlannerImpactTab } from '@/components/topology/planner/PlannerImpactTab'

type RightPanelTab = 'changes' | 'actions' | 'impact'

const RIGHT_PANEL_TABS: { key: RightPanelTab; label: string }[] = [
  { key: 'changes', label: 'Changes' },
  { key: 'actions', label: 'Action List' },
  { key: 'impact', label: 'Impact' },
]

function RightPanel({ planId }: { planId: string }) {
  const [activeTab, setActiveTab] = useState<RightPanelTab>('changes')

  return (
    <div className="w-80 border-l border-border shrink-0 flex flex-col overflow-hidden">
      <div className="flex border-b border-border shrink-0">
        {RIGHT_PANEL_TABS.map(({ key, label }) => (
          <button
            key={key}
            type="button"
            onClick={() => setActiveTab(key)}
            className={`px-3 py-1.5 text-xs font-medium border-b-2 transition-colors -mb-px ${
              activeTab === key
                ? 'border-purple-500 text-purple-500'
                : 'border-transparent text-muted-foreground hover:text-foreground'
            }`}
          >
            {label}
          </button>
        ))}
      </div>
      <div className="flex-1 overflow-y-auto">
        {activeTab === 'changes' && <ChangesPanel />}
        {activeTab === 'actions' && <ActionListPanel planId={planId} />}
        {activeTab === 'impact' && <PlannerImpactTab />}
      </div>
    </div>
  )
}

function PlannerLayout() {
  const { plan, loading } = usePlanner()

  return (
    <div className="flex-1 flex flex-col bg-background overflow-hidden">
      <PlannerToolbar />
      <ConflictBanner />
      {loading ? (
        <div className="flex-1 flex items-center justify-center text-sm text-muted-foreground">
          Loading topology…
        </div>
      ) : !plan ? (
        <PlannerLanding />
      ) : (
        <div className="flex-1 flex min-h-0">
          <div className="flex-1 relative min-w-0">
            <PlannerMap />
          </div>
          <RightPanel planId={plan.id} />
        </div>
      )}
    </div>
  )
}

export function TopologyPlannerPage() {
  const isOps = useIsOpsUser()

  if (!isOps) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <h1 className="text-lg font-semibold">Not authorized</h1>
          <p className="text-sm text-muted-foreground mt-1">
            The Topology Planner is available to DoubleZero operators only.
          </p>
        </div>
      </div>
    )
  }

  return (
    <PlannerProvider>
      <PlannerLayout />
    </PlannerProvider>
  )
}
