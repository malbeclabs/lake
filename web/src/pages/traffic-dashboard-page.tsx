import { DashboardProvider, useDashboard } from '@/components/traffic-dashboard/dashboard-context'
import { DashboardFilters } from '@/components/traffic-dashboard/dashboard-filters'
import { Section } from '@/components/traffic-dashboard/section'
import { StressPanel } from '@/components/traffic-dashboard/stress-panel'
import { LocalizationPanel } from '@/components/traffic-dashboard/localization-panel'
import { TopDevicesPanel, TopInterfacesPanel } from '@/components/traffic-dashboard/attribution-panel'
import { DrilldownPanel } from '@/components/traffic-dashboard/drilldown-panel'
import { BurstinessPanel } from '@/components/traffic-dashboard/burstiness-panel'
import { CapacityPanel } from '@/components/traffic-dashboard/capacity-panel'

function DashboardContent() {
  const { selectedEntity, pinnedEntities, timeRange, metric } = useDashboard()
  const showCapacity = ['7d', '14d', '30d'].includes(timeRange)
  const isUtil = metric === 'utilization'

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <div className="flex items-center justify-between gap-4 mb-6 flex-wrap">
          <h1 className="text-2xl font-bold">Traffic Dashboard</h1>
          <DashboardFilters />
        </div>

        <div className="space-y-4">
          <Section
            title={isUtil ? 'System Stress' : 'Aggregate Throughput'}
            description={isUtil
              ? 'P50, P95, and max utilization across all interfaces per time bucket. Spikes indicate widespread congestion.'
              : 'P50, P95, and max throughput across all interfaces per time bucket.'}
          >
            <StressPanel />
          </Section>

          <Section
            title={isUtil ? 'Utilization by Group' : 'Throughput by Group'}
            description={isUtil
              ? 'Average P95 utilization per group. Click a group to filter the panels below.'
              : 'Average P95 throughput per group. Click a group to filter the panels below.'}
          >
            <LocalizationPanel />
          </Section>

          <div className="grid grid-cols-2 gap-4 items-start">
            <Section
              title="Top Devices"
              description="Devices ranked by peak aggregate throughput. Click a row to drill down."
            >
              <TopDevicesPanel />
            </Section>
            <Section
              title="Top Interfaces"
              description={isUtil
                ? 'Interfaces ranked by utilization. Click a row to drill down.'
                : 'Interfaces ranked by peak throughput. Click a row to drill down.'}
            >
              <TopInterfacesPanel />
            </Section>
          </div>

          {(selectedEntity || pinnedEntities.length > 0) && (
            <Section
              title="Drilldown"
              description="Rx and Tx traffic for the selected entity. Pin multiple entities to compare side by side."
            >
              <DrilldownPanel />
            </Section>
          )}

          <Section
            title="Spike Detection"
            description="Interfaces with the largest gap between typical (P50) and peak (P99) utilization. Large gaps indicate bursty traffic."
          >
            <BurstinessPanel />
          </Section>

          {showCapacity && (
            <Section
              title="Capacity Planning"
              description="Interfaces ranked by sustained P95 utilization over the selected window. Use longer time ranges (7d+) for meaningful trends."
              defaultOpen={false}
            >
              <CapacityPanel />
            </Section>
          )}
        </div>
      </div>
    </div>
  )
}

export function TrafficDashboardPage() {
  return (
    <DashboardProvider>
      <DashboardContent />
    </DashboardProvider>
  )
}
