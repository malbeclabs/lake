import { BarChart3 } from 'lucide-react'
import { DashboardProvider, useDashboard } from '@/components/traffic-dashboard/dashboard-context'
import { DashboardFilters, DashboardFilterBadges } from '@/components/traffic-dashboard/dashboard-filters'
import { PageHeader } from '@/components/page-header'
import { Section } from '@/components/traffic-dashboard/section'
import { StressPanel } from '@/components/traffic-dashboard/stress-panel'
import { LocalizationPanel } from '@/components/traffic-dashboard/localization-panel'
import { TopDevicesPanel, TopInterfacesPanel } from '@/components/traffic-dashboard/attribution-panel'
import { DrilldownPanel } from '@/components/traffic-dashboard/drilldown-panel'
import { BurstinessPanel } from '@/components/traffic-dashboard/burstiness-panel'
import { HealthPanel } from '@/components/traffic-dashboard/health-panel'
import { CapacityPanel } from '@/components/traffic-dashboard/capacity-panel'

function DashboardContent() {
  const { selectedEntity, pinnedEntities, metric, intfType } = useDashboard()
  const showCapacity = intfType !== 'tunnel' && intfType !== 'other'
  const isUtil = metric === 'utilization'

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={BarChart3}
          title="Traffic Overview"
          actions={<DashboardFilters />}
        />
        <div className="-mt-3 mb-6">
          <DashboardFilterBadges />
        </div>

        <div className="space-y-4">
          <Section
            title={isUtil ? 'System Stress' : metric === 'packets' ? 'Aggregate Packet Rate' : 'Aggregate Throughput'}
            description={isUtil
              ? 'P50, P95, and max utilization across all interfaces per time bucket. Spikes indicate widespread congestion.'
              : metric === 'packets'
                ? 'P50, P95, and max packet rate across all interfaces per time bucket.'
                : 'P50, P95, and max throughput across all interfaces per time bucket.'}
          >
            <StressPanel />
          </Section>

          <Section
            title={isUtil ? 'Utilization by Group' : metric === 'packets' ? 'Packet Rate by Group' : 'Throughput by Group'}
            description={isUtil
              ? 'Average P95 utilization per group. Click a group to filter the panels below.'
              : metric === 'packets'
                ? 'Average P95 packet rate per group. Click a group to filter the panels below.'
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
                : metric === 'packets'
                  ? 'Interfaces ranked by peak packet rate. Click a row to drill down.'
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
            description="Interfaces with the largest gap between typical (P50) and peak (P99) traffic levels. Large gaps indicate bursty traffic."
          >
            <BurstinessPanel />
          </Section>

          <Section
            title="Interface Health"
            description="Interfaces with errors, discards, or carrier transitions in the selected time window."
          >
            <HealthPanel />
          </Section>

          {showCapacity && (
            <Section
              title="Capacity Planning"
              description="Interfaces ranked by sustained P95 utilization over the selected window. Best with 24h+ for stable trends."
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
