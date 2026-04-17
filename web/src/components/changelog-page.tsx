import { FileText } from 'lucide-react'

interface ChangelogEntry {
  date: string
  changes: {
    type: 'feature' | 'improvement' | 'fix'
    description: string
  }[]
}

const changelog: ChangelogEntry[] = [
  {
    date: 'April 17, 2026',
    changes: [
      { type: 'improvement', description: 'Shreds scoreboard optimized for mobile' },
      { type: 'improvement', description: 'Edge scoreboard migrated to v2 slot feed race data' },
      { type: 'fix', description: 'Expiring seat threshold aligned between API and UI' },
    ],
  },
  {
    date: 'April 16, 2026',
    changes: [
      { type: 'improvement', description: 'UI polish and responsiveness improvements across components' },
      { type: 'improvement', description: 'Shreds scoreboard accessible without authentication' },
      { type: 'improvement', description: 'Scoreboard win rate computed as share of tracked-feed wins' },
      { type: 'improvement', description: 'Faster detail page queries via primary key filtering' },
      { type: 'fix', description: 'Partial-host gaps hidden in scoreboard live view' },
    ],
  },
  {
    date: 'April 15, 2026',
    changes: [
      { type: 'improvement', description: 'Edge scoreboard renamed to "Shreds Scoreboard" with smarter back link' },
      { type: 'improvement', description: 'Subscribe Now CTA on edge scoreboard' },
      { type: 'improvement', description: 'Live tail runway extended to ~25 minutes with deep seed anchoring' },
    ],
  },
  {
    date: 'April 14, 2026',
    changes: [
      { type: 'improvement', description: 'Edge scoreboard live tail reworked with faster cache refresh' },
      { type: 'improvement', description: 'Arrow navigation replaces drag scroll on edge scoreboard' },
      { type: 'improvement', description: 'Hover tooltips on publisher stats in scoreboard hero' },
      { type: 'improvement', description: 'Rx/Tx toggle on device drill-down traffic charts' },
      { type: 'fix', description: 'Live slot streaming stoppage from concurrent poll race' },
      { type: 'fix', description: 'Scoreboard cache refresh error handling and sinceSlot cursor' },
    ],
  },
  {
    date: 'April 12, 2026',
    changes: [
      { type: 'feature', description: 'P50/P90/P95/P99 percentile modes in traffic charts' },
      { type: 'feature', description: 'Drained links shown on topology map' },
      { type: 'improvement', description: 'Server-side filtering, sorting, and pagination on all listing pages' },
      { type: 'improvement', description: 'Deleted users supported in listings and detail pages' },
      { type: 'improvement', description: 'Active incident types shown on drained links' },
      { type: 'improvement', description: 'Faster topology initial map load' },
      { type: 'fix', description: 'Burstiness query crash when filtering by contributor or metro' },
      { type: 'fix', description: 'has_issues filter no longer over-matches no_data' },
    ],
  },
  {
    date: 'April 10, 2026',
    changes: [
      { type: 'feature', description: 'Edge scoreboard reworked with live slot tailing, drag-to-scroll, and hover tooltips' },
      { type: 'improvement', description: 'Stake share delta removed from status page' },
      { type: 'fix', description: 'Path latency average improvement calculation' },
      { type: 'fix', description: 'Counter regression inflating bps after stale telemetry' },
    ],
  },
  {
    date: 'April 8, 2026',
    changes: [
      { type: 'feature', description: 'CYOA tab in spike detection' },
      { type: 'improvement', description: 'Bucketed win rate view in edge scoreboard slot chart' },
      { type: 'improvement', description: 'Expiring seat threshold tightened to less than one prepaid epoch' },
      { type: 'improvement', description: 'USDC label on shreds device price column' },
    ],
  },
  {
    date: 'April 7, 2026',
    changes: [
      { type: 'feature', description: 'Sigma aggregate toggle in traffic dashboards' },
      { type: 'improvement', description: 'Aggregate toggle state persisted in URL' },
      { type: 'improvement', description: 'Client and version filters on publisher check' },
      { type: 'improvement', description: 'Compact MCP get_schema output' },
      { type: 'fix', description: 'Re-activated links no longer hidden behind drained toggle in health timeline' },
      { type: 'fix', description: 'Inflated rates from interleaved event-only rows' },
    ],
  },
  {
    date: 'April 6, 2026',
    changes: [
      { type: 'feature', description: 'Device interface types integrated across dashboards' },
      { type: 'improvement', description: 'Real-time ISIS state used for timeline collecting bucket' },
      { type: 'fix', description: 'ISIS down false positives and carryforward propagation issues' },
    ],
  },
  {
    date: 'April 5, 2026',
    changes: [
      { type: 'feature', description: 'Shreds pages overhauled with status lifecycle, devices endpoint, and nav reorganization' },
      { type: 'improvement', description: 'SLA renamed to SLO across UI and API' },
      { type: 'improvement', description: 'Read-only query validation enforced on MCP and query endpoints' },
      { type: 'improvement', description: 'Sub-5m buckets supported on link latency charts' },
      { type: 'improvement', description: 'RTT chart on status page for high latency links' },
      { type: 'fix', description: 'Escrow event balance showing $0 for null values' },
    ],
  },
  {
    date: 'April 4, 2026',
    changes: [
      { type: 'feature', description: 'Shred payment escrow activity page' },
      { type: 'improvement', description: 'Last activity column and clickable rows on seats page' },
      { type: 'improvement', description: 'Copy icons and dollar formatting on seats and activity pages' },
      { type: 'improvement', description: 'Collecting bucket data shown in detail charts' },
    ],
  },
  {
    date: 'April 2, 2026',
    changes: [
      { type: 'feature', description: 'Shred subscription program indexing and pages' },
      { type: 'feature', description: 'Unified link and device metrics endpoints with detail page charts' },
      { type: 'improvement', description: 'Delay override used in path computation with committed latency display' },
      { type: 'improvement', description: 'Committed RTT shown for all configured links' },
      { type: 'improvement', description: 'UI polish for charts, sidebar, and latency toggles' },
    ],
  },
  {
    date: 'April 1, 2026',
    changes: [
      { type: 'improvement', description: 'Landing page replaced with status redirect and MCP CTA added to chat' },
    ],
  },
  {
    date: 'March 30, 2026',
    changes: [
      { type: 'feature', description: 'Shred stats chart on multicast group detail page' },
      { type: 'improvement', description: 'Info tooltip on traffic chart on multicast group page' },
      { type: 'fix', description: 'Publisher check includes non-publishing validators in totals' },
      { type: 'fix', description: 'Traffic chart x-axis gap when data ingestion lags' },
      { type: 'fix', description: 'Status banner no longer escalates when all links are individually healthy' },
    ],
  },
  {
    date: 'March 29, 2026',
    changes: [
      { type: 'feature', description: 'Devnet and testnet database connections enabled by default' },
      { type: 'improvement', description: 'Aggregate packet loss removed from status banner' },
      { type: 'improvement', description: 'Row deselection in spike detection table' },
      { type: 'fix', description: 'Link latency chart x-axis spans full time range' },
      { type: 'fix', description: 'ISIS adjacencies disambiguated on duplicate tunnel nets' },
    ],
  },
  {
    date: 'March 27, 2026',
    changes: [
      { type: 'improvement', description: 'Remaining queries migrated to rollup tables for faster loads' },
      { type: 'fix', description: 'Spurious No Data badge on detail pages during collection' },
    ],
  },
  {
    date: 'March 25, 2026',
    changes: [
      { type: 'feature', description: 'Link latency dashboard page' },
      { type: 'improvement', description: 'Device and user detail pages modernized with uPlot charts' },
      { type: 'improvement', description: 'Spike detection redesigned with anomaly-based detection' },
      { type: 'improvement', description: 'Timeline histogram migrated off recharts' },
      { type: 'improvement', description: 'Legend interactions added to link latency chart' },
      { type: 'fix', description: 'User detail chart not showing data' },
      { type: 'fix', description: 'Publisher check and multicast traffic for renamed shred group' },
    ],
  },
  {
    date: 'March 24, 2026',
    changes: [
      { type: 'improvement', description: 'Replace recharts with uPlot on status page charts' },
      { type: 'improvement', description: 'Server-side listing filters with multi-filter support' },
      { type: 'improvement', description: 'Rollup table migration for faster incidents, status, and timeline pages' },
      { type: 'fix', description: 'Inflated traffic rates on validator and user listings' },
    ],
  },
  {
    date: 'March 19, 2026',
    changes: [
      { type: 'feature', description: 'ISIS topology indexed and surfaced across UI' },
      { type: 'improvement', description: 'Committed latency used for path latency and k-shortest path finding' },
      { type: 'improvement', description: 'Entity public key on detail pages with copy-to-clipboard' },
      { type: 'improvement', description: 'Fade chart lines on hover over metric selectors' },
      { type: 'improvement', description: 'Disable query polling in background tabs' },
      { type: 'fix', description: 'Google sign-in for Brave browser' },
      { type: 'fix', description: 'ClickHouse connection pool exhaustion under load' },
    ],
  },
  {
    date: 'March 17, 2026',
    changes: [
      { type: 'feature', description: 'Edge scoreboard page' },
      { type: 'feature', description: 'Real-time slot view and condition filters on publisher check' },
      { type: 'improvement', description: 'Page caching for edge scoreboard and publisher check' },
      { type: 'improvement', description: 'Exclude loss probes from DZ latency comparison averages' },
      { type: 'fix', description: 'Internal-only pages redirecting on refresh' },
    ],
  },
  {
    date: 'March 13, 2026',
    changes: [
      { type: 'feature', description: 'Tabular legends with values on latency and link status charts' },
      { type: 'improvement', description: 'Provisioning links in disabled table and improved collecting cells' },
      { type: 'improvement', description: 'Devices marked unhealthy when not sending latency probes' },
      { type: 'fix', description: 'No-data classification for one-sided and collecting buckets' },
      { type: 'fix', description: 'Inflated on-DZ validator count in performance view' },
    ],
  },
  {
    date: 'March 11, 2026',
    changes: [
      { type: 'feature', description: 'Ledger dashboard pages for Solana and DoubleZero' },
      { type: 'improvement', description: 'Soft-drained and hard-drained links differentiated with stripe patterns' },
      { type: 'improvement', description: 'Links with 1000ms committed RTT classified as provisioning' },
      { type: 'fix', description: 'Stake share delta calculations and publisher shreds count' },
    ],
  },
  {
    date: 'March 9, 2026',
    changes: [
      { type: 'feature', description: 'Redesigned path latency page' },
      { type: 'feature', description: 'FCS errors as a separate filter and metric' },
      { type: 'improvement', description: 'Incident grouping by link with temporal deduplication' },
      { type: 'improvement', description: 'Validator name, stake, and search on publisher check' },
      { type: 'fix', description: 'Inflated issue duration on status banner' },
      { type: 'fix', description: 'Ongoing incidents incorrectly showing as resolved' },
    ],
  },
  {
    date: 'March 8, 2026',
    changes: [
      { type: 'feature', description: 'Incidents page with link and device scopes' },
      { type: 'feature', description: 'Down link classification in overall status banner' },
      { type: 'improvement', description: 'Simplified link status classifications and drained handling' },
      { type: 'fix', description: 'Dark mode visibility for issue tags' },
    ],
  },
  {
    date: 'March 5, 2026',
    changes: [
      { type: 'feature', description: 'Validators.app data ingestion and UI' },
      { type: 'feature', description: 'Publisher check page for bebop group members' },
      { type: 'improvement', description: 'In/out shown independently for interface errors and discards' },
      { type: 'improvement', description: 'Row highlighting when navigating from status summary tables' },
      { type: 'improvement', description: 'Merged ongoing outages per link with simplified type display' },
    ],
  },
  {
    date: 'March 1, 2026',
    changes: [
      { type: 'feature', description: 'Metro filter on path latency page' },
      { type: 'improvement', description: 'Server-side multicast tree segments and animation performance' },
    ],
  },
  {
    date: 'February 27, 2026',
    changes: [
      { type: 'feature', description: 'Multicast group member count chart' },
      { type: 'improvement', description: 'Server-side pagination for multicast group members' },
      { type: 'improvement', description: 'Soft-drained links excluded from status page down classification' },
      { type: 'fix', description: 'Y-axis scale cut off in traffic and interface charts' },
      { type: 'fix', description: 'Down links not appearing in status page link history' },
    ],
  },
  {
    date: 'February 24, 2026',
    changes: [
      { type: 'improvement', description: 'Improved multicast group detail page interactivity' },
      { type: 'improvement', description: 'Improved gossip node detail page DZ info' },
      { type: 'improvement', description: 'Improved chart legends and detail page layout' },
      { type: 'fix', description: 'Maintenance planner metric-based routing' },
      { type: 'fix', description: 'Legend hover highlighting multiple table rows' },
    ],
  },
  {
    date: 'February 21, 2026',
    changes: [
      { type: 'feature', description: 'Outage severity classification' },
      { type: 'feature', description: 'Validator disconnection detection in timeline' },
      { type: 'improvement', description: 'Inline search replacing spotlight filters across all pages' },
      { type: 'improvement', description: 'Multicast groups added to search and autocomplete' },
      { type: 'fix', description: 'False issue badges and high latency classification on status page' },
    ],
  },
  {
    date: 'February 20, 2026',
    changes: [
      { type: 'feature', description: 'Redesigned multicast overlay and multicast group pages' },
    ],
  },
  {
    date: 'February 19, 2026',
    changes: [
      { type: 'feature', description: 'Service management dashboard' },
      { type: 'improvement', description: 'Polished sidebar navigation and performance pages' },
    ],
  },
  {
    date: 'February 17, 2026',
    changes: [
      { type: 'feature', description: 'Traffic overview dashboard' },
      { type: 'feature', description: 'Fullscreen toggle for topology views' },
      { type: 'improvement', description: 'Tunnel traffic user kind filter and grouping' },
      { type: 'improvement', description: 'Graceful fallback when WebGL/GPU is unavailable' },
    ],
  },
  {
    date: 'February 15, 2026',
    changes: [
      { type: 'feature', description: 'Traffic analytics dashboard with aggregate stress charts, per-group localization, top interfaces table, drilldown panel, spike detection, and capacity planning' },
      { type: 'feature', description: 'Bidirectional Rx/Tx charts across overview and interfaces pages with drag-to-zoom time range selection' },
    ],
  },
  {
    date: 'February 14, 2026',
    changes: [
      { type: 'feature', description: 'Redesigned multicast overlay with per-publisher tree paths and animated flow dots' },
      { type: 'improvement', description: 'Vibrant default topology colors and animated link dots across all views' },
    ],
  },
  {
    date: 'February 13, 2026',
    changes: [
      { type: 'feature', description: '3D globe view for topology page' },
    ],
  },
  {
    date: 'February 10, 2026',
    changes: [
      { type: 'improvement', description: 'Agent flags links as down when recent 5 minutes show 100% packet loss' },
      { type: 'fix', description: 'Connection pool exhaustion from concurrent cache refreshes' },
    ],
  },
  {
    date: 'February 6, 2026',
    changes: [
      { type: 'fix', description: 'Multiple filters not being applied on entity pages' },
    ],
  },
  {
    date: 'February 5, 2026',
    changes: [
      { type: 'feature', description: 'MCP server for using DoubleZero Data tools in MCP clients' },
      { type: 'improvement', description: 'Multi-hop latency and metro path query reliability in agent' },
    ],
  },
  {
    date: 'February 4, 2026',
    changes: [
      { type: 'fix', description: 'Chat no longer incorrectly sums in+out when reporting link utilization' },
      { type: 'improvement', description: 'Device selector in path finder now searches by metro name (e.g. "Tokyo") in addition to metro code' },
    ],
  },
  {
    date: 'February 3, 2026',
    changes: [
      { type: 'feature', description: 'Committed latency and override latency columns on links page' },
      { type: 'improvement', description: 'Chat shows "Preparing answer..." during synthesis instead of appearing idle' },
      { type: 'improvement', description: 'Sticky headers and improved drawer behavior on path latency page' },
      { type: 'fix', description: 'Latency showing 0ms when there is 100% packet loss' },
    ],
  },
  {
    date: 'February 2, 2026',
    changes: [
      { type: 'fix', description: 'Chat responses intermittently disappearing after streaming' },
    ],
  },
  {
    date: 'February 1, 2026',
    changes: [
      { type: 'feature', description: 'Multi-environment support for querying devnet and testnet data' },
    ],
  },
  {
    date: 'January 31, 2026',
    changes: [
      { type: 'feature', description: 'Timeline preset filters and DZ stake attribution events' },
      { type: 'improvement', description: 'Redesigned timeline page with vertical layout and collapsible filters' },
      { type: 'fix', description: 'Version check always showing update available' },
    ],
  },
  {
    date: 'January 30, 2026',
    changes: [
      { type: 'feature', description: 'Terms of use page and chat disclaimer' },
      { type: 'feature', description: 'Discards graph on traffic page' },
    ],
  },
  {
    date: 'January 29, 2026',
    changes: [
      { type: 'feature', description: 'Copy button on chat responses with rich text support for Slack and Notion' },
      { type: 'feature', description: 'Slack bot with self-serve OAuth install and interactive query progress' },
    ],
  },
  {
    date: 'January 28, 2026',
    changes: [
      { type: 'feature', description: 'Health graphs on device and link status pages' },
      { type: 'feature', description: 'Traffic charts page with unified detail views' },
      { type: 'fix', description: 'Fix query timeout crashes and stale cache overwrites' },
    ],
  },
  {
    date: 'January 27, 2026',
    changes: [
      { type: 'fix', description: 'Validators incorrectly showing all as on DZ' },
      { type: 'fix', description: 'Outage queries causing high memory usage and connection pool exhaustion' },
    ],
  },
  {
    date: 'January 26, 2026',
    changes: [
      { type: 'feature', description: 'Multicast trees visualization' },
      { type: 'fix', description: 'Negative counter deltas in traffic queries filtered out' },
    ],
  },
  {
    date: 'January 25, 2026',
    changes: [
      { type: 'feature', description: 'Device health issues shown in status banner with expandable per-interface charts' },
      { type: 'feature', description: 'Metro-to-metro path finding with multi-path comparison' },
      { type: 'improvement', description: 'Searchable device selector and reverse path option for path finding' },
      { type: 'improvement', description: 'Interface charts show in/out traffic separately on +/- axis' },
      { type: 'improvement', description: 'Consistent device health thresholds with issue breakdown in popover' },
    ],
  },
  {
    date: 'January 23, 2026',
    changes: [
      { type: 'feature', description: 'Telemetry stopped indicator in status page summary' },
      { type: 'feature', description: 'Device and interface info shown on link info panel and hover tooltip' },
      { type: 'improvement', description: 'Unified what-if removal analysis for maintenance planner' },
    ],
  },
  {
    date: 'January 22, 2026',
    changes: [
      { type: 'feature', description: 'Multi-device failure analysis with combined impact view' },
      { type: 'feature', description: 'Sortable, filterable tables on all entity pages with autocomplete' },
      { type: 'feature', description: 'IS-IS delay override tracking in link timeline' },
      { type: 'improvement', description: 'Expandable per-device breakdown showing affected paths and disconnected devices' },
      { type: 'improvement', description: 'Timeline filter state persisted in URL for shareable links' },
      { type: 'improvement', description: 'Zoom and pan to selected items when loading topology from URL' },
      { type: 'improvement', description: 'Suggested questions refresh when clicking Chat in navigation' },
      { type: 'improvement', description: 'Grace period before showing update notifications' },
      { type: 'fix', description: 'Path finding mode now toggles off when clicking active control' },
      { type: 'fix', description: 'Link removal panel now shows latency impact first' },
      { type: 'fix', description: 'Blank page when navigating to new query sessions' },
      { type: 'fix', description: 'Query progress spinner not updating to completed state' },
    ],
  },
  {
    date: 'January 21, 2026',
    changes: [
      { type: 'feature', description: 'Soft drained links list in status page header' },
      { type: 'feature', description: 'Pin toggle for current outages on outages page' },
      { type: 'improvement', description: 'Transpacific links draw across the Pacific Ocean on topology map' },
      { type: 'improvement', description: 'Better zoom behavior when selecting links on topology' },
      { type: 'improvement', description: 'Wider name column on links status history table' },
      { type: 'improvement', description: 'Metro and contributor shown on device utilization cards' },
      { type: 'improvement', description: 'Sidebar no longer auto-collapses in topology unless already collapsed' },
      { type: 'fix', description: 'Sidebar state pollution from topology auto-collapse' },
      { type: 'fix', description: 'Issue thresholds for errors and discards' },
    ],
  },
  {
    date: 'January 14, 2026',
    changes: [
      { type: 'feature', description: 'DZ vs Internet performance comparison' },
      { type: 'feature', description: 'Path latency analysis' },
      { type: 'feature', description: 'Maintenance planner' },
      { type: 'feature', description: 'Redundancy analysis for network resilience' },
      { type: 'feature', description: 'Metro connectivity report' },
    ],
  },
  {
    date: 'January 10, 2026',
    changes: [
      { type: 'feature', description: 'Network topology map and graph visualization' },
      { type: 'feature', description: 'Path calculator for analyzing network routes' },
    ],
  },
  {
    date: 'January 6, 2026',
    changes: [
      { type: 'feature', description: 'Neo4j graph database support' },
      { type: 'feature', description: 'Cypher query support' },
    ],
  },
  {
    date: 'January 2, 2026',
    changes: [
      { type: 'feature', description: 'Outages tracker' },
    ],
  },
  {
    date: 'December 30, 2025',
    changes: [
      { type: 'feature', description: 'Network status dashboard' },
      { type: 'feature', description: 'Event timeline' },
      { type: 'feature', description: 'Device, link, metro, and contributor browsers' },
      { type: 'feature', description: 'Solana validator and gossip node browsers' },
    ],
  },
  {
    date: 'December 26, 2025',
    changes: [
      { type: 'feature', description: 'AI-powered chat for natural language network queries' },
      { type: 'feature', description: 'SQL query editor' },
    ],
  },
  {
    date: 'December 24, 2025',
    changes: [
      { type: 'feature', description: 'Slack bot interface for AI agent' },
    ],
  },
  {
    date: 'December 23, 2025',
    changes: [
      { type: 'feature', description: 'ClickHouse data indexer for DZ network telemetry' },
      { type: 'feature', description: 'Solana RPC data ingestion' },
      { type: 'feature', description: 'AI agent with tool-calling for natural language queries' },
    ],
  },
]

function ChangeTypeBadge({ type }: { type: 'feature' | 'improvement' | 'fix' }) {
  const styles = {
    feature: 'bg-green-500/10 text-green-500 border-green-500/20',
    improvement: 'bg-blue-500/10 text-blue-500 border-blue-500/20',
    fix: 'bg-orange-500/10 text-orange-500 border-orange-500/20',
  }

  const labels = {
    feature: 'New',
    improvement: 'Improved',
    fix: 'Fixed',
  }

  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 text-xs font-medium rounded border ${styles[type]}`}
    >
      {labels[type]}
    </span>
  )
}

export function ChangelogPage() {
  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-3xl mx-auto px-6 py-8">
        <div className="flex items-center gap-3 mb-8">
          <div className="p-2 rounded-lg bg-primary/10">
            <FileText className="h-6 w-6 text-primary" />
          </div>
          <div>
            <h1 className="text-2xl font-semibold">Changelog</h1>
            <p className="text-sm text-muted-foreground">What's new</p>
          </div>
        </div>

        <div className="space-y-10">
          {changelog.map((entry) => (
            <div key={entry.date}>
              <h2 className="text-lg font-semibold mb-4">{entry.date}</h2>
              <div className="space-y-3 pl-4 border-l-2 border-border">
                {entry.changes.map((change, i) => (
                  <div key={i} className="flex items-start gap-3">
                    <ChangeTypeBadge type={change.type} />
                    <span className="text-sm text-foreground/90">{change.description}</span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
