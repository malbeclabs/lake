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
    date: 'January 31, 2026',
    changes: [
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
