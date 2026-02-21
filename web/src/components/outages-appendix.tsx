import { ArrowLeft } from 'lucide-react'
import { Link } from 'react-router-dom'

export function OutagesAppendix() {
  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-4xl mx-auto px-4 sm:px-8 py-8">
        {/* Header */}
        <div className="mb-8">
          <Link
            to="/outages"
            className="inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground mb-4"
          >
            <ArrowLeft className="h-4 w-4" />
            Back to Outages
          </Link>
          <h1 className="text-2xl font-semibold">How Outages Work</h1>
          <p className="text-muted-foreground mt-2">
            This page explains how outage events are identified, classified, and filtered
            on the Link Outages page. This is one interpretation of the available network data
            and does not represent the formal methodology used for rewards distribution or calculation.
          </p>
        </div>

        {/* Outage Types */}
        <section className="mb-12">
          <h2 className="text-xl font-semibold mb-6 pb-2 border-b-2 border-border">Outage Types</h2>
          <p className="text-sm text-muted-foreground mb-4">
            Three distinct outage types are tracked. Each uses different data sources and detection logic.
          </p>

          <div className="space-y-4">
            <div className="border border-border rounded-lg p-4">
              <div className="flex items-center gap-2 mb-2">
                <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-slate-100 text-slate-800 dark:bg-slate-800 dark:text-slate-200">
                  Status
                </span>
                <h3 className="font-medium">Status Changes (Drained)</h3>
              </div>
              <p className="text-sm text-muted-foreground">
                A link transitions to <code className="bg-muted px-1 py-0.5 rounded text-xs">soft-drained</code> or{' '}
                <code className="bg-muted px-1 py-0.5 rounded text-xs">hard-drained</code> from{' '}
                <code className="bg-muted px-1 py-0.5 rounded text-xs">activated</code>. The outage starts when the
                drain occurs and ends when the link returns to activated. Links currently drained always appear
                as ongoing outages regardless of when the drain started.
              </p>
            </div>

            <div className="border border-border rounded-lg p-4">
              <div className="flex items-center gap-2 mb-2">
                <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200">
                  Packet Loss
                </span>
                <h3 className="font-medium">Packet Loss Exceeding Threshold</h3>
              </div>
              <p className="text-sm text-muted-foreground">
                Link packet loss exceeds the configured threshold for 2 or more consecutive 5-minute buckets.
                Loss is measured per bucket as the percentage of probes that were lost or had zero RTT.
                Buckets with fewer than 3 samples are excluded.
              </p>
            </div>

            <div className="border border-border rounded-lg p-4">
              <div className="flex items-center gap-2 mb-2">
                <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200">
                  No Data
                </span>
                <h3 className="font-medium">Missing Telemetry</h3>
              </div>
              <p className="text-sm text-muted-foreground">
                No latency telemetry has been received for a link for 15+ minutes. Devices report latency
                measurements for each of their links — when those measurements stop arriving, the link is
                considered to have a no-data outage. Links with historical data but no recent data appear
                as ongoing no-data outages. Gaps within the time range that later resume are shown as
                completed outages. Gaps that overlap with drained periods are excluded (those are status
                outages instead).
              </p>
            </div>
          </div>
        </section>

        {/* Severity */}
        <section className="mb-12">
          <h2 className="text-xl font-semibold mb-6 pb-2 border-b-2 border-border">Severity Classification</h2>
          <p className="text-sm text-muted-foreground mb-4">
            Each outage is classified with a severity level based on its type and impact.
          </p>

          <div className="space-y-4">
            <div className="border border-border rounded-lg p-4">
              <div className="flex items-center gap-2 mb-2">
                <span className="inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200">
                  outage
                </span>
                <h3 className="font-medium">Outage</h3>
              </div>
              <ul className="text-sm text-muted-foreground space-y-1 ml-5 list-disc">
                <li>Packet loss with peak &ge; 10%</li>
                <li>All status changes (drained)</li>
                <li>All no-data events</li>
              </ul>
            </div>

            <div className="border border-border rounded-lg p-4">
              <div className="flex items-center gap-2 mb-2">
                <span className="inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-200">
                  degraded
                </span>
                <h3 className="font-medium">Degraded</h3>
              </div>
              <ul className="text-sm text-muted-foreground space-y-1 ml-5 list-disc">
                <li>Packet loss with peak between the threshold and 10%</li>
              </ul>
              <p className="text-sm text-muted-foreground mt-2">
                Only applies when using a threshold below 10% (e.g. 1%). At the default 10% threshold,
                all packet loss outages are classified as "outage" severity.
              </p>
            </div>
          </div>
        </section>

        {/* Filtering Logic */}
        <section className="mb-12">
          <h2 className="text-xl font-semibold mb-6 pb-2 border-b-2 border-border">Noise Filtering</h2>
          <p className="text-sm text-muted-foreground mb-4">
            Several filters are applied to reduce false positives and surface only meaningful outage events.
          </p>

          <div className="space-y-4">
            <div className="border border-border rounded-lg p-4">
              <h3 className="font-medium mb-2">Default Threshold: 10%</h3>
              <p className="text-sm text-muted-foreground">
                The default packet loss threshold is 10%. At 1%, a single lost packet out of 60 in a 5-minute
                bucket registers as 1.7% loss, which creates noise from normal network jitter. The 10% default
                ensures only significant loss events are surfaced. The threshold can be changed to 1% for
                deeper investigation.
              </p>
            </div>

            <div className="border border-border rounded-lg p-4">
              <h3 className="font-medium mb-2">Minimum Duration: 2 Consecutive Buckets (10 minutes)</h3>
              <p className="text-sm text-muted-foreground">
                A packet loss outage is only recorded if the loss exceeds the threshold for at least 2
                consecutive 5-minute buckets. Single-bucket spikes (isolated 5-minute blips) are discarded
                as transient noise. This applies to both completed outages in the time range and ongoing
                outage detection.
              </p>
            </div>

            <div className="border border-border rounded-lg p-4">
              <h3 className="font-medium mb-2">Outage Coalescing: 15-Minute Gap Merge</h3>
              <p className="text-sm text-muted-foreground">
                Packet loss outages on the same link that are separated by less than 15 minutes are merged
                into a single event. This prevents intermittent flapping from creating many separate outage
                entries. The merged outage uses the earliest start time, latest end time, and the maximum
                peak loss across all merged events.
              </p>
            </div>

            <div className="border border-border rounded-lg p-4">
              <h3 className="font-medium mb-2">Minimum Sample Count</h3>
              <p className="text-sm text-muted-foreground">
                5-minute buckets with fewer than 3 latency samples are excluded from loss calculations.
                This prevents low-sample-count buckets from producing unreliable loss percentages.
              </p>
            </div>
          </div>
        </section>

        {/* Detection Logic */}
        <section className="mb-12">
          <h2 className="text-xl font-semibold mb-6 pb-2 border-b-2 border-border">Detection Logic</h2>

          <div className="mb-8">
            <h3 className="text-lg font-semibold mb-4">Ongoing vs. Completed</h3>
            <p className="text-sm text-muted-foreground mb-4">
              Each outage type distinguishes between ongoing and completed events:
            </p>
            <ul className="text-sm text-muted-foreground space-y-2 ml-5 list-disc">
              <li>
                <strong>Ongoing outages</strong> are detected from current state (currently drained links,
                current high-loss links, links with no recent telemetry) and always appear regardless of the
                selected time range.
              </li>
              <li>
                <strong>Completed outages</strong> are detected from historical data within the selected
                time range. A completed outage has both a start and end time.
              </li>
            </ul>
          </div>

          <div className="mb-8">
            <h3 className="text-lg font-semibold mb-4">Packet Loss Outage Start</h3>
            <p className="text-sm text-muted-foreground">
              For ongoing packet loss outages, the system looks back up to 30 days to find when the loss
              first exceeded the threshold. It identifies the earliest bucket where loss went above threshold
              and hasn't dropped below since (accounting for the consecutive-bucket requirement).
            </p>
          </div>

          <div className="mb-8">
            <h3 className="text-lg font-semibold mb-4">No-Data Gap Detection</h3>
            <p className="text-sm text-muted-foreground">
              Completed no-data outages are detected by finding gaps of 15+ minutes between consecutive
              5-minute telemetry buckets. Gaps that overlap with drained periods are filtered out, since
              those indicate intentional drain operations rather than unexpected data loss.
            </p>
          </div>
        </section>

        {/* Controls */}
        <section className="mb-12">
          <h2 className="text-xl font-semibold mb-6 pb-2 border-b-2 border-border">Page Controls</h2>

          <div className="overflow-x-auto">
            <table className="w-full text-sm border border-border rounded-lg">
              <thead>
                <tr className="bg-muted/50">
                  <th className="px-4 py-2 text-left font-medium border-b border-border">Control</th>
                  <th className="px-4 py-2 text-left font-medium border-b border-border">Default</th>
                  <th className="px-4 py-2 text-left font-medium border-b border-border">Description</th>
                </tr>
              </thead>
              <tbody className="text-muted-foreground">
                <tr>
                  <td className="px-4 py-2 border-b border-border font-medium text-foreground">Time Range</td>
                  <td className="px-4 py-2 border-b border-border">24h</td>
                  <td className="px-4 py-2 border-b border-border">How far back to look for completed outages (3h to 30d)</td>
                </tr>
                <tr>
                  <td className="px-4 py-2 border-b border-border font-medium text-foreground">Threshold</td>
                  <td className="px-4 py-2 border-b border-border">10%</td>
                  <td className="px-4 py-2 border-b border-border">Packet loss percentage that triggers a packet loss outage</td>
                </tr>
                <tr>
                  <td className="px-4 py-2 border-b border-border font-medium text-foreground">Type</td>
                  <td className="px-4 py-2 border-b border-border">All</td>
                  <td className="px-4 py-2 border-b border-border">Filter to a specific outage type (Status, Packet Loss, No Data)</td>
                </tr>
                <tr>
                  <td className="px-4 py-2 font-medium text-foreground">Filters</td>
                  <td className="px-4 py-2">None</td>
                  <td className="px-4 py-2">Filter by metro, link, contributor, or device</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>

        {/* Data Sources */}
        <section className="mb-10">
          <h2 className="text-xl font-semibold mb-6 pb-2 border-b-2 border-border">Data Sources</h2>

          <h3 className="font-medium mt-4 mb-2">Views</h3>
          <ul className="text-sm text-muted-foreground space-y-2 ml-5 list-disc">
            <li><code className="bg-muted px-1 py-0.5 rounded text-xs">dz_links_current</code> — Current link status and metadata</li>
            <li><code className="bg-muted px-1 py-0.5 rounded text-xs">dz_link_status_changes</code> — Historical status transitions for links</li>
            <li><code className="bg-muted px-1 py-0.5 rounded text-xs">dz_devices_current</code> — Device metadata for metro resolution and device filtering</li>
            <li><code className="bg-muted px-1 py-0.5 rounded text-xs">dz_metros_current</code> — Metro code lookups</li>
            <li><code className="bg-muted px-1 py-0.5 rounded text-xs">dz_contributors_current</code> — Contributor code lookups</li>
          </ul>

          <h3 className="font-medium mt-4 mb-2">Base Tables</h3>
          <ul className="text-sm text-muted-foreground space-y-2 ml-5 list-disc">
            <li><code className="bg-muted px-1 py-0.5 rounded text-xs">fact_dz_device_link_latency</code> — Per-second latency and loss measurements from network probes</li>
          </ul>
        </section>

        {/* Footer */}
        <div className="text-center text-sm text-muted-foreground pt-4 border-t border-border">
          <Link to="/outages" className="hover:text-foreground">
            &larr; Back to Outages
          </Link>
        </div>
      </div>
    </div>
  )
}
