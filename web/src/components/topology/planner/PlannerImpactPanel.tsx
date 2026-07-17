import { type ReactNode } from 'react'
import { AlertTriangle, Shield, ArrowRight, Zap, Loader2 } from 'lucide-react'
import type {
  PlanImpactReport,
  PartitionIssue,
  MetroLatencyDelta,
  RedundancyChange,
  CapacityRisk,
  PlanOverlapWarning,
  DataIssue,
  ChangeRef,
  ImpactSeverity,
} from '@/lib/api'
import {
  sortLatencyDeltas,
  sortRedundancy,
  sortCapacityRisks,
  severityRank,
  countBySeverity,
  formatDeltaMs,
} from './impact-format'

interface PlannerImpactPanelProps {
  report: PlanImpactReport | null
  isLoading: boolean
  error: string | null
  changeLabels: Map<number, string>
}

const SEVERITY_TEXT: Record<ImpactSeverity, string> = {
  high: 'text-red-500',
  medium: 'text-amber-500',
  low: 'text-blue-500',
}

const SEVERITY_DOT: Record<ImpactSeverity, string> = {
  high: 'bg-red-500',
  medium: 'bg-amber-500',
  low: 'bg-blue-500',
}

function SeverityDot({ severity }: { severity: ImpactSeverity }) {
  return <span className={`inline-block w-2 h-2 rounded-full flex-shrink-0 ${SEVERITY_DOT[severity]}`} />
}

/** Render each causing change as a read-only chip. A client-supplied label
 *  (keyed by change seq) overrides the finding's own ChangeRef.label. The Go
 *  side can leave this slice nil (no footprint match) -> JSON null, so guard
 *  before mapping. */
function CausedBy({
  causedBy,
  changeLabels,
}: {
  causedBy: ChangeRef[] | null
  changeLabels: Map<number, string>
}) {
  if (!causedBy || causedBy.length === 0) return null
  return (
    <div className="flex flex-wrap gap-1">
      {causedBy.map((cr, i) => (
        <span
          key={`${cr.seq}-${i}`}
          className="inline-flex items-center px-1.5 py-0.5 rounded bg-[var(--muted)] text-muted-foreground text-[10px]"
        >
          {changeLabels.get(cr.seq) ?? cr.label}
        </span>
      ))}
    </div>
  )
}

function SectionTitle({ children }: { children: ReactNode }) {
  return (
    <div className="font-medium text-muted-foreground uppercase tracking-wider text-[10px]">
      {children}
    </div>
  )
}

export function PlannerImpactPanel({
  report,
  isLoading,
  error,
  changeLabels,
}: PlannerImpactPanelProps) {
  const counts = report ? countBySeverity(report) : null

  return (
    <div className="p-3 text-xs space-y-3">
      <div className="flex items-center justify-between">
        <span className="font-medium flex items-center gap-1.5">
          <Zap className="h-3.5 w-3.5 text-purple-500" />
          Impact
        </span>
        {isLoading && report && (
          <span className="flex items-center gap-1 text-muted-foreground text-[10px]">
            <Loader2 className="h-3 w-3 animate-spin" />
            Updating
          </span>
        )}
      </div>

      {error && (
        <div className="p-2 bg-red-500/10 border border-red-500/30 rounded text-red-500 flex items-center gap-1.5">
          <AlertTriangle className="h-3.5 w-3.5 flex-shrink-0" />
          <span>{error}</span>
        </div>
      )}

      {!report && !isLoading && !error && (
        <div className="text-muted-foreground">Add changes to see impact on the draft network.</div>
      )}

      {!report && isLoading && (
        <div className="text-muted-foreground flex items-center gap-1.5">
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
          Computing impact...
        </div>
      )}

      {report && counts && (
        <>
          {/* Severity summary */}
          <div className="flex flex-wrap gap-1.5">
            <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-red-500/15 text-red-500">
              {counts.high} high
            </span>
            <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-amber-500/15 text-amber-500">
              {counts.medium} medium
            </span>
            <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-blue-500/15 text-blue-500">
              {counts.low} low
            </span>
          </div>

          {counts.total === 0 && (
            <div className="text-green-500 flex items-center gap-1.5">
              <div className="w-2 h-2 rounded-full bg-green-500" />
              No impact detected - the draft keeps the network fully connected.
            </div>
          )}

          {/* 1. Connectivity / partitions */}
          {report.partition_issues.length > 0 && (
            <div className="space-y-1.5 pt-2 border-t border-[var(--border)]">
              <SectionTitle>Connectivity</SectionTitle>
              {[...report.partition_issues]
                .sort((a, b) => severityRank(a.severity) - severityRank(b.severity))
                .map((p: PartitionIssue, i) => (
                  <div key={`${p.entity_pk}-${i}`} className="flex items-start gap-1.5">
                    <SeverityDot severity={p.severity} />
                    <div className="space-y-0.5">
                      <div>
                        <span className={`font-medium ${SEVERITY_TEXT[p.severity]}`}>{p.entity_code}</span>
                        <span className="text-muted-foreground"> · {p.description}</span>
                      </div>
                      <CausedBy causedBy={p.caused_by} changeLabels={changeLabels} />
                    </div>
                  </div>
                ))}
            </div>
          )}

          {/* 2. Metro-pair latency, worst-first */}
          {report.latency_deltas.length > 0 && (
            <div className="space-y-1.5 pt-2 border-t border-[var(--border)]">
              <SectionTitle>Metro-pair latency</SectionTitle>
              {sortLatencyDeltas(report.latency_deltas).map((d: MetroLatencyDelta, i) => (
                <div key={`${d.metro_a}-${d.metro_z}-${i}`} data-testid="impact-latency-row" className="flex items-start gap-1.5">
                  <SeverityDot severity={d.severity} />
                  <div className="space-y-0.5">
                    <div className="flex items-center gap-1">
                      <span className="text-foreground">{d.metro_a}</span>
                      <ArrowRight className="h-2.5 w-2.5 text-muted-foreground" />
                      <span className="text-foreground">{d.metro_z}</span>
                      {d.after_us < 0 ? (
                        <span className="ml-1 text-red-500 font-medium">no path</span>
                      ) : (
                        <span className={`ml-1 ${d.delta_us > 0 ? 'text-amber-500' : 'text-green-500'}`}>
                          {formatDeltaMs(d.delta_us)}
                        </span>
                      )}
                    </div>
                    <CausedBy causedBy={d.caused_by} changeLabels={changeLabels} />
                  </div>
                </div>
              ))}
            </div>
          )}

          {/* 3. Redundancy (path count before/after) */}
          {report.redundancy_changes.length > 0 && (
            <div className="space-y-1.5 pt-2 border-t border-[var(--border)]">
              <SectionTitle>
                <span className="inline-flex items-center gap-1">
                  <Shield className="h-3 w-3" />
                  Redundancy
                </span>
              </SectionTitle>
              {sortRedundancy(report.redundancy_changes).map((r: RedundancyChange, i) => (
                <div key={`${r.metro_a}-${r.metro_z}-${i}`} className="flex items-start gap-1.5">
                  <SeverityDot severity={r.severity} />
                  <div className="space-y-0.5">
                    <div>
                      <span className="text-foreground">{r.metro_a}</span>
                      <ArrowRight className="inline h-2.5 w-2.5 mx-0.5 text-muted-foreground" />
                      <span className="text-foreground">{r.metro_z}</span>
                      <span className="text-muted-foreground"> · paths </span>
                      <span className={r.after_paths < r.before_paths ? 'text-amber-500' : 'text-muted-foreground'}>
                        {r.before_paths} → {r.after_paths}
                      </span>
                    </div>
                    <CausedBy causedBy={r.caused_by} changeLabels={changeLabels} />
                  </div>
                </div>
              ))}
            </div>
          )}

          {/* 4. Capacity / bandwidth fallback risk (estimate) */}
          {report.capacity_risks.length > 0 && (
            <div className="space-y-1.5 pt-2 border-t border-[var(--border)]">
              <SectionTitle>Capacity fallback risk</SectionTitle>
              {sortCapacityRisks(report.capacity_risks).map((c: CapacityRisk, i) => (
                <div key={`${c.link_pk}-${i}`} className="flex items-start gap-1.5">
                  <SeverityDot severity={c.severity} />
                  <div className="space-y-0.5">
                    <div className="flex items-center gap-1 flex-wrap">
                      <span className="text-foreground">{c.description}</span>
                      {c.estimated && (
                        <span className="px-1 py-0.5 rounded bg-[var(--muted)] text-muted-foreground text-[9px] uppercase tracking-wider">
                          estimate
                        </span>
                      )}
                    </div>
                    {c.note && <div className="text-[10px] text-muted-foreground">{c.note}</div>}
                    <CausedBy causedBy={c.caused_by} changeLabels={changeLabels} />
                  </div>
                </div>
              ))}
            </div>
          )}

          {/* Cross-plan overlap warnings */}
          {report.overlap_warnings.length > 0 && (
            <div className="space-y-1.5 pt-2 border-t border-[var(--border)]">
              <SectionTitle>Overlapping plans</SectionTitle>
              {[...report.overlap_warnings]
                .sort((a, b) => severityRank(a.severity) - severityRank(b.severity))
                .map((o: PlanOverlapWarning, i) => (
                  <div key={`${o.other_plan_id}-${o.entity_pk}-${i}`} className="flex items-start gap-1.5">
                    <SeverityDot severity={o.severity} />
                    <div className="space-y-0.5">
                      <div className="text-muted-foreground">
                        <span className={`font-medium ${SEVERITY_TEXT[o.severity]}`}>{o.other_plan_name}</span>{' '}
                        ({o.other_plan_status}) also touches {o.entity_type}{' '}
                        <span className="text-foreground">{o.entity_code}</span>
                      </div>
                      {o.description && (
                        <div className="text-[10px] text-muted-foreground">{o.description}</div>
                      )}
                    </div>
                  </div>
                ))}
            </div>
          )}

          {/* Data issues that limited the analysis */}
          {report.data_issues.length > 0 && (
            <div className="space-y-1 pt-2 border-t border-[var(--border)]">
              <SectionTitle>Data notes</SectionTitle>
              {report.data_issues.map((d: DataIssue, i) => (
                <div key={i} className="text-muted-foreground flex items-start gap-1.5">
                  <AlertTriangle className="h-3 w-3 flex-shrink-0 mt-0.5 text-amber-500" />
                  <span>{d.message}</span>
                </div>
              ))}
            </div>
          )}

          {report.estimated && (
            <div className="text-[10px] text-muted-foreground italic">
              Some results include estimates and may be approximate.
            </div>
          )}
        </>
      )}
    </div>
  )
}
