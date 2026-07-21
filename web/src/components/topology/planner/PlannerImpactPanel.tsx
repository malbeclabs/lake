import { type ReactNode, useId, useState } from 'react'
import { AlertTriangle, ChevronDown, Shield, ArrowRight, Zap, Loader2 } from 'lucide-react'
import type {
  PlanImpactReport,
  PartitionIssue,
  RedundancyChange,
  CapacityRisk,
  PlanOverlapWarning,
  DataIssue,
  ChangeRef,
  ImpactSeverity,
} from '@/lib/api'
import {
  groupLatencyDeltas,
  splitLatencyImprovements,
  sortRedundancy,
  sortRedundancyImprovements,
  sortCapacityRisks,
  severityRank,
  countBySeverity,
  formatDeltaMs,
  formatMs,
  type LatencyDeltaGroup,
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

/** Dot for an improvement row -- improvements are not risks, so they never
 *  use the risk severity color maps (SEVERITY_TEXT / SEVERITY_DOT). */
function GreenDot() {
  return <span className="inline-block w-2 h-2 rounded-full flex-shrink-0 bg-green-500" />
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

/** Compact "before X ms → after Y ms" line for one metro-pair's individual
 *  latency, always ms (never raw µs). Skipped for unreachable pairs -- "no
 *  path" already says everything an after-value can't. */
function BeforeAfter({ beforeUs, afterUs }: { beforeUs: number; afterUs: number }) {
  return (
    <span className="text-[10px] text-muted-foreground">
      before {formatMs(beforeUs)} → after {formatMs(afterUs)}
    </span>
  )
}

/** The "N metros" side of a collapsed latency-delta row. Hovering (or
 *  focusing, for keyboard users) reveals the individual member metros --
 *  each with its own before/after latency in ms -- so the summary row stays
 *  compact without hiding the detail entirely. */
function LatencyGroupMembers({ group }: { group: LatencyDeltaGroup }) {
  const [open, setOpen] = useState(false)
  return (
    <span
      className="relative inline-flex"
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
    >
      <span
        tabIndex={0}
        className="text-foreground underline decoration-dotted decoration-muted-foreground cursor-default"
        onFocus={() => setOpen(true)}
        onBlur={() => setOpen(false)}
      >
        {group.otherMetros.length} metros
      </span>
      {open && (
        <div
          data-testid="latency-group-members"
          className="absolute left-0 top-full mt-1 z-10 max-w-xs whitespace-normal rounded border border-[var(--border)] bg-[var(--popover)] px-2 py-1 text-popover-foreground shadow-lg space-y-0.5"
        >
          {group.members.map((m, i) => (
            <div key={`${m.metro_a}-${m.metro_z}-${i}`}>
              <span className="text-foreground">{group.otherMetros[i]}</span>
              {m.after_us >= 0 && (
                <span className="text-muted-foreground"> · </span>
              )}
              {m.after_us >= 0 && <BeforeAfter beforeUs={m.before_us} afterUs={m.after_us} />}
            </div>
          ))}
        </div>
      )}
    </span>
  )
}

/** One row of a grouped latency-delta list -- shared by the "Latency
 *  changes" risk section and the "Latency improvements" section. `dot`
 *  carries the severity dot for a risk row or a green dot for an improvement
 *  row, so the two sections never mix into the risk severity styling. */
function LatencyGroupRow({
  group,
  changeLabels,
  dot,
  testId,
}: {
  group: LatencyDeltaGroup
  changeLabels: Map<number, string>
  dot: ReactNode
  testId: string
}) {
  return (
    <div data-testid={testId} className="flex items-start gap-1.5">
      {dot}
      <div className="space-y-0.5">
        <div className="flex items-center gap-1 flex-wrap">
          {group.otherMetros.length > 1 ? (
            <LatencyGroupMembers group={group} />
          ) : (
            <span className="text-foreground">{group.otherMetros[0]}</span>
          )}
          <ArrowRight className="h-2.5 w-2.5 text-muted-foreground" />
          <span className="text-foreground">{group.commonMetro}</span>
          {group.unreachable ? (
            <span className="ml-1 text-red-500 font-medium">no path</span>
          ) : (
            <span className={`ml-1 ${(group.deltaUs ?? 0) > 0 ? 'text-amber-500' : 'text-green-500'}`}>
              {formatDeltaMs(group.deltaUs ?? 0)}
            </span>
          )}
        </div>
        {!group.unreachable && group.otherMetros.length === 1 && (
          <BeforeAfter beforeUs={group.members[0].before_us} afterUs={group.members[0].after_us} />
        )}
        <CausedBy causedBy={group.causedBy} changeLabels={changeLabels} />
      </div>
    </div>
  )
}

/** One row of a redundancy path-count list -- shared by the "Redundancy"
 *  risk section (amber when paths drop) and the "Added redundancy"
 *  improvement section (always green, since after_paths > before_paths). */
function RedundancyRow({
  item,
  changeLabels,
  dot,
  pathsClassName,
  testId,
}: {
  item: RedundancyChange
  changeLabels: Map<number, string>
  dot: ReactNode
  pathsClassName: string
  testId?: string
}) {
  return (
    <div data-testid={testId} className="flex items-start gap-1.5">
      {dot}
      <div className="space-y-0.5">
        <div>
          <span className="text-foreground">{item.metro_a}</span>
          <ArrowRight className="inline h-2.5 w-2.5 mx-0.5 text-muted-foreground" />
          <span className="text-foreground">{item.metro_z}</span>
          <span className="text-muted-foreground"> · paths </span>
          <span className={pathsClassName}>
            {item.before_paths} → {item.after_paths}
          </span>
        </div>
        <CausedBy causedBy={item.caused_by} changeLabels={changeLabels} />
      </div>
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

/** A foldable finding-category section: header shows the category name + a
 *  count of the underlying findings (before any display-only grouping, e.g.
 *  latency's "N metros" collapse), and toggles the section body. Keyboard
 *  accessible: a real <button> with aria-expanded, so it works with Enter/
 *  Space and is announced by screen readers. Default expanded. */
function CollapsibleSection({
  title,
  icon,
  count,
  children,
}: {
  title: string
  icon?: ReactNode
  count: number
  children: ReactNode
}) {
  const [expanded, setExpanded] = useState(true)
  const contentId = useId()
  return (
    <div className="space-y-1.5 pt-2 border-t border-[var(--border)]">
      <button
        type="button"
        aria-expanded={expanded}
        aria-controls={contentId}
        onClick={() => setExpanded((e) => !e)}
        className="w-full flex items-center justify-between gap-1.5 text-left group"
      >
        <span className="inline-flex items-center gap-1 font-medium text-muted-foreground uppercase tracking-wider text-[10px] group-hover:text-foreground">
          {icon}
          {title}
          <span className="normal-case tracking-normal">({count})</span>
        </span>
        <ChevronDown
          className={`h-3 w-3 flex-shrink-0 text-muted-foreground transition-transform ${expanded ? '' : '-rotate-90'}`}
        />
      </button>
      {expanded && (
        <div id={contentId} className="space-y-1.5">
          {children}
        </div>
      )}
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
  const latencyImprovements = report?.latency_improvements ?? []
  const redundancyImprovements = report?.redundancy_improvements ?? []

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

          {counts.total === 0 &&
            latencyImprovements.length === 0 &&
            redundancyImprovements.length === 0 && (
              <div className="text-green-500 flex items-center gap-1.5">
                <div className="w-2 h-2 rounded-full bg-green-500" />
                No impact detected - the draft keeps the network fully connected.
              </div>
            )}

          {/* 1. Connectivity / partitions */}
          {report.partition_issues.length > 0 && (
            <CollapsibleSection title="Connectivity" count={report.partition_issues.length}>
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
            </CollapsibleSection>
          )}

          {/* 2. Metro-pair latency, worst-first. Pairs sharing a common
              endpoint metro and the same added latency are collapsed into
              one "N metros +Xms to Y" row (members shown on hover) so a
              removed link doesn't dump dozens of near-identical rows. The
              header count is the raw finding count (pre-grouping). */}
          {report.latency_deltas.length > 0 && (
            <CollapsibleSection title="Latency changes" count={report.latency_deltas.length}>
              {groupLatencyDeltas(report.latency_deltas).map((g) => (
                <LatencyGroupRow
                  key={g.key}
                  group={g}
                  changeLabels={changeLabels}
                  dot={<SeverityDot severity={g.severity} />}
                  testId="impact-latency-row"
                />
              ))}
            </CollapsibleSection>
          )}

          {/* 3. Redundancy (path count before/after) */}
          {report.redundancy_changes.length > 0 && (
            <CollapsibleSection
              title="Redundancy"
              count={report.redundancy_changes.length}
              icon={<Shield className="h-3 w-3" />}
            >
              {sortRedundancy(report.redundancy_changes).map((r: RedundancyChange, i) => (
                <RedundancyRow
                  key={`${r.metro_a}-${r.metro_z}-${i}`}
                  item={r}
                  changeLabels={changeLabels}
                  dot={<SeverityDot severity={r.severity} />}
                  pathsClassName={r.after_paths < r.before_paths ? 'text-amber-500' : 'text-muted-foreground'}
                />
              ))}
            </CollapsibleSection>
          )}

          {/* 4. Capacity / bandwidth fallback risk (estimate) */}
          {report.capacity_risks.length > 0 && (
            <CollapsibleSection title="Capacity" count={report.capacity_risks.length}>
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
            </CollapsibleSection>
          )}

          {/* Cross-plan overlap warnings */}
          {report.overlap_warnings.length > 0 && (
            <CollapsibleSection title="Overlap warnings" count={report.overlap_warnings.length}>
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
            </CollapsibleSection>
          )}

          {/* Positive findings, always after every risk section: metro pairs
              that got faster or newly reachable, and pairs that gained a
              backup path. These are not risks, so they never feed into
              countBySeverity or the risk severity color maps -- each row
              uses a plain green dot instead of a SeverityDot. */}
          {latencyImprovements.length > 0 && (
            <CollapsibleSection
              title="Latency improvements"
              count={latencyImprovements.length}
              icon={<Zap className="h-3 w-3 text-green-500" />}
            >
              {(() => {
                const { reductions, newlyReachable } = splitLatencyImprovements(
                  latencyImprovements,
                )
                return (
                  <>
                    {groupLatencyDeltas(reductions).map((g) => (
                      <LatencyGroupRow
                        key={g.key}
                        group={g}
                        changeLabels={changeLabels}
                        dot={<GreenDot />}
                        testId="impact-latency-improvement-row"
                      />
                    ))}
                    {newlyReachable.map((d, i) => (
                      <div
                        key={`${d.metro_a}-${d.metro_z}-${i}`}
                        data-testid="impact-latency-reachable-row"
                        className="flex items-start gap-1.5"
                      >
                        <GreenDot />
                        <div className="space-y-0.5">
                          <div className="flex items-center gap-1 flex-wrap">
                            <span className="text-foreground">{d.metro_a}</span>
                            <ArrowRight className="h-2.5 w-2.5 text-muted-foreground" />
                            <span className="text-foreground">{d.metro_z}</span>
                            <span className="ml-1 px-1 py-0.5 rounded bg-green-500/15 text-green-500 text-[9px] uppercase tracking-wider">
                              now reachable
                            </span>
                          </div>
                          <CausedBy causedBy={d.caused_by} changeLabels={changeLabels} />
                        </div>
                      </div>
                    ))}
                  </>
                )
              })()}
            </CollapsibleSection>
          )}

          {redundancyImprovements.length > 0 && (
            <CollapsibleSection
              title="Added redundancy"
              count={redundancyImprovements.length}
              icon={<Shield className="h-3 w-3 text-green-500" />}
            >
              {sortRedundancyImprovements(redundancyImprovements).map((r, i) => (
                <RedundancyRow
                  key={`${r.metro_a}-${r.metro_z}-${i}`}
                  item={r}
                  changeLabels={changeLabels}
                  dot={<GreenDot />}
                  pathsClassName="text-green-500"
                  testId="impact-redundancy-improvement-row"
                />
              ))}
            </CollapsibleSection>
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
