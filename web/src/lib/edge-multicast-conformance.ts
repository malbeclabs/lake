import type { EdgeMulticastConformance } from '@/lib/api'

// The rules panel and the control that opens it are gated on ONE predicate, deliberately shared.
//
// They drifted once — the chevron gated on the rules, the row on the conformance payload — and the
// gap is reachable: the window is 15 minutes while the page refetches every 30s, so a rule ages out
// of it with the panel open. The toggle reverted to a plain span and the row stayed, leaving an
// empty shaded band under the group with no control left to close it.
//
// It lives here rather than beside the components because a non-component export from a component
// module breaks Fast Refresh, and because a predicate two call sites must agree on is easier to
// keep honest with a test of its own.
export function hasConformanceRules(c?: EdgeMulticastConformance): boolean {
  return (c?.top_rules?.length ?? 0) > 0
}
