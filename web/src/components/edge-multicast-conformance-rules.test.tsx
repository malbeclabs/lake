import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import type { EdgeMulticastConformance } from '@/lib/api'

import { TooltipProvider } from '@/components/ui/tooltip'

import { ConformanceCell, ConformanceRulesRow } from './edge-multicast-page'

// The rules that fired were one line of a tooltip each. A rule id names a finding only to a
// reader who knows the catalog, so they are a panel now, with the catalog's own description.

function conformance(over: Partial<EdgeMulticastConformance>): EdgeMulticastConformance {
  return {
    verdict: 'violating',
    must: 33,
    should: 0,
    info: 0,
    passes: 0,
    graded: 33,
    na: 0,
    unverifiable: 0,
    exempted: 0,
    instances: 1,
    ...over,
  }
}

function rules(c: EdgeMulticastConformance) {
  return render(
    <table>
      <tbody>
        <ConformanceRulesRow conformance={c} columns={8} id="conformance-rules-test" />
      </tbody>
    </table>,
  )
}

describe('ConformanceRulesRow', () => {
  it('describes the rule rather than only naming it', () => {
    rules(
      conformance({
        top_rules: [
          {
            rule_id: 'MSG.WRONG_PORT_PLACEMENT',
            severity: 'must',
            count: 33,
            summary: 'Messages are published on the port their type belongs to',
            spec_url: 'https://example.invalid/spec#wrong-port',
            nodes: ['cmh1', 'was1'],
            validators: ['kalshi_perps_tob'],
          },
        ],
      }),
    )

    expect(screen.getByText('MSG.WRONG_PORT_PLACEMENT')).toBeInTheDocument()
    expect(
      screen.getByText('Messages are published on the port their type belongs to'),
    ).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'spec' })).toHaveAttribute(
      'href',
      'https://example.invalid/spec#wrong-port',
    )
    // The count is a detection count, so the line names the recorders behind it.
    expect(screen.getByText(/seen at cmh1, was1/)).toBeInTheDocument()
    // The instance name is the only thing that narrows a finding below the group.
    expect(screen.getByText(/kalshi_perps_tob/)).toBeInTheDocument()
  })

  // spec_url is a Prometheus label value, and an href is the one place where such a string
  // becomes executable.
  it('does not link a spec url that is not https', () => {
    rules(
      conformance({
        top_rules: [
          {
            rule_id: 'MSG.SEQ.GAP',
            severity: 'must',
            count: 1,
            spec_url: 'javascript:alert(1)',
          },
        ],
      }),
    )

    expect(screen.getByText('MSG.SEQ.GAP')).toBeInTheDocument()
    expect(screen.queryByRole('link')).toBeNull()
  })

  // A capped list that does not say what it left out reads as the whole finding.
  it('says how many rules it is not showing', () => {
    rules(
      conformance({
        rules_fired: 13,
        top_rules: [{ rule_id: 'A.RULE', severity: 'should', count: 2 }],
      }),
    )

    expect(screen.getByText(/\+12 more rules fired/)).toBeInTheDocument()
  })
})

describe('ConformanceCell', () => {
  function cell(c: EdgeMulticastConformance | undefined, expanded = false) {
    return render(
      <TooltipProvider>
        <ConformanceCell
          conformance={c}
          expanded={expanded}
          onToggle={() => {}}
          panelId="conformance-rules-test"
        />
      </TooltipProvider>,
    )
  }

  // A chevron on this column is itself a finding; a clean group offers nothing to open.
  it('is a disclosure only when a rule fired', () => {
    cell(conformance({ top_rules: [{ rule_id: 'A.RULE', severity: 'must', count: 1 }] }))
    expect(screen.getByRole('button')).toBeInTheDocument()
  })

  // The chevron says which way the panel is; a screen reader never sees it, so the state has to
  // be on the control, and the row it opens is a sibling rather than a child of the button.
  it('reports whether the panel it controls is open', () => {
    const c = conformance({ top_rules: [{ rule_id: 'A.RULE', severity: 'must', count: 1 }] })
    const { unmount } = cell(c)
    const closed = screen.getByRole('button')
    expect(closed).toHaveAttribute('aria-expanded', 'false')
    expect(closed).toHaveAttribute('aria-controls', 'conformance-rules-test')
    unmount()

    cell(c, true)
    expect(screen.getByRole('button')).toHaveAttribute('aria-expanded', 'true')
  })

  it('offers nothing to open on a group where nothing fired', () => {
    cell(conformance({ verdict: 'conforming', must: 0, passes: 33 }))
    expect(screen.queryByRole('button')).toBeNull()
    expect(screen.getByText('conforming')).toBeInTheDocument()
  })

  // Nobody checked is a different statement from a clean one.
  it('renders an em dash for a group no validator covers', () => {
    cell(undefined)
    expect(screen.getByText('—')).toBeInTheDocument()
  })
})

// --- The publisher grain -----------------------------------------------------
//
// The cell renders on a publisher line as well as a group row now, and the two say different
// things about an absence: a group row with no verdict means no validator covers the feed, a line
// with none means no finding named that path. Neither is a pass, and both render the same em dash,
// so the tests below pin what the cell does with a verdict rather than the dash's meaning.

function cell(c?: EdgeMulticastConformance) {
  return render(
    <TooltipProvider>
      <table>
        <tbody>
          <tr>
            <td>
              <ConformanceCell
                conformance={c}
                expanded={false}
                onToggle={() => {}}
                panelId="conformance-rules-line"
              />
            </td>
          </tr>
        </tbody>
      </table>
    </TooltipProvider>,
  )
}

describe('ConformanceCell at the publisher grain', () => {
  it('renders a publisher verdict, so one path can read differently from its peer', () => {
    cell(conformance({ verdict: 'violating', must: 4, passes: 900, graded: 904 }))
    expect(screen.getByText('violating')).toBeInTheDocument()
  })

  it('renders a clean verdict for a publisher whose peer is the one violating', () => {
    cell(conformance({ verdict: 'conforming', must: 0, passes: 900, graded: 900 }))
    expect(screen.getByText('conforming')).toBeInTheDocument()
    expect(screen.queryByText('violating')).not.toBeInTheDocument()
  })

  // A line with nothing to report is a dash, never a green badge. Until the validators carry the
  // source address every line is in this state, and rendering it as a pass would assert a clean
  // bill of health over a path nothing has graded.
  it('renders an em dash, not a pass, for a line no finding named', () => {
    cell(undefined)
    expect(screen.getByText('—')).toBeInTheDocument()
    expect(screen.queryByText('conforming')).not.toBeInTheDocument()
  })

  // The chevron is itself a finding: it appears only where a rule fired.
  it('offers the rule panel only when a rule fired', () => {
    const { unmount } = cell(conformance({ verdict: 'conforming', must: 0, passes: 10, graded: 10 }))
    expect(screen.queryByRole('button')).not.toBeInTheDocument()
    unmount()

    cell(
      conformance({
        top_rules: [
          {
            rule_id: 'FRAME.SEQ_RESET_GAP',
            severity: 'must',
            count: 4,
          },
        ],
        rules_fired: 1,
      }),
    )
    expect(screen.getByRole('button')).toBeInTheDocument()
  })
})

describe('ConformanceCell with no verdict of its own', () => {
  // The group row of a group whose every finding named a publisher. Its own counters are zero
  // because the split worked, not because nothing was graded — so it must say nothing rather than
  // render `ungraded`, whose tooltip claims nothing reached a verdict. That badge sat directly
  // above lines reading conforming over 900 passed checks.
  it('renders an absence, never a verdict, when the group has nothing to say at its grain', () => {
    cell(conformance({ verdict: '', must: 0, should: 0, info: 0, passes: 0, graded: 0 }))
    expect(screen.getByText('—')).toBeInTheDocument()
    expect(screen.queryByText('ungraded')).not.toBeInTheDocument()
    expect(screen.queryByText('conforming')).not.toBeInTheDocument()
  })

  // And the real ungraded state still renders, since that is the reading the verdict exists for.
  it('still renders ungraded where nothing graded the group at all', () => {
    cell(conformance({ verdict: 'ungraded', must: 0, passes: 0, graded: 1000 }))
    expect(screen.getByText('ungraded')).toBeInTheDocument()
  })

  // The suppressed verdict must not take the group's own counts down with it. `exempted` and
  // `unattributed` belong to the group and to no line, and this cell is the only place on the
  // page that reports either — so a bare dash here hides them completely.
  it('keeps the group-only counts reachable when the verdict is suppressed', () => {
    const { container } = cell(
      conformance({ verdict: '', must: 0, passes: 0, graded: 0, exempted: 7, unattributed: 2 }),
    )
    const dash = screen.getByText('—')
    expect(dash).toBeInTheDocument()
    // Radix marks a tooltip trigger with data-state; a bare dash carries none.
    expect(dash).toHaveAttribute('data-state')
    expect(container.querySelector('[data-state]')).not.toBeNull()
  })

  // A group no validator covers is a different absence and stays a plain dash: there is no entry
  // behind it and nothing to say about one.
  it('leaves a group with no validator as a bare dash', () => {
    cell(undefined)
    expect(screen.getByText('—')).not.toHaveAttribute('data-state')
  })
})
