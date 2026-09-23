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
        <ConformanceRulesRow conformance={c} columns={8} />
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
  function cell(c: EdgeMulticastConformance | undefined) {
    return render(
      <TooltipProvider>
        <ConformanceCell conformance={c} expanded={false} onToggle={() => {}} />
      </TooltipProvider>,
    )
  }

  // A chevron on this column is itself a finding; a clean group offers nothing to open.
  it('is a disclosure only when a rule fired', () => {
    cell(conformance({ top_rules: [{ rule_id: 'A.RULE', severity: 'must', count: 1 }] }))
    expect(screen.getByRole('button')).toBeInTheDocument()
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
