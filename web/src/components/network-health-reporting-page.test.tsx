import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import type { UseQueryResult } from '@tanstack/react-query'
import {
  DeviceSlotsPanel,
  DrainTimingPanel,
  GroupBoundary,
  OutageSummaryStrip,
  RootCauseBreakdown,
  availabilityText,
  deriveAvailability,
  deviceSlotRow,
  hasKnownDenominator,
  panelDegraded,
  rootCauseLabel,
  sameNHWindow,
  splitDiaRows,
} from './network-health-reporting-page'
import { TooltipProvider } from '@/components/ui/tooltip'
import type {
  NHAvailabilityGroup,
  NHImpactful,
  NHOverview,
  NetworkHealthAvailability,
  NetworkHealthDeviceSlots,
  NetworkHealthDiaInterface,
  NetworkHealthDrainTiming,
  NetworkHealthOutageSummary,
  NetworkHealthRootCause,
  NetworkHealthWindow,
} from '@/lib/api'

function win(over: Partial<NetworkHealthWindow> = {}): NetworkHealthWindow {
  return { start: '2026-07-14', end: '2026-08-13', days: 30, label: 'last 30 days', ...over }
}

/** An overview payload carrying only the two fields deriveAvailability reads. */
function overview(activeLinks: number, w = win()): NHOverview {
  return { window: w, headline: { active_links: activeLinks } } as unknown as NHOverview
}

function impactful(hours: number, w = win(), over: Partial<NHImpactful> = {}): NHImpactful {
  return { window: w, impactful_downtime_hours: hours, impactful_downtime_delta: null, ...over }
}

describe('sameNHWindow', () => {
  it('matches on start and end', () => {
    expect(sameNHWindow(win(), win())).toBe(true)
    expect(sameNHWindow(win(), win({ end: '2026-08-12' }))).toBe(false)
    expect(sameNHWindow(win(), win({ start: '2026-07-15' }))).toBe(false)
  })

  // Two different custom ranges can hold the same day count, so `days` cannot
  // stand in for the window.
  it('separates two ranges of equal length', () => {
    expect(sameNHWindow(win({ start: '2026-01-01', end: '2026-01-31' }), win({ start: '2026-02-01', end: '2026-03-03' }))).toBe(false)
  })

  it('is false when either side is absent', () => {
    expect(sameNHWindow(undefined, win())).toBe(false)
    expect(sameNHWindow(win(), undefined)).toBe(false)
  })
})

describe('deriveAvailability', () => {
  it('divides failure hours by the same window they were measured over', () => {
    // 72 h against 100 links x 720 h.
    expect(deriveAvailability(overview(100), impactful(72))).toBe(99.9)
  })

  // The regression this guard exists for: keepPreviousData hands back the 30-day
  // impactful figure while overview already reports the 7-day window, which would
  // divide 30 days of failure hours by 168 hours.
  it('withholds the figure when the two payloads describe different windows', () => {
    const sevenDay = win({ start: '2026-08-06', end: '2026-08-13', days: 7 })
    expect(deriveAvailability(overview(100, sevenDay), impactful(72, win()))).toBeNull()
  })

  it('withholds an impactful payload that could not compute', () => {
    expect(deriveAvailability(overview(100), impactful(0, win(), { unavailable: true }))).toBeNull()
  })

  it('withholds when either group is missing, or there are no active links', () => {
    expect(deriveAvailability(undefined, impactful(72))).toBeNull()
    expect(deriveAvailability(overview(100), undefined)).toBeNull()
    expect(deriveAvailability(overview(0), impactful(72))).toBeNull()
  })

  it('clamps at zero rather than reporting negative availability', () => {
    expect(deriveAvailability(overview(1), impactful(10_000))).toBe(0)
  })
})

describe('availabilityText', () => {
  it('formats a derivable figure as a percent', () => {
    expect(availabilityText(overview(100), impactful(72), false)).toBe('99.9%')
  })

  // A window mismatch during a range change is transient, so it must not read as
  // a settled "no value".
  it('reads as loading while a group still holds the previous window', () => {
    const sevenDay = win({ start: '2026-08-06', end: '2026-08-13', days: 7 })
    expect(availabilityText(overview(100, sevenDay), impactful(72, win()), true)).toBe('loading...')
  })

  // A refetch in flight will replace the trailing payload, so the mismatch it
  // leaves behind is a wait rather than an absent measurement.
  it('reads as loading on a window mismatch while a fetch is in flight', () => {
    const yesterday = win({ start: '2026-07-13', end: '2026-08-12' })
    expect(availabilityText(overview(100), impactful(72, yesterday), false, true)).toBe('loading...')
  })

  // With both queries settled the mismatch does not resolve on its own: a
  // failing impactful refresh leaves the worker serving its last good blob,
  // whose window trails the overview's until the refresh recovers.
  it('keeps the dash on a window mismatch once both queries are settled', () => {
    const yesterday = win({ start: '2026-07-13', end: '2026-08-12' })
    expect(availabilityText(overview(100), impactful(72, yesterday), false, false)).toBe('—')
  })

  // The dash is reserved for a window with nothing to derive from.
  it('keeps the dash when the figure is genuinely underivable', () => {
    expect(availabilityText(overview(0), impactful(72), false)).toBe('—')
    expect(availabilityText(overview(100), impactful(0, win(), { unavailable: true }), false)).toBe('—')
  })
})

describe('rootCauseLabel', () => {
  // Every token the backend publishes (nhRootCauseTokens plus the "other"
  // catch-all it maps unrecognised upstream values to) needs a display label, or
  // the raw snake_case token renders on the public page.
  const published = [
    'self_resolved',
    'network_external',
    'fiber_cut',
    'configuration',
    'hardware',
    'carrier',
    'false_positive',
    'duplicate',
    'software',
    'dz_managed',
    'human_error',
    'other',
  ]

  it('labels every published token without leaking the raw token', () => {
    for (const token of published) {
      const label = rootCauseLabel(token)
      expect(label).not.toBe(token)
      expect(label[0]).toBe(label[0].toUpperCase())
    }
  })

  it('names the catch-all bucket', () => {
    expect(rootCauseLabel('other')).toBe('Other')
  })

  it('falls back to the raw value for a token it has never seen', () => {
    expect(rootCauseLabel('brand_new_cause')).toBe('brand_new_cause')
  })
})

function cause(c: string, count: number): NetworkHealthRootCause {
  return { cause: c, count, pct: null }
}

describe('RootCauseBreakdown', () => {
  const renderCauses = (causes: NetworkHealthRootCause[]) =>
    render(
      <TooltipProvider>
        <RootCauseBreakdown causes={causes} />
      </TooltipProvider>,
    )

  it('charts the catch-all bucket with a proper label', () => {
    renderCauses([cause('fiber_cut', 4), cause('other', 9)])
    expect(screen.getByText('Other')).toBeInTheDocument()
    expect(screen.queryByText('other')).not.toBeInTheDocument()
  })

  it('labels the tokens the ops API emits outside the original seven', () => {
    renderCauses([cause('duplicate', 25), cause('software', 3), cause('dz_managed', 1), cause('human_error', 1)])
    expect(screen.getByText('Duplicate')).toBeInTheDocument()
    expect(screen.getByText('Software')).toBeInTheDocument()
    expect(screen.getByText('DoubleZero managed')).toBeInTheDocument()
    expect(screen.getByText('Human error')).toBeInTheDocument()
  })
})

function drainTiming(over: Partial<NetworkHealthDrainTiming> = {}): NetworkHealthDrainTiming {
  return {
    outage_count: 190,
    events_with_drain: 40,
    drains: 60,
    undrains: 55,
    time_to_drain_p50_min: 12,
    time_to_drain_max_min: 90,
    time_drained_p50_min: 30,
    time_drained_max_min: 400,
    time_to_undrain_p50_min: 20,
    time_to_undrain_max_min: 200,
    drain_within_30m_pct: 55,
    matched_undrains: 50,
    ...over,
  }
}

describe('DrainTimingPanel', () => {
  const renderPanel = () =>
    render(
      <TooltipProvider>
        <DrainTimingPanel dt={drainTiming()} />
      </TooltipProvider>,
    )

  // outage_count comes from the shared sustained-failure definition (>= 10 min,
  // provisioning excluded, loss-only failures included), which the rest of the
  // page calls "link failures". The old "link-down events" wording named a
  // narrower, smaller figure.
  it('names the denominator the way the rest of the page names it', () => {
    renderPanel()
    expect(screen.getByText(/40 of 190 link failures/)).toBeInTheDocument()
    expect(screen.queryByText(/link-down events/)).not.toBeInTheDocument()
  })
})

describe('hasKnownDenominator', () => {
  it('accepts a real zero measured against a real denominator', () => {
    expect(hasKnownDenominator(0, 10)).toBe(true)
    expect(hasKnownDenominator(25, 20)).toBe(true)
  })

  it('rejects a row the server marked unknown, or one with no denominator', () => {
    expect(hasKnownDenominator(null, 10)).toBe(false)
    expect(hasKnownDenominator(null, 0)).toBe(false)
    expect(hasKnownDenominator(undefined, 5)).toBe(false)
    expect(hasKnownDenominator(0, 0)).toBe(false)
  })
})

function slots(over: Partial<NetworkHealthDeviceSlots> = {}): NetworkHealthDeviceSlots {
  return {
    pk: 'dev1',
    code: 'dz-ny5-sw01',
    unicast: 5,
    mcast_sub: 0,
    mcast_pub: 0,
    max_users: 20,
    used_pct: 25,
    ...over,
  }
}

describe('deviceSlotRow', () => {
  it('sizes each segment against the cap', () => {
    const r = deviceSlotRow(slots({ unicast: 5, mcast_sub: 2, mcast_pub: 1, max_users: 20, used_pct: 40 }))
    expect(r.known).toBe(true)
    expect(r.used).toBe(8)
    expect(r.widths).toEqual({ unicast: '25.0%', sub: '10.0%', pub: '5.0%' })
  })

  // max_users 0 is how a device is stopped from taking NEW users while the ones
  // it has stay connected, so this row is reachable with seats in use. The old
  // `max_users || 1` fallback drew it as a 500%-wide bar.
  it('reports a device with no cap as unknown rather than sizing against 1', () => {
    const r = deviceSlotRow(slots({ unicast: 5, max_users: 0, used_pct: null }))
    expect(r.known).toBe(false)
    expect(r.used).toBe(5)
    expect(r.widths).toBeNull()
  })

  it('reports an empty device with no cap as unknown, not as 0%', () => {
    const r = deviceSlotRow(slots({ unicast: 0, mcast_sub: 0, mcast_pub: 0, max_users: 0, used_pct: null }))
    expect(r.known).toBe(false)
    expect(r.widths).toBeNull()
  })
})

describe('DeviceSlotsPanel', () => {
  const renderPanel = (rows: NetworkHealthDeviceSlots[]) =>
    render(
      <MemoryRouter>
        <TooltipProvider>
          <DeviceSlotsPanel rows={rows} />
        </TooltipProvider>
      </MemoryRouter>,
    )

  it('labels an uncapped device "no cap" instead of "of 0"', () => {
    renderPanel([slots({ unicast: 5, max_users: 0, used_pct: null })])
    expect(screen.getByText('no cap')).toBeInTheDocument()
    expect(screen.getByTitle(/No max_users cap set\./)).toBeInTheDocument()
  })

  it('keeps a capped device on its numbers', () => {
    renderPanel([slots()])
    expect(screen.queryByText('no cap')).not.toBeInTheDocument()
    expect(screen.getByTitle(/5 of 20 max\./)).toBeInTheDocument()
  })
})

function dia(over: Partial<NetworkHealthDiaInterface> = {}): NetworkHealthDiaInterface {
  return {
    device_pk: 'dev1',
    device: 'dz-ny5-sw01',
    intf: 'Ethernet1',
    port_gbps: 10,
    cir_gbps: 1,
    p50_gbps: 1,
    p99_gbps: 2,
    util_pct: 20,
    denom: 'port',
    ...over,
  }
}

describe('splitDiaRows', () => {
  // A busy interface whose port speed has not landed yet would chart at 0% and
  // sort below genuinely idle uplinks.
  it('keeps an interface with no port speed out of the chart', () => {
    const busy = dia({ intf: 'Ethernet9', port_gbps: 0, util_pct: null, p99_gbps: 8.2 })
    const { measured, unknown } = splitDiaRows([dia(), busy])
    expect(measured.map((d) => d.intf)).toEqual(['Ethernet1'])
    expect(unknown.map((d) => d.intf)).toEqual(['Ethernet9'])
  })

  it('charts a genuinely idle interface, which has a real denominator', () => {
    const { measured, unknown } = splitDiaRows([dia({ util_pct: 0, p50_gbps: 0, p99_gbps: 0 })])
    expect(measured).toHaveLength(1)
    expect(unknown).toHaveLength(0)
  })

  it('loses no interface and keeps the order the server sent', () => {
    const rows = [dia({ intf: 'a' }), dia({ intf: 'b', port_gbps: 0, util_pct: null }), dia({ intf: 'c' })]
    const { measured, unknown } = splitDiaRows(rows)
    expect(measured.length + unknown.length).toBe(rows.length)
    expect(measured.map((d) => d.intf)).toEqual(['a', 'c'])
  })
})

function summary(over: Partial<NetworkHealthOutageSummary> = {}): NetworkHealthOutageSummary {
  return { link_outages: 0, outage_hours: 0, links_affected: 0, device_outages: 0, devices_affected: 0, ...over }
}

describe('OutageSummaryStrip', () => {
  const renderStrip = (s: NetworkHealthOutageSummary | null) =>
    render(
      <TooltipProvider>
        <OutageSummaryStrip summary={s} />
      </TooltipProvider>,
    )

  // A null summary only happens when the query failed, so the panel must say so.
  // Returning null instead dropped the frame too, because this strip is a
  // GroupBoundary child and GroupBoundary draws no frame on its data branch.
  it('renders the framed unavailable state instead of vanishing', () => {
    renderStrip(null)
    expect(screen.getByText('Failure summary')).toBeInTheDocument()
    expect(screen.getByText(/Couldn't load this section\./)).toBeInTheDocument()
  })

  // The other half: a quiet window is a successful zeroed payload and must still
  // show its tiles.
  it('renders the tiles for a window with no failures', () => {
    renderStrip(summary())
    expect(screen.getByText('Failure summary')).toBeInTheDocument()
    expect(screen.queryByText(/Couldn't load this section\./)).not.toBeInTheDocument()
    expect(screen.getByText('Link failures')).toBeInTheDocument()
    expect(screen.getByText('Devices affected')).toBeInTheDocument()
  })
})

describe('panelDegraded', () => {
  it('matches when any of the panel sources failed', () => {
    expect(panelDegraded(['sla'], ['latency_links', 'sla'])).toBe(true)
    expect(panelDegraded(['sla', 'latency_links'], ['sla'])).toBe(true)
  })

  it('ignores a failure in a panel this one does not read', () => {
    expect(panelDegraded(['link_availability'], ['device_availability'])).toBe(false)
  })

  it('is false when either side is empty or absent', () => {
    expect(panelDegraded(undefined, ['sla'])).toBe(false)
    expect(panelDegraded([], ['sla'])).toBe(false)
    expect(panelDegraded(['sla'], undefined)).toBe(false)
    expect(panelDegraded(['sla'], [])).toBe(false)
  })
})

function availRow(code: string): NetworkHealthAvailability {
  return {
    pk: `${code}-pk`,
    code,
    avail_pct: 99.9,
    drained_pct: 0,
    outage_pct: 0.1,
    avail_hours: 719,
    outage_hours: 1,
    drained_hours: 0,
  }
}

/** A resolved group query; GroupBoundary reads only these fields. */
function resolved<T>(data: T): UseQueryResult<T> {
  return { data, isPending: false, isPlaceholderData: false, error: null } as unknown as UseQueryResult<T>
}

function availGroup(over: Partial<NHAvailabilityGroup> = {}): NHAvailabilityGroup {
  return {
    link_availability: [availRow('lnk-a')],
    device_availability: [availRow('dev-a')],
    ...over,
  }
}

describe('GroupBoundary', () => {
  // The two availability panels share one group query, so the degraded check has
  // to be per panel: a failed link scan must not blank the device table.
  const renderPair = (g: NHAvailabilityGroup) => {
    const q = resolved(g)
    return render(
      <TooltipProvider>
        <GroupBoundary query={q} title="Least available links" sources={['link_availability']}>
          {(d) => <div>{`links: ${d.link_availability.map((r) => r.code).join(',')}`}</div>}
        </GroupBoundary>
        <GroupBoundary query={q} title="Least available devices" sources={['device_availability']}>
          {(d) => <div>{`devices: ${d.device_availability.map((r) => r.code).join(',')}`}</div>}
        </GroupBoundary>
      </TooltipProvider>,
    )
  }

  // The Availability group has no critical panel, so a failed scan sets no group
  // error: without the degraded check the panel drew the query's empty list as a
  // real "no data" result.
  it('degrades only the panel whose own source failed', () => {
    renderPair(availGroup({ degraded: ['link_availability'] }))
    expect(screen.getAllByText(/Couldn't load this section\./)).toHaveLength(1)
    expect(screen.getByText('Least available links')).toBeInTheDocument()
    expect(screen.queryByText('links: lnk-a')).not.toBeInTheDocument()
    expect(screen.getByText('devices: dev-a')).toBeInTheDocument()
  })

  it('renders both panels when degraded is empty or absent', () => {
    renderPair(availGroup({ degraded: [] }))
    expect(screen.getByText('links: lnk-a')).toBeInTheDocument()
    expect(screen.getByText('devices: dev-a')).toBeInTheDocument()
    expect(screen.queryByText(/Couldn't load this section\./)).not.toBeInTheDocument()

    renderPair(availGroup())
    expect(screen.getAllByText('links: lnk-a')).toHaveLength(2)
    expect(screen.queryByText(/Couldn't load this section\./)).not.toBeInTheDocument()
  })

  it('still renders the whole group unavailable on a group error', () => {
    renderPair(availGroup({ error: 'availability data temporarily unavailable' }))
    expect(screen.getAllByText(/Couldn't load this section\./)).toHaveLength(2)
    expect(screen.queryByText('links: lnk-a')).not.toBeInTheDocument()
    expect(screen.queryByText('devices: dev-a')).not.toBeInTheDocument()
  })
})
