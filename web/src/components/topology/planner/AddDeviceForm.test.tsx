import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { AddDeviceForm } from './AddDeviceForm'
import { PlannerMap } from './PlannerMap'
import { buildDraft, type DraftTopology } from './draft'
import type {
  PlanChange,
  TopologyDevice,
  TopologyLink,
  TopologyMetro,
  TopologyResponse,
} from '@/lib/api'

const METROS: TopologyMetro[] = [
  { pk: 'M1', code: 'nyc', name: 'New York', latitude: 40, longitude: -74 },
  { pk: 'M2', code: 'lon', name: 'London', latitude: 51, longitude: 0 },
  { pk: 'M3', code: 'par', name: 'Paris', latitude: 48, longitude: 2 },
]

describe('AddDeviceForm', () => {
  it('defaults to the given metro and the switch device type', () => {
    render(
      <AddDeviceForm
        metros={METROS}
        defaultMetroPk="M2"
        onSubmit={vi.fn()}
        onCancel={vi.fn()}
      />
    )
    expect(screen.getByLabelText(/Metro/)).toHaveValue('M2')
    expect(screen.getByLabelText(/Type/)).toHaveValue('switch')
  })

  it('requires contributor, metro and code before submitting', () => {
    const onSubmit = vi.fn()
    render(
      <AddDeviceForm
        metros={METROS}
        defaultMetroPk=""
        onSubmit={onSubmit}
        onCancel={vi.fn()}
      />
    )
    fireEvent.click(screen.getByText('Add device'))
    expect(onSubmit).not.toHaveBeenCalled()
    expect(
      screen.getByText('Metro, contributor and code are required.')
    ).toBeInTheDocument()
  })

  it('trims contributor pk and code, and reports the chosen metro and type', () => {
    const onSubmit = vi.fn()
    render(
      <AddDeviceForm
        metros={METROS}
        defaultMetroPk="M1"
        onSubmit={onSubmit}
        onCancel={vi.fn()}
      />
    )
    fireEvent.change(screen.getByLabelText(/Contributor pk/), {
      target: { value: '  contrib1  ' },
    })
    fireEvent.change(screen.getByLabelText(/Code/), { target: { value: '  nyc-x9  ' } })
    fireEvent.change(screen.getByLabelText(/Metro/), { target: { value: 'M3' } })
    fireEvent.change(screen.getByLabelText(/Type/), { target: { value: 'router' } })
    fireEvent.click(screen.getByText('Add device'))
    expect(onSubmit).toHaveBeenCalledWith({
      contributorPk: 'contrib1',
      metroPk: 'M3',
      code: 'nyc-x9',
      deviceType: 'router',
    })
  })

  it('calls onCancel when Cancel is clicked', () => {
    const onCancel = vi.fn()
    render(
      <AddDeviceForm
        metros={METROS}
        defaultMetroPk="M1"
        onSubmit={vi.fn()}
        onCancel={onCancel}
      />
    )
    fireEvent.click(screen.getByText('Cancel'))
    expect(onCancel).toHaveBeenCalledTimes(1)
  })
})

// --- PlannerMap add-device / remove-device tools ----------------------------

interface MockPlanner {
  draft: DraftTopology | null
  baseline: TopologyResponse | null
  tool: string
  setTool: (t: string) => void
  selectedLinkKey: string | null
  selectLink: (k: string | null) => void
  addChange: (input: unknown) => void
}

// Shared, hoisted refs so the mocked modules below can read values the tests set.
const { plannerRef, mapClickTargetRef } = vi.hoisted(() => ({
  plannerRef: { current: null as unknown as MockPlanner },
  // Where a simulated map click "lands" (lng/lat). Tests point this near a metro.
  mapClickTargetRef: { current: { lng: 0, lat: 0 } },
}))

vi.mock('@/hooks/use-theme', () => ({
  useTheme: () => ({ resolvedTheme: 'light', theme: 'light', setTheme: () => {} }),
}))

vi.mock('./PlannerContext', () => ({
  usePlanner: () => plannerRef.current,
}))

// Same light MapLibre stand-ins used by AddLinkForm.test.tsx / MoveLinkEndForm.test.tsx,
// plus a dedicated "map-surface" sibling (not an ancestor of children) that fires the
// MapGL onClick prop. Keeping it a sibling -- rather than wrapping children in the
// same clickable element -- means a click on a device Marker never bubbles into the
// map's own onClick, matching how react-map-gl/maplibre keeps marker DOM nodes
// separate from the map canvas's click handling.
vi.mock('react-map-gl/maplibre', () => ({
  __esModule: true,
  default: ({
    children,
    onClick,
  }: {
    children?: React.ReactNode
    onClick?: (e: { lngLat: { lng: number; lat: number }; features?: unknown[] }) => void
  }) => (
    <div>
      <div
        data-testid="map-surface"
        onClick={() => onClick?.({ lngLat: mapClickTargetRef.current, features: [] })}
      />
      {children}
    </div>
  ),
  Source: ({ children }: { children?: React.ReactNode }) => <div>{children}</div>,
  Layer: () => null,
  Marker: ({
    children,
    draggable,
  }: {
    children?: React.ReactNode
    draggable?: boolean
  }) => (draggable ? <button type="button">{children}</button> : <div>{children}</div>),
}))

function makeDevice(pk: string, code: string, metroPk: string): TopologyDevice {
  return {
    pk, code, status: 'active', device_type: 'switch', metro_pk: metroPk,
    contributor_pk: 'c1', contributor_code: 'co',
    user_count: 0, unicast_users_count: 0, multicast_subscribers_count: 0,
    multicast_publishers_count: 0, max_unicast_users: 0, max_multicast_subscribers: 0,
    max_multicast_publishers: 0, validator_count: 0, stake_sol: 0, stake_share: 0,
    interfaces: [],
  }
}

function makeTopoLink(pk: string, code: string, aPk: string, zPk: string): TopologyLink {
  return {
    pk, code, status: 'active', link_type: 'WAN', bandwidth_bps: 10_000_000_000,
    side_a_pk: aPk, side_a_code: '', side_a_iface_name: 'eth0', side_a_ip: '',
    side_z_pk: zPk, side_z_code: '', side_z_iface_name: 'eth1', side_z_ip: '',
    contributor_pk: '', contributor_code: '',
    side_a_contributor_pk: '', side_a_contributor_code: '',
    side_z_contributor_pk: '', side_z_contributor_code: '',
    latency_us: 5_000, jitter_us: 0, latency_a_to_z_us: 0, jitter_a_to_z_us: 0,
    latency_z_to_a_us: 0, jitter_z_to_a_us: 0, loss_percent: 0, sample_count: 0,
    in_bps: 0, out_bps: 0, committed_rtt_ns: 5_000_000, isis_delay_override_ns: 0,
  }
}

// dA-dB-dC chain: L1 joins dA-dB, L2 joins dB-dC, so dB has two attached links.
function makeBaseline(): TopologyResponse {
  return {
    metros: METROS,
    devices: [
      makeDevice('dA', 'nyc-a', 'M1'),
      makeDevice('dB', 'lon-b', 'M2'),
      makeDevice('dC', 'par-c', 'M3'),
    ],
    links: [makeTopoLink('L1', 'l1-link', 'dA', 'dB'), makeTopoLink('L2', 'l2-link', 'dB', 'dC')],
    validators: [],
  }
}

function makePlanner(
  tool: string,
  setTool: (t: string) => void,
  addChange: (i: unknown) => void,
  changes: PlanChange[] = []
): MockPlanner {
  const baseline = makeBaseline()
  return {
    draft: buildDraft(baseline, changes),
    baseline,
    tool,
    setTool,
    selectedLinkKey: null,
    selectLink: () => {},
    addChange,
  }
}

describe('PlannerMap add-device tool', () => {
  it('drops a device at the click point and preselects the nearest metro', () => {
    const addChange = vi.fn()
    const setTool = vi.fn()
    plannerRef.current = makePlanner('add-device', setTool, addChange)
    mapClickTargetRef.current = { lng: -74, lat: 40 } // exactly M1 (nyc)
    render(<PlannerMap />)

    fireEvent.click(screen.getByTestId('map-surface'))

    expect(screen.getByText('New device')).toBeInTheDocument()
    expect(screen.getByLabelText(/Metro/)).toHaveValue('M1')
  })

  it('stages an add_device with a local_ref, a resolved metro code, and returns to select', () => {
    const addChange = vi.fn()
    const setTool = vi.fn()
    plannerRef.current = makePlanner('add-device', setTool, addChange)
    mapClickTargetRef.current = { lng: -74, lat: 40 } // nearest metro M1 (nyc)
    render(<PlannerMap />)

    fireEvent.click(screen.getByTestId('map-surface'))
    fireEvent.change(screen.getByLabelText(/Contributor pk/), {
      target: { value: 'contrib1' },
    })
    fireEvent.change(screen.getByLabelText(/Code/), { target: { value: 'nyc-x9' } })
    fireEvent.click(screen.getByText('Add device'))

    expect(addChange).toHaveBeenCalledTimes(1)
    expect(addChange).toHaveBeenCalledWith(
      expect.objectContaining({
        op_type: 'add_device',
        local_ref: expect.stringMatching(/^tmp_dev_/),
        payload: expect.objectContaining({
          contributor_pk: 'contrib1',
          metro_pk: 'M1',
          code: 'nyc-x9',
          device_type: 'switch',
        }),
        // ref_snapshot must carry the human-readable metro CODE ("nyc"), not the pk
        // ("M1") -- same invariant buildRemoveDeviceSnapshot enforces for remove_device,
        // since ref_snapshot exists to stay readable after the pk is gone.
        ref_snapshot: expect.objectContaining({ device_code: 'nyc-x9', metro_code: 'nyc' }),
      })
    )
    expect(setTool).toHaveBeenCalledWith('select')
    // The form closes once the change is staged.
    expect(screen.queryByText('New device')).not.toBeInTheDocument()
  })

  it('cancelling the form clears the pending placement', () => {
    const addChange = vi.fn()
    const setTool = vi.fn()
    plannerRef.current = makePlanner('add-device', setTool, addChange)
    render(<PlannerMap />)

    fireEvent.click(screen.getByTestId('map-surface'))
    expect(screen.getByText('New device')).toBeInTheDocument()

    fireEvent.click(screen.getByText('Cancel'))
    expect(screen.queryByText('New device')).not.toBeInTheDocument()
    expect(addChange).not.toHaveBeenCalled()
  })

  // CRITICAL (stale-state-across-tool-change, same family as the add-link source
  // reset): a pending device placement picked in one add-device session must never
  // survive a switch away from the tool and back. If it did, re-entering the tool
  // would silently reopen a form still pinned to the abandoned drop point.
  it('resets a stale pending placement when the tool changes away and back', () => {
    const addChange = vi.fn()
    const setTool = vi.fn()
    plannerRef.current = makePlanner('add-device', setTool, addChange)
    const { rerender } = render(<PlannerMap />)

    fireEvent.click(screen.getByTestId('map-surface'))
    expect(screen.getByText('New device')).toBeInTheDocument()

    // Abandon the flow by switching tools (e.g. via the toolbar) without submitting.
    plannerRef.current = makePlanner('select', setTool, addChange)
    rerender(<PlannerMap />)
    plannerRef.current = makePlanner('add-device', setTool, addChange)
    rerender(<PlannerMap />)

    // No stale form should reappear just from re-entering the tool.
    expect(screen.queryByText('New device')).not.toBeInTheDocument()

    // A fresh click still works and opens a brand new (empty) form.
    fireEvent.click(screen.getByTestId('map-surface'))
    expect(screen.getByText('New device')).toBeInTheDocument()
    expect(addChange).not.toHaveBeenCalled()
  })
})

// Drain the microtask queue so an awaited chain of addChange calls settles before
// assertions. A few ticks cover the loop's resumptions.
async function flushMicrotasks() {
  for (let i = 0; i < 8; i++) await Promise.resolve()
}

describe('PlannerMap remove-device helper', () => {
  it('stages a remove_link per attached link, then the remove_device, when the device has links', async () => {
    const addChange = vi.fn().mockResolvedValue(undefined)
    const setTool = vi.fn()
    plannerRef.current = makePlanner('remove-device', setTool, addChange)
    render(<PlannerMap />)

    fireEvent.click(screen.getByTitle('lon-b')) // dB: attached to both L1 and L2
    await flushMicrotasks()

    expect(addChange).toHaveBeenCalledTimes(3)
    expect(addChange).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({ op_type: 'remove_link', ref_link_pk: 'L1' })
    )
    expect(addChange).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ op_type: 'remove_link', ref_link_pk: 'L2' })
    )
    expect(addChange).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        op_type: 'remove_device',
        ref_device_pk: 'dB',
        ref_snapshot: expect.objectContaining({ device_code: 'lon-b', metro_code: 'lon' }),
      })
    )
  })

  // Reads the live draft each time rather than a cached list, so a link already
  // staged for removal (e.g. the operator manually removed it first) is never
  // staged a second time -- same stale-state concern, applied to a helper that
  // fires immediately instead of holding its own multi-step state.
  it('excludes a link already staged for removal from the helper', async () => {
    const addChange = vi.fn().mockResolvedValue(undefined)
    const setTool = vi.fn()
    const removeL1: PlanChange = {
      id: 'c1', plan_id: 'p', seq: 10, op_type: 'remove_link', ref_link_pk: 'L1',
      payload: {}, ref_snapshot: {}, state: 'pending', version: 1, created_at: '', updated_at: '',
    } as PlanChange
    plannerRef.current = makePlanner('remove-device', setTool, addChange, [removeL1])
    render(<PlannerMap />)

    fireEvent.click(screen.getByTitle('lon-b'))
    await flushMicrotasks()

    expect(addChange).toHaveBeenCalledTimes(2)
    expect(addChange).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({ op_type: 'remove_link', ref_link_pk: 'L2' })
    )
    expect(addChange).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ op_type: 'remove_device', ref_device_pk: 'dB' })
    )
  })

  // Review round 1 (Important concurrency defect): the backend assigns each change's
  // seq via an unlocked SELECT MAX(seq)+10 per transaction, so the helper MUST await
  // each addChange before firing the next -- otherwise remove_device can land a seq
  // below its own attached-link removals (engine "device still has attached link(s)"
  // error) or collide on the unique (plan_id, seq) constraint and drop a change.
  // This proves strict sequencing: with a deferred addChange that only resolves when
  // the test says so, each next call must NOT fire until the previous one resolves.
  it('awaits each staging call in sequence before firing the next', async () => {
    const calls: Array<{ op_type: string; ref_link_pk?: string; ref_device_pk?: string }> = []
    const resolvers: Array<() => void> = []
    const addChange = vi.fn((input: unknown) => {
      calls.push(input as (typeof calls)[number])
      return new Promise<void>((resolve) => resolvers.push(resolve))
    })
    const setTool = vi.fn()
    plannerRef.current = makePlanner('remove-device', setTool, addChange)
    render(<PlannerMap />)

    fireEvent.click(screen.getByTitle('lon-b')) // dB: attached to L1 and L2
    await flushMicrotasks()

    // Only the FIRST call has fired; the helper is blocked awaiting its promise. A
    // concurrent (unawaited) helper would have fired all three synchronously here.
    expect(addChange).toHaveBeenCalledTimes(1)
    expect(calls[0].op_type).toBe('remove_link')
    expect(calls[0].ref_link_pk).toBe('L1')

    // Resolving #1 lets the loop proceed to the second remove_link.
    resolvers[0]()
    await flushMicrotasks()
    expect(addChange).toHaveBeenCalledTimes(2)
    expect(calls[1].op_type).toBe('remove_link')
    expect(calls[1].ref_link_pk).toBe('L2')

    // Resolving #2 lets it proceed to the remove_device LAST.
    resolvers[1]()
    await flushMicrotasks()
    expect(addChange).toHaveBeenCalledTimes(3)
    expect(calls[2].op_type).toBe('remove_device')
    expect(calls[2].ref_device_pk).toBe('dB')

    resolvers[2]()
    await flushMicrotasks()

    // N+1 total (2 links + device), in strict order.
    expect(addChange).toHaveBeenCalledTimes(3)
    expect(calls.map((c) => c.op_type)).toEqual([
      'remove_link',
      'remove_link',
      'remove_device',
    ])
  })
})
