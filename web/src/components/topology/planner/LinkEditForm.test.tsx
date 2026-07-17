import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { LinkEditForm } from './LinkEditForm'
import { UNSET_LATENCY_NS } from './estimator'
import type { DraftLink } from './draft'

function makeLink(over: Partial<DraftLink> = {}): DraftLink {
  return {
    pk: 'L1',
    code: 'nyc-lon1',
    status: 'active',
    link_type: 'WAN',
    bandwidth_bps: 10_000_000_000,
    side_a_pk: 'dA',
    side_a_code: 'nyc-x1',
    side_a_iface_name: 'eth0',
    side_a_ip: '',
    side_z_pk: 'dB',
    side_z_code: 'lon-x1',
    side_z_iface_name: 'eth1',
    side_z_ip: '',
    contributor_pk: '',
    contributor_code: '',
    side_a_contributor_pk: '',
    side_a_contributor_code: '',
    side_z_contributor_pk: '',
    side_z_contributor_code: '',
    latency_us: 5_000,
    jitter_us: 0,
    latency_a_to_z_us: 0,
    jitter_a_to_z_us: 0,
    latency_z_to_a_us: 0,
    jitter_z_to_a_us: 0,
    loss_percent: 0,
    sample_count: 0,
    in_bps: 0,
    out_bps: 0,
    committed_rtt_ns: 5_000_000,
    isis_delay_override_ns: 0,
    changeState: 'unchanged',
    ...over,
  }
}

describe('LinkEditForm', () => {
  it('pre-fills latency (ms) and bandwidth (Gbps) from the link', () => {
    render(
      <LinkEditForm link={makeLink()} onSubmit={vi.fn()} onCancel={vi.fn()} />
    )
    expect(screen.getByLabelText(/Latency/)).toHaveValue(5)
    expect(screen.getByLabelText(/Bandwidth/)).toHaveValue(10)
  })

  it('rounds latency to ns and bandwidth to bps on save', () => {
    const onSubmit = vi.fn()
    render(
      <LinkEditForm link={makeLink()} onSubmit={onSubmit} onCancel={vi.fn()} />
    )
    fireEvent.change(screen.getByLabelText(/Latency/), { target: { value: '1.5005' } })
    fireEvent.change(screen.getByLabelText(/Bandwidth/), { target: { value: '2.5' } })
    fireEvent.click(screen.getByText('Save'))
    expect(onSubmit).toHaveBeenCalledWith(1_500_500, 2_500_000_000)
  })

  it('rejects a non-positive latency and does not submit', () => {
    const onSubmit = vi.fn()
    render(
      <LinkEditForm link={makeLink()} onSubmit={onSubmit} onCancel={vi.fn()} />
    )
    fireEvent.change(screen.getByLabelText(/Latency/), { target: { value: '0' } })
    fireEvent.click(screen.getByText('Save'))
    expect(onSubmit).not.toHaveBeenCalled()
  })

  it('rejects a non-numeric bandwidth and does not submit', () => {
    const onSubmit = vi.fn()
    render(
      <LinkEditForm link={makeLink()} onSubmit={onSubmit} onCancel={vi.fn()} />
    )
    fireEvent.change(screen.getByLabelText(/Bandwidth/), { target: { value: 'abc' } })
    fireEvent.click(screen.getByText('Save'))
    expect(onSubmit).not.toHaveBeenCalled()
  })

  it('calls onCancel when Cancel is clicked', () => {
    const onCancel = vi.fn()
    render(
      <LinkEditForm link={makeLink()} onSubmit={vi.fn()} onCancel={onCancel} />
    )
    fireEvent.click(screen.getByText('Cancel'))
    expect(onCancel).toHaveBeenCalledTimes(1)
  })

  // CRITICAL (review round 1): the form must never carry one link's edited state
  // onto another. Open edit on A, change values, switch the selected link to B
  // without saving, then save -> the staged values must be B's, not A's edits.
  it('re-initializes its fields when the selected link changes (no cross-link carry)', () => {
    const onSubmit = vi.fn()
    const linkA = makeLink({ pk: 'A', code: 'a-link', latency_us: 5_000, bandwidth_bps: 10e9 })
    const linkB = makeLink({ pk: 'B', code: 'b-link', latency_us: 2_000, bandwidth_bps: 100e9 })

    const { rerender } = render(
      <LinkEditForm link={linkA} onSubmit={onSubmit} onCancel={vi.fn()} />
    )
    // Edit A's fields but do NOT save.
    fireEvent.change(screen.getByLabelText(/Latency/), { target: { value: '9999' } })
    fireEvent.change(screen.getByLabelText(/Bandwidth/), { target: { value: '400' } })

    // Selection switches to B (same component instance, new link prop).
    rerender(<LinkEditForm link={linkB} onSubmit={onSubmit} onCancel={vi.fn()} />)

    // Fields now show B's values, not A's abandoned edits.
    expect(screen.getByLabelText(/Latency/)).toHaveValue(2)
    expect(screen.getByLabelText(/Bandwidth/)).toHaveValue(100)

    // Saving stages B's re-initialized values under B, never A's 9999/400.
    fireEvent.click(screen.getByText('Save'))
    expect(onSubmit).toHaveBeenCalledTimes(1)
    expect(onSubmit).toHaveBeenCalledWith(2_000_000, 100_000_000_000)
  })

  // IMPORTANT (review round 1): a latency that converts to the 1e9-ns unset
  // sentinel must be rejected (the impact engine silently drops such an edge).
  it('rejects a latency that equals the unset sentinel (1e9 ns) and shows an error', () => {
    const onSubmit = vi.fn()
    render(
      <LinkEditForm link={makeLink()} onSubmit={onSubmit} onCancel={vi.fn()} />
    )
    // 1,000 ms * 1e6 = 1e9 ns = UNSET_LATENCY_NS.
    expect(UNSET_LATENCY_NS).toBe(1_000 * 1e6)
    fireEvent.change(screen.getByLabelText(/Latency/), { target: { value: '1000' } })
    fireEvent.click(screen.getByText('Save'))
    expect(onSubmit).not.toHaveBeenCalled()
    expect(screen.getByRole('alert')).toBeInTheDocument()
  })

  it('rejects a non-positive bandwidth and does not submit', () => {
    const onSubmit = vi.fn()
    render(
      <LinkEditForm link={makeLink()} onSubmit={onSubmit} onCancel={vi.fn()} />
    )
    fireEvent.change(screen.getByLabelText(/Bandwidth/), { target: { value: '0' } })
    fireEvent.click(screen.getByText('Save'))
    expect(onSubmit).not.toHaveBeenCalled()
    expect(screen.getByRole('alert')).toBeInTheDocument()
  })

  it('accepts a valid edit just below the sentinel', () => {
    const onSubmit = vi.fn()
    render(
      <LinkEditForm link={makeLink()} onSubmit={onSubmit} onCancel={vi.fn()} />
    )
    fireEvent.change(screen.getByLabelText(/Latency/), { target: { value: '999.999' } })
    fireEvent.change(screen.getByLabelText(/Bandwidth/), { target: { value: '10' } })
    fireEvent.click(screen.getByText('Save'))
    expect(onSubmit).toHaveBeenCalledWith(999_999_000, 10_000_000_000)
  })
})
