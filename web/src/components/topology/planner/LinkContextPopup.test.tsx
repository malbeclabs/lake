import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { LinkContextPopup } from './LinkContextPopup'
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
    latency_us: 1_500,
    jitter_us: 0,
    latency_a_to_z_us: 0,
    jitter_a_to_z_us: 0,
    latency_z_to_a_us: 0,
    jitter_z_to_a_us: 0,
    loss_percent: 0,
    sample_count: 0,
    in_bps: 0,
    out_bps: 0,
    committed_rtt_ns: 1_500_000,
    isis_delay_override_ns: 0,
    changeState: 'unchanged',
    ...over,
  }
}

describe('LinkContextPopup', () => {
  it('shows the link code and a latency/bandwidth summary', () => {
    render(<LinkContextPopup link={makeLink()} onDelete={vi.fn()} onEdit={vi.fn()} />)
    expect(screen.getByText('nyc-lon1')).toBeInTheDocument()
    expect(screen.getByText('1.50 ms · 10 Gbps')).toBeInTheDocument()
  })

  it('calls onEdit when "Edit latency / bandwidth" is clicked', () => {
    const onEdit = vi.fn()
    render(<LinkContextPopup link={makeLink()} onDelete={vi.fn()} onEdit={onEdit} />)
    fireEvent.click(screen.getByText('Edit latency / bandwidth'))
    expect(onEdit).toHaveBeenCalledTimes(1)
  })

  it('calls onDelete when "Delete link" is clicked on a not-yet-removed link', () => {
    const onDelete = vi.fn()
    render(<LinkContextPopup link={makeLink()} onDelete={onDelete} onEdit={vi.fn()} />)
    const button = screen.getByText('Delete link')
    expect(button).toBeEnabled()
    fireEvent.click(button)
    expect(onDelete).toHaveBeenCalledTimes(1)
  })

  it('disables delete and shows "Already removed" once the link is staged as removed', () => {
    const onDelete = vi.fn()
    render(
      <LinkContextPopup
        link={makeLink({ changeState: 'removed' })}
        onDelete={onDelete}
        onEdit={vi.fn()}
      />
    )
    const button = screen.getByText('Already removed')
    expect(button).toBeDisabled()
    fireEvent.click(button)
    expect(onDelete).not.toHaveBeenCalled()
  })

  it('always shows the drag hint', () => {
    render(<LinkContextPopup link={makeLink()} onDelete={vi.fn()} onEdit={vi.fn()} />)
    expect(screen.getByText('Drag an endpoint handle to move it.')).toBeInTheDocument()
  })
})
