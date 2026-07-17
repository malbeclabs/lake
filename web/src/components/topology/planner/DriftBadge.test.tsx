import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { DriftBadge } from './DriftBadge'

describe('DriftBadge', () => {
  it('renders nothing for pending drift', () => {
    const { container } = render(<DriftBadge drift="pending" />)
    expect(container).toBeEmptyDOMElement()
  })

  it('shows "Broken" with the red/danger style for broken drift', () => {
    render(<DriftBadge drift="broken" />)
    const badge = screen.getByText('Broken')
    expect(badge.className).toContain('text-red-700')
  })

  it('shows "Already done" with the muted style for already_done drift', () => {
    render(<DriftBadge drift="already_done" />)
    const badge = screen.getByText('Already done')
    expect(badge.className).toContain('bg-muted')
    expect(badge.className).not.toContain('text-red-700')
  })
})
