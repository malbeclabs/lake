import { describe, expect, it } from 'vitest'
import { sparklinePoints } from './sparkline'

describe('sparklinePoints', () => {
  it('maps the lowest value to the bottom and the highest to the top', () => {
    // y is inverted in SVG: the largest value sits at y=0.
    const out = sparklinePoints([10, 20], 100, 40)
    expect(out).toBe('0,40 100,0')
  })

  it('omits zero values so gaps break the line instead of dropping to the floor', () => {
    const out = sparklinePoints([10, 0, 20], 100, 40)
    expect(out).toBe('0,40 100,0')
  })

  it('renders a flat series at mid-height rather than dividing by zero', () => {
    expect(sparklinePoints([5, 5, 5], 100, 40)).toBe('0,20 50,20 100,20')
  })

  it('returns an empty string when there is nothing to plot', () => {
    expect(sparklinePoints([], 100, 40)).toBe('')
    expect(sparklinePoints([0, 0], 100, 40)).toBe('')
  })
})
