import { describe, expect, it } from 'vitest'
import { sparklinePoints } from './sparkline-points'

describe('sparklinePoints', () => {
  it('maps the lowest value to the bottom and the highest to the top', () => {
    // y is inverted in SVG: the largest value sits at y=0.
    const out = sparklinePoints([10, 20], 100, 40)
    expect(out).toEqual(['0,40 100,0'])
  })

  it('splits into separate segments at a zero so gaps break the line instead of bridging it', () => {
    const out = sparklinePoints([10, 0, 20], 100, 40)
    expect(out).toEqual(['0,40', '100,0'])
  })

  it('renders a flat series at mid-height rather than dividing by zero', () => {
    expect(sparklinePoints([5, 5, 5], 100, 40)).toEqual(['0,20 50,20 100,20'])
  })

  it('returns an empty array when there is nothing to plot', () => {
    expect(sparklinePoints([], 100, 40)).toEqual([])
    expect(sparklinePoints([0, 0], 100, 40)).toEqual([])
  })

  it('preserves original index positions across a gap rather than re-indexing after filtering', () => {
    // If the gap were closed up before computing x, the run starting at index 3
    // would land at x=50 (re-indexed) instead of x=75 (its true position of 5).
    const out = sparklinePoints([10, 20, 0, 30, 40], 100, 40)
    expect(out).toHaveLength(2)
    expect(out[1].startsWith('75,')).toBe(true)
  })
})
