import { describe, it, expect } from 'vitest'
import { deriveLinkType } from './link-type'

describe('deriveLinkType', () => {
  it('is WAN for same contributor, different metros', () => {
    const a = { contributorKey: 'c1', metroKey: 'nyc' }
    const z = { contributorKey: 'c1', metroKey: 'lon' }
    expect(deriveLinkType(a, z)).toEqual({ valid: true, type: 'WAN' })
  })

  it('is DZX for different contributors, same metro', () => {
    const a = { contributorKey: 'c1', metroKey: 'nyc' }
    const z = { contributorKey: 'c2', metroKey: 'nyc' }
    expect(deriveLinkType(a, z)).toEqual({ valid: true, type: 'DZX' })
  })

  it('is ambiguous (null, operator picks) for same contributor, same metro', () => {
    const a = { contributorKey: 'c1', metroKey: 'nyc' }
    const z = { contributorKey: 'c1', metroKey: 'nyc' }
    expect(deriveLinkType(a, z)).toEqual({ valid: true, type: null, ambiguous: true })
  })

  it('is invalid for different contributors, different metros', () => {
    const a = { contributorKey: 'c1', metroKey: 'nyc' }
    const z = { contributorKey: 'c2', metroKey: 'lon' }
    const result = deriveLinkType(a, z)
    expect(result.valid).toBe(false)
    expect(result.type).toBeNull()
    expect(result.reason).toBe(
      'A cross-contributor link must be within one metro (DZX); a cross-metro link must be owned by one contributor (WAN). This pair is neither.'
    )
  })

  it('is invalid when an endpoint is missing its contributor or metro', () => {
    const known = { contributorKey: 'c1', metroKey: 'nyc' }
    const blankMetro = { contributorKey: 'c2', metroKey: '' }
    const blankContrib = { contributorKey: '', metroKey: 'lon' }

    const result1 = deriveLinkType(known, blankMetro)
    expect(result1.valid).toBe(false)
    expect(result1.type).toBeNull()
    expect(result1.reason).toBe(
      'Cannot determine link type: an endpoint is missing its contributor or metro.'
    )

    const result2 = deriveLinkType(blankContrib, known)
    expect(result2.valid).toBe(false)
    expect(result2.type).toBeNull()
  })
})
