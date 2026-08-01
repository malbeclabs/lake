import { describe, it, expect } from 'vitest'
import { contributorHue, evBg, evBorderColor, evText, dotColor } from './colors'

describe('contributorHue', () => {
  it('returns a value in [0, 360)', () => {
    expect(contributorHue('RockawayX')).toBeGreaterThanOrEqual(0)
    expect(contributorHue('RockawayX')).toBeLessThan(360)
  })

  it('is deterministic — same name always returns same hue', () => {
    expect(contributorHue('Malbec Labs')).toBe(contributorHue('Malbec Labs'))
  })

  it('different names produce different hues', () => {
    expect(contributorHue('RockawayX')).not.toBe(contributorHue('Malbec Labs'))
  })

  it('handles empty string without throwing', () => {
    expect(() => contributorHue('')).not.toThrow()
  })
})

describe('color helpers', () => {
  it('evBg returns oklch string with hue and default alpha', () => {
    expect(evBg(120)).toBe('oklch(0.62 0.14 120 / 30%)')
  })

  it('evBg accepts custom alpha', () => {
    expect(evBg(120, 50)).toBe('oklch(0.62 0.14 120 / 50%)')
  })

  it('evBorderColor returns oklch string with hue and default alpha', () => {
    expect(evBorderColor(240)).toBe('oklch(0.68 0.16 240 / 90%)')
  })

  it('evText returns oklch string with hue', () => {
    expect(evText(60)).toBe('oklch(0.78 0.14 60)')
  })

  it('dotColor returns oklch string with hue', () => {
    expect(dotColor(300)).toBe('oklch(0.65 0.15 300)')
  })
})
