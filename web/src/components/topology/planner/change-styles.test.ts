import { describe, it, expect } from 'vitest'
import { linkChangeStyle, deviceChangeStyle, CHANGE_LEGEND } from './change-styles'

describe('linkChangeStyle', () => {
  it('greys and strikes removed links', () => {
    const s = linkChangeStyle('removed', true)
    expect(s.struck).toBe(true)
    expect(s.dashed).toBe(true)
    expect(s.opacity).toBeLessThan(0.6)
  })
  it('uses a planned colour for added links, not struck', () => {
    const s = linkChangeStyle('added', true)
    expect(s.struck).toBe(false)
    expect(s.color).toBe('#10b981')
  })
  it('uses a modified colour for modified links', () => {
    expect(linkChangeStyle('modified', true).color).toBe('#f59e0b')
  })
})

describe('deviceChangeStyle', () => {
  it('rings added devices', () => {
    expect(deviceChangeStyle('added', true).ring).toBe(true)
  })
  it('strikes removed devices', () => {
    expect(deviceChangeStyle('removed', true).struck).toBe(true)
  })
})

describe('CHANGE_LEGEND', () => {
  it('covers added, removed and modified', () => {
    const states = CHANGE_LEGEND.map((l) => l.state)
    expect(states).toEqual(expect.arrayContaining(['added', 'removed', 'modified']))
  })
})
