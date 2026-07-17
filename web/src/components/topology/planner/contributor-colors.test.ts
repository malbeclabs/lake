import { describe, it, expect } from 'vitest'
import { buildContributorColors, CONTRIBUTOR_COLORS } from './contributor-colors'
import type { DraftDevice, DraftLink } from './draft'

describe('buildContributorColors', () => {
  it('assigns two contributors distinct, stable colors', () => {
    const devices = [
      { pk: 'd1', contributor_pk: 'cB', contributor_code: 'bravo' },
      { pk: 'd2', contributor_pk: 'cA', contributor_code: 'alpha' },
    ] as DraftDevice[]
    const { colorByPk } = buildContributorColors(devices, [])
    expect(colorByPk.get('cA')).not.toBe(colorByPk.get('cB'))
    expect(colorByPk.get('cA')).toBeTruthy()
    expect(colorByPk.get('cB')).toBeTruthy()
  })

  it('assigns color index by contributor code sorted ascending', () => {
    // 'alpha' < 'bravo' alphabetically, so cA (alpha) gets index 0 regardless of
    // which order the devices appear in.
    const devices = [
      { pk: 'd1', contributor_pk: 'cB', contributor_code: 'bravo' },
      { pk: 'd2', contributor_pk: 'cA', contributor_code: 'alpha' },
    ] as DraftDevice[]
    const { colorByPk } = buildContributorColors(devices, [])
    expect(colorByPk.get('cA')).toBe(CONTRIBUTOR_COLORS[0])
    expect(colorByPk.get('cB')).toBe(CONTRIBUTOR_COLORS[1])
  })

  it('counts devices and links per contributor', () => {
    const devices = [
      { pk: 'd1', contributor_pk: 'cA', contributor_code: 'alpha' },
      { pk: 'd2', contributor_pk: 'cA', contributor_code: 'alpha' },
      { pk: 'd3', contributor_pk: 'cB', contributor_code: 'bravo' },
    ] as DraftDevice[]
    const links = [
      {
        pk: 'l1', contributor_pk: 'cA', contributor_code: 'alpha',
        side_a_contributor_pk: 'cA', side_a_contributor_code: 'alpha',
        side_z_contributor_pk: 'cB', side_z_contributor_code: 'bravo',
      },
    ] as DraftLink[]
    const { contributors } = buildContributorColors(devices, links)
    const alpha = contributors.find((c) => c.pk === 'cA')
    const bravo = contributors.find((c) => c.pk === 'cB')
    expect(alpha?.deviceCount).toBe(2)
    expect(alpha?.linkCount).toBe(1)
    expect(bravo?.deviceCount).toBe(1)
    expect(bravo?.linkCount).toBe(0)
  })

  it('falls back to side_a_contributor_pk when a link has no contributor_pk', () => {
    const links = [
      {
        pk: 'l1', contributor_pk: '', contributor_code: '',
        side_a_contributor_pk: 'cA', side_a_contributor_code: 'alpha',
        side_z_contributor_pk: 'cB', side_z_contributor_code: 'bravo',
      },
    ] as DraftLink[]
    const { contributors } = buildContributorColors([], links)
    const alpha = contributors.find((c) => c.pk === 'cA')
    const bravo = contributors.find((c) => c.pk === 'cB')
    expect(alpha?.linkCount).toBe(1)
    expect(bravo?.linkCount).toBe(0)
  })

  it('sorts the legend by device count descending, then code ascending', () => {
    const devices = [
      { pk: 'd1', contributor_pk: 'cA', contributor_code: 'alpha' },
      { pk: 'd2', contributor_pk: 'cB', contributor_code: 'bravo' },
      { pk: 'd3', contributor_pk: 'cB', contributor_code: 'bravo' },
    ] as DraftDevice[]
    const { contributors } = buildContributorColors(devices, [])
    expect(contributors.map((c) => c.pk)).toEqual(['cB', 'cA'])
  })
})
