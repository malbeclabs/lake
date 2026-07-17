import type { DraftTopology, DraftLink } from './draft'

// Links touching a device in the draft, excluding those already staged removed.
export function attachedLinks(draft: DraftTopology, deviceKey: string): DraftLink[] {
  return draft.links.filter(
    (l) =>
      l.changeState !== 'removed' &&
      (l.side_a_pk === deviceKey || l.side_z_pk === deviceKey)
  )
}
