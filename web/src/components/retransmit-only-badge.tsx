interface RetransmitOnlyBadgeProps {
  /** `retransmit_only_enabled` as returned by the API (0 or 1). */
  enabled: number
}

/**
 * Marks a metro (or a device inheriting its metro's flag) as serving the
 * retransmit multicast group only — the leader group is excluded, so a seat
 * there does not receive leader shreds.
 */
export function RetransmitOnlyBadge({ enabled }: RetransmitOnlyBadgeProps) {
  if (!enabled) {
    return null
  }

  return (
    <span
      className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium bg-amber-500/10 text-amber-600 dark:text-amber-400 border border-amber-500/20"
      title="This metro serves the retransmit multicast group only (leader group excluded)"
    >
      retransmit-only
    </span>
  )
}
