import type { ShredSubscription } from '@/lib/api'

export type SubscriptionStatus = 'active' | 'pending' | 'expired'

/** Lifecycle order, which is also the order the status param serializes in. */
export const SUBSCRIPTION_STATUSES: SubscriptionStatus[] = ['active', 'pending', 'expired']

/**
 * The chips the table opens with. Expired is left out because the page is about
 * who is subscribed now: a seat past its termination is one the program should
 * already have taken off the pass, so it is a loose end to go looking for rather
 * than something to read the live picture through. The tab count above follows
 * these chips rather than the whole program — the tab on screen reports the
 * table's own filtered total and the other one is read at its own defaults — so
 * an expired seat sits outside both numbers until the chip is turned on.
 */
export const DEFAULT_SUBSCRIPTION_STATUSES: SubscriptionStatus[] = ['active', 'pending']

/** Milliseconds since the epoch, with a missing timestamp reading as long past. */
function subscriptionTime(value: string): number {
  const parsed = Date.parse(value)
  return Number.isNaN(parsed) ? 0 : parsed
}

/**
 * The three states a feed seat can be in, all cut on window_end — the same date
 * the Term Ends column shows, so the badge flips on the day that column names.
 * This is the same test the API's status filter applies in SQL
 * (shredSubscriptionStatusClauses), so a row's badge and the chips above the
 * table never disagree about it.
 */
export function getSubscriptionStatus(
  sub: Pick<ShredSubscription, 'window_end' | 'current_users'>,
  now: number,
): SubscriptionStatus {
  if (subscriptionTime(sub.window_end) <= now) return 'expired'
  return sub.current_users > 0 ? 'active' : 'pending'
}

export const subscriptionStatusConfig: Record<
  SubscriptionStatus,
  { label: string; className: string; dotClass: string }
> = {
  active: {
    label: 'Active',
    className: 'bg-green-500/10 text-green-600 dark:text-green-400 border-green-500/20',
    dotClass: 'bg-green-500',
  },
  pending: {
    label: 'Pending',
    className: 'bg-blue-500/10 text-blue-600 dark:text-blue-400 border-blue-500/20',
    dotClass: 'bg-blue-500',
  },
  expired: {
    label: 'Expired',
    className: 'bg-red-500/10 text-red-600 dark:text-red-400 border-red-500/20',
    dotClass: 'bg-red-500',
  },
}

/**
 * What the Connected column is counting.
 *
 * current_users is ticked at connect and released at disconnect, and it counts
 * only the users consuming this feed — measured across all 139 mainnet seats it
 * equals the payer's activated multicast users in the feed's metro exactly, and
 * excludes the ibrl users some of them also run there. So it is connections, not
 * pass holders, and the seat's caps belong in a tooltip rather than in the cell.
 */
export function connectedUsersTitle(
  sub: Pick<ShredSubscription, 'current_users' | 'max_users' | 'max_future_users'>,
): string {
  const base = `${sub.current_users} connected to this feed now, of ${sub.max_users} the seat allows until the term ends`
  return sub.max_future_users === sub.max_users
    ? base
    : `${base}; ${sub.max_future_users} after it`
}

/** Day granularity: these are billing dates, not events. */
export function subscriptionDate(value: string): string {
  if (!value) return '—'
  const parsed = Date.parse(value)
  if (Number.isNaN(parsed)) return '—'
  return new Date(parsed).toLocaleDateString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  })
}
