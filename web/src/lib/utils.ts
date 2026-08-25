import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

// handleRowClick sends a row click to `url`, unless the click landed on something interactive.
//
// `state` is forwarded to the router, which is how a drill-down carries its way back: every detail
// page in this app reads `state.back` (see use-back-link.ts) and falls back to its own list page
// when there is none. Pass it and the reader returns to the page they actually came from.
//
// The cmd/ctrl path opens a new tab instead, and router state does not survive that — a new tab has
// no history to go back through either, so there is nothing to carry.
export function handleRowClick(
  e: React.MouseEvent,
  url: string,
  navigate: (url: string, options?: { state?: unknown }) => void,
  state?: unknown
): void {
  // Don't handle if the click was on an interactive element (link, button, input)
  const target = e.target as HTMLElement
  if (target.closest('a, button, input')) return

  if (e.metaKey || e.ctrlKey) {
    window.open(url, '_blank')
  } else {
    navigate(url, state === undefined ? undefined : { state })
  }
}
