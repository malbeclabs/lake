import { Navigate } from 'react-router-dom'
import { useAuth } from '@/contexts/AuthContext'

// Route guard for the pages the sidebar already hides behind `user?.is_internal_user`.
//
// It exists because the sidebar and the router were deciding the same thing and only one of them
// enforced it. Hiding the nav entry keeps an unannounced venue out of the way of anyone browsing;
// it does nothing about the URL, and the page itself is the disclosure. The API returns 403 either
// way, so no data escapes — what escapes is the venue's name, the feature's name, and the
// vocabulary in the column headings, which is exactly what the sidebar comment says it is keeping
// back.
//
// Redirects rather than rendering nothing: a blank page inside our own app is its own puzzle, and
// the anonymous visitor has no way to tell it from a broken one.
export function InternalOnly({ children }: { children: React.ReactNode }) {
  const { user, isLoading } = useAuth()

  // The load has to finish before this can decide anything, and getting that wrong is worse than
  // no guard at all. `user` is null while the session resolves, so deciding early bounces an
  // internal user who hard-refreshes one of these URLs, or opens one from a bookmark or a Slack
  // link — the page would work only when reached from inside the app, which is the shape of bug
  // nobody reproduces on the first try. Render nothing for that moment instead: it is the same
  // blank the rest of the app shows while auth resolves, and it ends either way.
  if (isLoading) return null
  if (!user?.is_internal_user) return <Navigate to="/" replace />
  return <>{children}</>
}
