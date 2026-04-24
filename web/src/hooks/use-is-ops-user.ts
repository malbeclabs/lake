// web/src/hooks/use-is-ops-user.ts
import { useAuth } from '@/contexts/AuthContext'

/**
 * Returns true if the current user is authenticated with an internal-domain
 * Google account (@doublezero.xyz, @doublezero.us, @malbeclabs.com).
 * This mirrors the server-side isInternalDomain() check in auth.go.
 */
export function useIsOpsUser(): boolean {
  const { user } = useAuth()
  return user?.is_internal_user === true
}
