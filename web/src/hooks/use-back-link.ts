import { useLocation } from 'react-router-dom'

export interface BackTarget {
  to: string
  label: string
}

export function useBackLink(defaultTarget: BackTarget): BackTarget {
  const location = useLocation()
  const back = (location.state as { back?: BackTarget } | null)?.back
  if (back && typeof back.to === 'string' && typeof back.label === 'string') {
    return back
  }
  return defaultTarget
}
