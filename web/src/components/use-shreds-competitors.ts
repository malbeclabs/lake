import { useQuery } from '@tanstack/react-query'

import { fetchShredsCompetitors } from '@/lib/api'

const WINDOW_DAYS = 30
const REFETCH_MS = 10 * 60 * 1000

// One query behind both the headline tile and the daily chart, so the two can
// never disagree about the latest closed day.
export function useShredsCompetitors() {
  return useQuery({
    queryKey: ['shreds-competitors', WINDOW_DAYS],
    queryFn: () => fetchShredsCompetitors(WINDOW_DAYS),
    refetchInterval: REFETCH_MS,
    staleTime: REFETCH_MS,
  })
}
