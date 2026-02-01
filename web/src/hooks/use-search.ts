import { useState, useEffect, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fetchAutocomplete, getEnv, type SearchSuggestion } from '@/lib/api'

const RECENT_SEARCHES_BASE_KEY = 'search-recent'
const MAX_RECENT_SEARCHES = 5

function recentSearchesKey() {
  const env = getEnv()
  return env === 'mainnet-beta' ? RECENT_SEARCHES_BASE_KEY : `${RECENT_SEARCHES_BASE_KEY}-${env}`
}

export function useSearchAutocomplete(query: string, enabled = true) {
  return useQuery({
    queryKey: ['search-autocomplete', query],
    queryFn: () => fetchAutocomplete(query),
    enabled: enabled && query.length >= 2,
    staleTime: 30000,
    gcTime: 60000,
  })
}

export function useRecentSearches() {
  const [recentSearches, setRecentSearches] = useState<SearchSuggestion[]>([])

  useEffect(() => {
    const stored = localStorage.getItem(recentSearchesKey())
    if (stored) {
      try {
        setRecentSearches(JSON.parse(stored))
      } catch {
        // Invalid JSON, ignore
      }
    }
  }, [])

  const addRecentSearch = useCallback((item: SearchSuggestion) => {
    setRecentSearches(prev => {
      // Remove duplicate if exists
      const filtered = prev.filter(s => s.id !== item.id || s.type !== item.type)
      // Add to front
      const updated = [item, ...filtered].slice(0, MAX_RECENT_SEARCHES)
      localStorage.setItem(recentSearchesKey(), JSON.stringify(updated))
      return updated
    })
  }, [])

  const clearRecentSearches = useCallback(() => {
    setRecentSearches([])
    localStorage.removeItem(recentSearchesKey())
  }, [])

  return { recentSearches, addRecentSearch, clearRecentSearches }
}
