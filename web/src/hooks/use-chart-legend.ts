import { useState, useCallback } from 'react'

export interface UseChartLegendReturn {
  hoveredSeries: string | null
  selectedSeries: Set<string>
  handleClick: (key: string, event: React.MouseEvent) => void
  handleMouseEnter: (key: string) => void
  handleMouseLeave: () => void
  getOpacity: (key: string) => number
}

export function useChartLegend(_allKeys: string[]): UseChartLegendReturn {
  const [hoveredSeries, setHoveredSeries] = useState<string | null>(null)
  const [selectedSeries, setSelectedSeries] = useState<Set<string>>(new Set())

  const handleClick = useCallback((key: string, event: React.MouseEvent) => {
    if (event.ctrlKey || event.metaKey) {
      // Ctrl/Cmd+Click: toggle individual
      setSelectedSeries(prev => {
        const next = new Set(prev)
        if (next.has(key)) {
          next.delete(key)
        } else {
          next.add(key)
        }
        return next
      })
    } else {
      // Plain click: solo (or deselect if already solo'd)
      setSelectedSeries(prev => {
        if (prev.size === 1 && prev.has(key)) {
          return new Set()
        }
        return new Set([key])
      })
    }
  }, [])

  const handleMouseEnter = useCallback((key: string) => {
    setHoveredSeries(key)
  }, [])

  const handleMouseLeave = useCallback(() => {
    setHoveredSeries(null)
  }, [])

  const getOpacity = useCallback((key: string): number => {
    // If nothing selected and nothing hovered, all active
    if (selectedSeries.size === 0 && !hoveredSeries) return 1

    // If hovering, highlight hovered + selected, dim others
    if (hoveredSeries) {
      if (key === hoveredSeries) return 1
      if (selectedSeries.size > 0 && selectedSeries.has(key)) return 1
      return 0.2
    }

    // If selected but not hovering, show selected, dim others
    if (selectedSeries.size > 0) {
      return selectedSeries.has(key) ? 1 : 0.2
    }

    return 1
  }, [hoveredSeries, selectedSeries])

  return {
    hoveredSeries,
    selectedSeries,
    handleClick,
    handleMouseEnter,
    handleMouseLeave,
    getOpacity,
  }
}
