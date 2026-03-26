// Distinct pastel color palette for chart series (up to 20)
const SERIES_COLORS_DARK = [
  '#6dd5a0', '#7aabe8', '#f0a870', '#c49aee', '#ef8a8a',
  '#5dc8d9', '#e0c56a', '#ee8ab8', '#5dc4b6', '#a48cee',
  '#edb96a', '#8b8ef5', '#5cc8a0', '#e87090', '#60bce8',
  '#a4d468', '#d88aee', '#f08090', '#5aacc4', '#9a7ae8',
]
const SERIES_COLORS_LIGHT = [
  '#4aaa7a', '#5a8ac8', '#d08a50', '#a07acc', '#cc6060',
  '#4a9aaa', '#b09840', '#c05a90', '#4a9a88', '#7a6acc',
  '#c09040', '#6a68cc', '#3a9a6a', '#c04a6a', '#4a8ab0',
  '#7a9a40', '#a860b0', '#c86068', '#4a8a9a', '#7a58c0',
]

export function getSeriesColors(isDark: boolean) {
  return isDark ? SERIES_COLORS_DARK : SERIES_COLORS_LIGHT
}
