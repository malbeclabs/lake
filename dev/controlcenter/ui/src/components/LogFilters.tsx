interface LogFiltersProps {
  service: string
  level: string
  keyword: string
  onServiceChange: (service: string) => void
  onLevelChange: (level: string) => void
  onKeywordChange: (keyword: string) => void
}

export function LogFilters({
  service,
  level,
  keyword,
  onServiceChange,
  onLevelChange,
  onKeywordChange,
}: LogFiltersProps) {
  return (
    <div className="flex flex-wrap gap-4 p-4 bg-card border border-border rounded-lg transition-colors">
      <div className="flex-1 min-w-[200px]">
        <label className="block text-sm font-medium mb-1">Service</label>
        <select
          value={service}
          onChange={(e) => onServiceChange(e.target.value)}
          className="w-full px-3 py-2 rounded-md border border-input bg-background"
        >
          <option value="all">All Services</option>
          <option value="indexer">Indexer</option>
          <option value="api">API</option>
          <option value="web">Web</option>
        </select>
      </div>

      <div className="flex-1 min-w-[200px]">
        <label className="block text-sm font-medium mb-1">Log Level</label>
        <select
          value={level}
          onChange={(e) => onLevelChange(e.target.value)}
          className="w-full px-3 py-2 rounded-md border border-input bg-background"
        >
          <option value="all">All Levels</option>
          <option value="ERROR">ERROR</option>
          <option value="WARN">WARN</option>
          <option value="INFO">INFO</option>
          <option value="DEBUG">DEBUG</option>
        </select>
      </div>

      <div className="flex-1 min-w-[200px]">
        <label className="block text-sm font-medium mb-1">Keyword Search</label>
        <input
          type="text"
          value={keyword}
          onChange={(e) => onKeywordChange(e.target.value)}
          placeholder="Filter logs..."
          className="w-full px-3 py-2 rounded-md border border-input bg-background"
        />
      </div>
    </div>
  )
}
