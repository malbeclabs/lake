import { useRef, useEffect, useState } from 'react'
import { ChevronDown } from 'lucide-react'
import { cn } from '@/lib/utils'

interface MultiSelectProps {
  allLabel: string          // e.g. "All contributors"
  activeLabel: string       // e.g. "Contributors"
  options: string[]
  selected: Set<string>
  onChange: (next: Set<string>) => void
  renderOption?: (value: string) => React.ReactNode  // optional leading element per option
  searchable?: boolean
}

export function MultiSelect({
  allLabel,
  activeLabel,
  options,
  selected,
  onChange,
  renderOption,
  searchable = false,
}: MultiSelectProps) {
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState('')
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  const filtered = query
    ? options.filter((o) => o.toLowerCase().includes(query.toLowerCase()))
    : options

  function toggle(value: string) {
    const next = new Set(selected)
    if (next.has(value)) next.delete(value)
    else next.add(value)
    onChange(next)
  }

  const label = selected.size === 0 ? allLabel : activeLabel
  const count = selected.size > 0 ? selected.size : null

  return (
    <div ref={ref} className="relative">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className={cn(
          'h-[30px] flex items-center gap-1.5 px-3 text-sm border border-border bg-[var(--input)] text-muted-foreground hover:text-foreground transition-colors',
          open && 'border-border/80'
        )}
      >
        <span>{label}</span>
        {count !== null && (
          <span className="text-[10px] bg-accent text-white px-1.5 leading-5 rounded-sm">
            {count}
          </span>
        )}
        <ChevronDown className="h-3.5 w-3.5 ml-0.5 opacity-60" />
      </button>

      {open && (
        <div className="absolute top-full left-0 mt-1 z-50 min-w-[200px] bg-[var(--input)] border border-border shadow-lg">
          {searchable && (
            <div className="p-2 border-b border-border">
              <input
                type="text"
                placeholder="Search…"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                className="w-full h-7 px-2 text-sm bg-[var(--card)] border border-border text-foreground placeholder:text-muted-foreground outline-none"
                autoFocus
              />
            </div>
          )}
          <div className="max-h-56 overflow-y-auto">
            {filtered.map((option) => (
              <label
                key={option}
                className="flex items-center gap-2.5 px-3 py-1.5 text-sm text-muted-foreground hover:text-foreground hover:bg-muted/30 cursor-pointer"
              >
                <input
                  type="checkbox"
                  checked={selected.has(option)}
                  onChange={() => toggle(option)}
                  className="accent-[var(--accent)]"
                />
                {renderOption?.(option)}
                <span>{option}</span>
              </label>
            ))}
            {filtered.length === 0 && (
              <div className="px-3 py-2 text-sm text-muted-foreground">No results</div>
            )}
          </div>
          <div className="flex border-t border-border">
            <button
              type="button"
              onClick={() => onChange(new Set())}
              className="flex-1 py-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              Clear
            </button>
            <div className="w-px bg-border" />
            <button
              type="button"
              onClick={() => onChange(new Set(options))}
              className="flex-1 py-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              Select all
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
