import { useMemo, useState } from 'react'
import { Search, X, MapPin, Server } from 'lucide-react'
import { filterLocations, type LocationOption } from '@/lib/path-calculator'

function KindBadge({ kind }: { kind: LocationOption['kind'] }) {
  const isMetro = kind === 'metro'
  return (
    <span
      className={`inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-[10px] font-medium ${
        isMetro
          ? 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400'
          : 'bg-muted text-muted-foreground'
      }`}
    >
      {isMetro ? <MapPin className="h-3 w-3" /> : <Server className="h-3 w-3" />}
      {isMetro ? 'Metro' : 'Device'}
    </span>
  )
}

export function LocationSearch({
  label,
  placeholder,
  value,
  onChange,
  options,
  excludePK,
}: {
  label: string
  placeholder: string
  value: LocationOption | null
  onChange: (o: LocationOption | null) => void
  options: LocationOption[]
  excludePK?: string
}) {
  const [search, setSearch] = useState('')
  const [isOpen, setIsOpen] = useState(false)

  const results = useMemo(
    () => filterLocations(options, search, excludePK),
    [options, search, excludePK],
  )

  return (
    <div className="flex-1 w-full">
      <label className="block text-sm font-medium text-muted-foreground mb-2">{label}</label>
      <div className="relative">
        {value ? (
          <div className="flex items-center gap-2 px-3 py-2 border border-border rounded-md bg-card">
            <KindBadge kind={value.kind} />
            <span className="font-mono text-sm flex-1 truncate">
              {value.code}
              {value.kind === 'metro' && value.name && (
                <span className="text-muted-foreground font-sans"> · {value.name}</span>
              )}
            </span>
            <button
              onClick={() => {
                onChange(null)
                setSearch('')
              }}
              className="p-1 hover:bg-muted rounded"
              title="Clear"
            >
              <X className="h-4 w-4 text-muted-foreground" />
            </button>
          </div>
        ) : (
          <>
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <input
                type="text"
                value={search}
                onChange={(e) => {
                  setSearch(e.target.value)
                  setIsOpen(true)
                }}
                onFocus={() => setIsOpen(true)}
                onBlur={() => setTimeout(() => setIsOpen(false), 150)}
                placeholder={placeholder}
                className="w-full pl-9 pr-3 py-2 border border-border rounded-md bg-card text-sm focus:outline-none focus:ring-2 focus:ring-primary/50"
              />
            </div>
            {isOpen && search && results.length > 0 && (
              <div className="absolute z-50 w-full mt-1 bg-card border border-border rounded-md shadow-lg max-h-60 overflow-y-auto">
                {results.map((opt) => (
                  <button
                    key={`${opt.kind}:${opt.pk}`}
                    onMouseDown={(e) => e.preventDefault()}
                    onClick={() => {
                      onChange(opt)
                      setSearch('')
                      setIsOpen(false)
                    }}
                    className="w-full px-3 py-2 text-left hover:bg-muted flex items-center gap-2"
                  >
                    <KindBadge kind={opt.kind} />
                    <span className="font-mono text-sm truncate">{opt.code}</span>
                    <span className="text-xs text-muted-foreground truncate ml-auto capitalize">
                      {opt.kind === 'metro' ? opt.name : opt.deviceType}
                    </span>
                  </button>
                ))}
              </div>
            )}
            {isOpen && search && results.length === 0 && (
              <div className="absolute z-50 w-full mt-1 bg-card border border-border rounded-md shadow-lg p-3 text-sm text-muted-foreground">
                No matching metros or devices
              </div>
            )}
          </>
        )}
      </div>
    </div>
  )
}
