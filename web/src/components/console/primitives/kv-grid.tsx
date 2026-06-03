import type { ReactNode } from 'react'

export interface KvRow {
  label: ReactNode
  value: ReactNode
}

interface KvGridProps {
  rows: KvRow[]
  className?: string
}

export function KvGrid({ rows, className = '' }: KvGridProps) {
  return (
    <dl
      className={`grid gap-y-[7px] gap-x-3.5 text-[13px] ${className}`}
      style={{ gridTemplateColumns: '110px 1fr' }}
    >
      {rows.map((r, i) => (
        <RowFragment key={i} row={r} />
      ))}
    </dl>
  )
}

function RowFragment({ row }: { row: KvRow }) {
  return (
    <>
      <dt className="text-muted-foreground">{row.label}</dt>
      <dd className="m-0 text-foreground">{row.value}</dd>
    </>
  )
}
