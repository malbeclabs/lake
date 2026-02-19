import type { LucideIcon } from 'lucide-react'
import type { ReactNode } from 'react'
import { useDocumentTitle } from '@/hooks/use-document-title'

interface PageHeaderProps {
  icon?: LucideIcon
  title: string
  count?: number
  subtitle?: ReactNode
  actions?: ReactNode
}

export function PageHeader({ icon: Icon, title, count, subtitle, actions }: PageHeaderProps) {
  useDocumentTitle(title)

  return (
    <div className="flex flex-wrap items-center justify-between gap-3 mb-6">
      <div className="flex items-center gap-3">
        {Icon && <Icon className="h-6 w-6 text-muted-foreground" />}
        <h1 className="text-2xl font-medium">{title}</h1>
        {count !== undefined && <span className="text-muted-foreground">({count})</span>}
        {subtitle}
      </div>
      {actions && <div className="flex items-center gap-2 flex-wrap">{actions}</div>}
    </div>
  )
}
