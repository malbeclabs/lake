import { useState } from 'react'
import { Link } from 'react-router-dom'
import {
  PanelLeftClose,
  PanelLeftOpen,
  MoreHorizontal,
  Pencil,
  Trash2,
  RefreshCw,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { ConfirmDialog } from './confirm-dialog'

interface SessionItem {
  id: string
  title: string
  isActive: boolean
  url: string
}

interface SessionPanelProps {
  items: SessionItem[]
  newLabel: string
  isNewActive: boolean
  historyUrl: string
  onNew: (e: React.MouseEvent) => void
  onSelect: (id: string) => void
  onDelete: (id: string) => void
  onRename: (id: string, name: string) => void
  onGenerateTitle: (id: string) => Promise<void>
}

export function SessionPanel({
  items,
  newLabel,
  isNewActive,
  historyUrl,
  onNew,
  onSelect,
  onDelete,
  onRename,
  onGenerateTitle,
}: SessionPanelProps) {
  const [isOpen, setIsOpen] = useState(() => {
    const saved = localStorage.getItem('session-panel-open')
    return saved !== null ? saved === 'true' : true
  })
  const [deleteSession, setDeleteSession] = useState<{ id: string; title: string } | null>(null)

  const handleToggle = (open: boolean) => {
    setIsOpen(open)
    localStorage.setItem('session-panel-open', String(open))
  }

  if (!isOpen) {
    return (
      <div className="flex flex-col items-center py-3 border-r border-border/50 shrink-0">
        <button
          onClick={() => handleToggle(true)}
          className="p-1.5 text-muted-foreground hover:text-foreground transition-colors"
          title="Show sessions"
        >
          <PanelLeftOpen className="h-4 w-4" />
        </button>
      </div>
    )
  }

  return (
    <>
      <div className="w-56 border-r border-border/50 flex flex-col shrink-0 bg-[var(--sidebar)]">
        {/* Header */}
        <div className="px-3 py-2.5 flex items-center justify-between border-b border-border/50">
          <button
            onClick={onNew}
            className={cn(
              'text-sm px-2 py-1 rounded transition-colors',
              isNewActive
                ? 'bg-[var(--sidebar-active)] text-foreground font-medium'
                : 'text-muted-foreground hover:text-foreground hover:bg-[var(--sidebar-active)]'
            )}
          >
            {newLabel}
          </button>
          <button
            onClick={() => handleToggle(false)}
            className="p-1 text-muted-foreground hover:text-foreground transition-colors"
            title="Hide sessions"
          >
            <PanelLeftClose className="h-4 w-4" />
          </button>
        </div>

        {/* Sessions list */}
        <div className="flex-1 overflow-y-auto py-2">
          <div className="px-2 space-y-0.5">
            {items.map(item => (
              <SessionRow
                key={item.id}
                item={item}
                onSelect={() => onSelect(item.id)}
                onDelete={() => setDeleteSession({ id: item.id, title: item.title })}
                onRename={(name) => onRename(item.id, name)}
                onGenerateTitle={() => onGenerateTitle(item.id)}
              />
            ))}
          </div>
        </div>

        {/* Footer */}
        <div className="px-3 py-2 border-t border-border/50">
          <Link
            to={historyUrl}
            className="text-xs text-muted-foreground hover:text-foreground transition-colors"
          >
            View all sessions
          </Link>
        </div>
      </div>

      <ConfirmDialog
        isOpen={!!deleteSession}
        title="Delete session"
        message={`Delete "${deleteSession?.title}"? This cannot be undone.`}
        onConfirm={() => {
          if (deleteSession) {
            onDelete(deleteSession.id)
          }
          setDeleteSession(null)
        }}
        onCancel={() => setDeleteSession(null)}
      />
    </>
  )
}

function SessionRow({
  item,
  onSelect,
  onDelete,
  onRename,
  onGenerateTitle,
}: {
  item: SessionItem
  onSelect: () => void
  onDelete: () => void
  onRename: (name: string) => void
  onGenerateTitle: () => Promise<void>
}) {
  const [showMenu, setShowMenu] = useState(false)
  const [isRenaming, setIsRenaming] = useState(false)
  const [renameValue, setRenameValue] = useState('')
  const [isGenerating, setIsGenerating] = useState(false)

  const handleStartRename = () => {
    setRenameValue(item.title)
    setIsRenaming(true)
    setShowMenu(false)
  }

  const handleSaveRename = () => {
    const newName = renameValue.trim()
    if (newName && newName !== item.title) {
      onRename(newName)
    }
    setIsRenaming(false)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSaveRename()
    } else if (e.key === 'Escape') {
      setIsRenaming(false)
    }
  }

  const handleGenerateTitle = async () => {
    setShowMenu(false)
    setIsGenerating(true)
    try {
      await onGenerateTitle()
    } finally {
      setIsGenerating(false)
    }
  }

  if (isRenaming) {
    return (
      <div className="px-2 py-1">
        <input
          type="text"
          value={renameValue}
          onChange={(e) => setRenameValue(e.target.value)}
          onKeyDown={handleKeyDown}
          onBlur={handleSaveRename}
          autoFocus
          className="w-full text-sm bg-card border border-border px-2 py-1 focus:outline-none focus:border-foreground rounded"
        />
      </div>
    )
  }

  return (
    <div
      className={cn(
        'group relative flex items-center gap-1 px-2 py-1.5 cursor-pointer transition-colors rounded',
        item.isActive
          ? 'bg-[var(--sidebar-active)] text-foreground'
          : 'text-muted-foreground hover:text-foreground hover:bg-[var(--sidebar-active)]'
      )}
      onClick={(e) => {
        if (e.metaKey || e.ctrlKey) {
          window.open(item.url, '_blank')
        } else {
          onSelect()
        }
      }}
    >
      <div className={cn('flex-1 min-w-0 text-sm truncate', item.isActive && 'font-medium')}>
        {isGenerating ? (
          <span className="flex items-center gap-1">
            <RefreshCw className="h-3 w-3 animate-spin" />
            <span className="text-muted-foreground">Generating...</span>
          </span>
        ) : item.title}
      </div>
      <button
        onClick={(e) => {
          e.stopPropagation()
          setShowMenu(!showMenu)
        }}
        className="p-0.5 opacity-0 group-hover:opacity-100 text-muted-foreground hover:text-foreground transition-all"
      >
        <MoreHorizontal className="h-3 w-3" />
      </button>

      {showMenu && (
        <>
          <div
            className="fixed inset-0 z-10"
            onClick={(e) => {
              e.stopPropagation()
              setShowMenu(false)
            }}
          />
          <div className="absolute right-0 top-full mt-1 z-20 bg-card border border-border shadow-md rounded py-1 min-w-[120px]">
            <button
              onClick={(e) => {
                e.stopPropagation()
                handleStartRename()
              }}
              className="w-full flex items-center gap-2 px-3 py-1.5 text-xs text-foreground hover:bg-muted transition-colors"
            >
              <Pencil className="h-3 w-3" />
              Rename
            </button>
            <button
              onClick={(e) => {
                e.stopPropagation()
                handleGenerateTitle()
              }}
              className="w-full flex items-center gap-2 px-3 py-1.5 text-xs text-foreground hover:bg-muted transition-colors"
            >
              <RefreshCw className="h-3 w-3" />
              Generate Title
            </button>
            <button
              onClick={(e) => {
                e.stopPropagation()
                setShowMenu(false)
                onDelete()
              }}
              className="w-full flex items-center gap-2 px-3 py-1.5 text-xs text-destructive hover:bg-muted transition-colors"
            >
              <Trash2 className="h-3 w-3" />
              Delete
            </button>
          </div>
        </>
      )}
    </div>
  )
}
