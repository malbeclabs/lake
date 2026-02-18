import { NavLink } from 'react-router-dom'
import { LayoutDashboard, FileText, Settings, Sun, Moon } from 'lucide-react'
import { cn } from '@/lib/utils'
import { useTheme } from '@/hooks/use-theme'

const NAV_ITEMS = [
  { to: '/', label: 'Dashboard', icon: LayoutDashboard, exact: true },
  { to: '/logs', label: 'Logs', icon: FileText, exact: false },
  { to: '/settings', label: 'Settings', icon: Settings, exact: false },
]

export function Sidebar() {
  const { theme, setTheme } = useTheme()
  const isDark = theme === 'dark'

  return (
    <aside
      className="flex flex-col h-screen w-64 shrink-0 sticky top-0"
      style={{ background: 'var(--sidebar)', borderRight: '1px solid var(--border)' }}
    >
      {/* Logo */}
      <div
        className="flex items-center h-12 px-3 shrink-0"
        style={{ borderBottom: '1px solid color-mix(in oklch, var(--border) 50%, transparent)' }}
      >
        <img
          src={isDark ? '/logoDark.svg' : '/logoLight.svg'}
          alt="DoubleZero"
          className="h-6 w-auto"
        />
      </div>

      {/* Nav */}
      <nav className="flex-1 flex flex-col gap-0.5 p-2 overflow-y-auto">
        <div className="px-3 py-1.5">
          <span className="text-xs font-medium" style={{ color: 'var(--muted-foreground)' }}>
            Control Center
          </span>
        </div>
        {NAV_ITEMS.map(({ to, label, icon: Icon, exact }) => (
          <NavLink
            key={to}
            to={to}
            end={exact}
            className={({ isActive }) =>
              cn(
                'w-full flex items-center gap-2 px-3 py-1.5 text-sm rounded-md transition-colors',
                isActive
                  ? 'bg-[var(--sidebar-active)] text-foreground font-medium'
                  : 'text-muted-foreground hover:text-foreground hover:bg-[var(--sidebar-active)]'
              )
            }
          >
            <Icon className="h-4 w-4 shrink-0" />
            {label}
          </NavLink>
        ))}
      </nav>

      {/* Footer — theme toggle */}
      <div
        className="shrink-0 p-3"
        style={{ borderTop: '1px solid color-mix(in oklch, var(--border) 50%, transparent)' }}
      >
        <div className="flex rounded-md border border-border overflow-hidden bg-card">
          <button
            onClick={() => setTheme('light')}
            className={cn(
              'flex-1 flex items-center justify-center gap-1.5 px-3 py-1.5 text-xs transition-colors',
              !isDark ? 'bg-muted text-foreground font-medium' : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
            )}
            aria-label="Light mode"
          >
            <Sun className="h-3.5 w-3.5" />
            Light
          </button>
          <button
            onClick={() => setTheme('dark')}
            className={cn(
              'flex-1 flex items-center justify-center gap-1.5 px-3 py-1.5 text-xs border-l border-border transition-colors',
              isDark ? 'bg-muted text-foreground font-medium' : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
            )}
            aria-label="Dark mode"
          >
            <Moon className="h-3.5 w-3.5" />
            Dark
          </button>
        </div>
      </div>
    </aside>
  )
}
