import { useState, useEffect } from 'react'
import { useLocation, Link, useNavigate } from 'react-router-dom'
import { useEnv } from '@/contexts/EnvContext'
import {
  PanelLeftClose,
  PanelLeftOpen,
  MessageSquare,
  Database,
  Globe,
  Activity,
  ArrowUpCircle,
  Server,
  Link2,
  MapPin,
  Users,
  Building2,
  Landmark,
  Radio,
  Clock,
  Search,
  Route,
  Map,
  Network,
  Shield,
  Wrench,
  AlertTriangle,
  Gauge,
  BarChart3,
  Zap,
  Sun,
  Moon,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { useTheme } from '@/hooks/use-theme'
import { useVersionCheck } from '@/hooks/use-version-check'
import { UserPopover } from './auth/UserPopover'

export function Sidebar() {
  const location = useLocation()
  const navigate = useNavigate()
  const { features } = useEnv()
  const hasNeo4j = features.neo4j !== false
  const hasSolana = features.solana !== false
  const { resolvedTheme, setTheme } = useTheme()
  const { updateAvailable, reload } = useVersionCheck()

  // Route detection
  const isStatusRoute = location.pathname.startsWith('/status')
  const isTimelineRoute = location.pathname === '/timeline'
  const isOutagesRoute = location.pathname === '/outages'
  const isChatRoute = location.pathname.startsWith('/chat')
  const isChatSessions = location.pathname === '/chat/sessions'
  const isQueryRoute = location.pathname.startsWith('/query')
  const isQuerySessions = location.pathname === '/query/sessions'
  const isTopologyRoute = location.pathname === '/topology' || location.pathname.startsWith('/topology/')
  const isPerformanceRoute = location.pathname.startsWith('/performance')
  const isTrafficRoute = location.pathname.startsWith('/traffic')
  const isDZRoute = location.pathname.startsWith('/dz/')
  const isSolanaRoute = location.pathname.startsWith('/solana/')

  // Topology sub-routes
  const isTopologyMap = location.pathname === '/topology/map'
  const isTopologyGraph = location.pathname === '/topology/graph'
  const isTopologyGlobe = location.pathname === '/topology/globe'
  const isTopologyPathCalculator = location.pathname === '/topology/path-calculator'
  const isTopologyRedundancy = location.pathname === '/topology/redundancy'
  const isTopologyMetroConnectivity = location.pathname === '/topology/metro-connectivity'
  const isTopologyMaintenance = location.pathname === '/topology/maintenance'
  const isTopologyTool = isTopologyPathCalculator || isTopologyRedundancy || isTopologyMetroConnectivity || isTopologyMaintenance

  // Performance sub-routes
  const isPerformanceDzVsInternet = location.pathname === '/performance/dz-vs-internet'
  const isPerformancePathLatency = location.pathname === '/performance/path-latency'

  // Traffic sub-routes
  const isTrafficDashboard = location.pathname === '/traffic/overview'
  const isTrafficInterfaces = location.pathname === '/traffic/interfaces'

  // Entity routes
  const isDevicesRoute = location.pathname === '/dz/devices'
  const isLinksRoute = location.pathname === '/dz/links'
  const isMetrosRoute = location.pathname === '/dz/metros'
  const isContributorsRoute = location.pathname === '/dz/contributors'
  const isUsersRoute = location.pathname === '/dz/users'
  const isValidatorsRoute = location.pathname === '/solana/validators'
  const isGossipNodesRoute = location.pathname === '/solana/gossip-nodes'

  const [isCollapsed, setIsCollapsed] = useState(() => {
    const userPref = localStorage.getItem('sidebar-user-collapsed')
    if (userPref !== null) return userPref === 'true'
    return typeof window !== 'undefined' && window.innerWidth < 1024
  })
  const [userCollapsed, setUserCollapsed] = useState<boolean | null>(() => {
    const saved = localStorage.getItem('sidebar-user-collapsed')
    return saved !== null ? saved === 'true' : null
  })

  // Auto-collapse/expand based on screen size and user preference
  useEffect(() => {
    if (userCollapsed !== null) {
      setIsCollapsed(userCollapsed)
      return
    }
    const isSmall = typeof window !== 'undefined' && window.innerWidth < 1024
    setIsCollapsed(isSmall)
  }, [userCollapsed])

  useEffect(() => {
    const checkWidth = () => {
      const isSmall = window.innerWidth < 1024
      if (isSmall) {
        setIsCollapsed(true)
      } else if (userCollapsed === null) {
        setIsCollapsed(false)
      } else {
        setIsCollapsed(userCollapsed)
      }
    }
    window.addEventListener('resize', checkWidth)
    return () => window.removeEventListener('resize', checkWidth)
  }, [userCollapsed])

  useEffect(() => {
    localStorage.setItem('sidebar-collapsed', String(isCollapsed))
  }, [isCollapsed])

  const handleSetCollapsed = (collapsed: boolean) => {
    setIsCollapsed(collapsed)
    setUserCollapsed(collapsed)
    localStorage.setItem('sidebar-user-collapsed', String(collapsed))
  }

  // Active state classes for nav items
  const navItemClass = (isActive: boolean) => cn(
    'w-full flex items-center gap-2 px-3 py-1.5 text-sm rounded-r transition-colors border-l-2',
    isActive
      ? 'border-accent bg-[var(--sidebar-active)] text-foreground font-medium'
      : 'border-transparent text-muted-foreground hover:text-foreground hover:bg-[var(--sidebar-active)]'
  )

  // Expanded parent nav item (e.g., "Topology" when on /topology/*) — subtle, no background
  const navItemExpandedClass = cn(
    'w-full flex items-center gap-2 px-3 py-1.5 text-sm rounded-r transition-colors border-l-2',
    'border-transparent text-foreground font-medium hover:bg-[var(--sidebar-active)]'
  )

  // Sub-nav item class (indented, with optional deeper nesting)
  const subNavItemClass = (isActive: boolean, nested?: boolean) => cn(
    'w-full flex items-center gap-2 pr-3 py-1.5 text-sm rounded-r transition-colors border-l-2',
    nested ? 'pl-12' : 'pl-8',
    isActive
      ? 'border-accent bg-[var(--sidebar-active)] text-foreground font-medium'
      : 'border-transparent text-muted-foreground hover:text-foreground hover:bg-[var(--sidebar-active)]'
  )

  const collapsedIconClass = (isActive: boolean) => cn(
    'p-2 rounded transition-colors',
    isActive
      ? 'bg-[oklch(25%_.04_250)] text-white hover:bg-[oklch(30%_.05_250)]'
      : 'text-muted-foreground hover:text-foreground hover:bg-[var(--sidebar-active)]'
  )

  // ─── Collapsed sidebar ───────────────────────────────────────────────
  if (isCollapsed) {
    return (
      <div className="w-12 border-r bg-[var(--sidebar)] flex flex-col items-center relative z-10">
        {/* Logo icon - matches expanded header height */}
        <div className="w-full h-12 flex items-center justify-center border-b border-border/50 shrink-0">
          <button
            onClick={() => handleSetCollapsed(false)}
            className="group relative"
            title="Expand sidebar"
          >
            <img src={resolvedTheme === 'dark' ? '/logoDarkSm.svg' : '/logoLightSm.svg'} alt="Data" className="h-6 group-hover:opacity-0 transition-opacity" />
            <PanelLeftOpen className="h-5 w-5 absolute inset-0 m-auto opacity-0 group-hover:opacity-100 transition-opacity text-muted-foreground" />
          </button>
        </div>

        <div className="flex-1 flex flex-col items-center gap-1 pt-4 overflow-y-auto min-h-0">
          {/* Search */}
          <button
            onClick={() => window.dispatchEvent(new CustomEvent('open-search'))}
            className="p-2 rounded transition-colors text-muted-foreground hover:text-foreground hover:bg-[var(--sidebar-active)]"
            title="Search (⌘K)"
          >
            <Search className="h-4 w-4" />
          </button>

          {/* Main nav */}
          <Link to="/status" className={collapsedIconClass(isStatusRoute)} title="Status">
            <Activity className="h-4 w-4" />
          </Link>
          <Link to="/topology/map" className={collapsedIconClass(isTopologyRoute)} title="Topology">
            <Globe className="h-4 w-4" />
          </Link>
          <Link to="/traffic/overview" className={collapsedIconClass(isTrafficRoute)} title="Traffic">
            <Network className="h-4 w-4" />
          </Link>
          <Link to="/performance/dz-vs-internet" className={collapsedIconClass(isPerformanceRoute)} title="Performance">
            <Gauge className="h-4 w-4" />
          </Link>
          <Link to="/timeline" className={collapsedIconClass(isTimelineRoute)} title="Timeline">
            <Clock className="h-4 w-4" />
          </Link>
          <Link to="/outages" className={collapsedIconClass(isOutagesRoute)} title="Outages">
            <AlertTriangle className="h-4 w-4" />
          </Link>
          <button
            onClick={(e) => {
              if (e.metaKey || e.ctrlKey) {
                window.open('/chat', '_blank')
              } else if (location.pathname === '/chat') {
                window.dispatchEvent(new CustomEvent('refresh-chat-suggestions'))
              } else {
                navigate('/chat')
              }
            }}
            className={collapsedIconClass(isChatRoute)}
            title="Chat"
          >
            <MessageSquare className="h-4 w-4" />
          </button>
          <button
            onClick={(e) => {
              if (e.metaKey || e.ctrlKey) {
                window.open('/query', '_blank')
              } else {
                navigate('/query')
              }
            }}
            className={collapsedIconClass(isQueryRoute)}
            title="Query"
          >
            <Database className="h-4 w-4" />
          </button>

          {/* Divider */}
          <div className="w-6 border-t border-border/50 my-2" />

          {/* Entity sections */}
          <Link to="/dz/devices" className={collapsedIconClass(isDZRoute)} title="DoubleZero">
            <Server className="h-4 w-4" />
          </Link>
          {hasSolana && (
            <Link to="/solana/validators" className={collapsedIconClass(isSolanaRoute)} title="Solana">
              <Landmark className="h-4 w-4" />
            </Link>
          )}
        </div>

        {/* Footer */}
        <div className="flex flex-col items-center gap-1 mb-3 shrink-0">
          {updateAvailable && (
            <button
              onClick={reload}
              className="p-2 text-blue-500 hover:text-blue-400 transition-colors animate-pulse"
              title="Click to reload and get the latest version"
            >
              <ArrowUpCircle className="h-4 w-4" />
            </button>
          )}
          <button
            onClick={() => setTheme(resolvedTheme === 'dark' ? 'light' : 'dark')}
            className="p-2 text-muted-foreground hover:text-foreground transition-colors"
            title={`Switch to ${resolvedTheme === 'dark' ? 'light' : 'dark'} mode`}
          >
            {resolvedTheme === 'dark' ? <Moon className="h-4 w-4" /> : <Sun className="h-4 w-4" />}
          </button>
          <UserPopover collapsed />
          <button
            onClick={() => handleSetCollapsed(false)}
            className="p-2 text-muted-foreground hover:text-foreground transition-colors"
            title="Expand sidebar"
          >
            <PanelLeftOpen className="h-4 w-4" />
          </button>
        </div>
      </div>
    )
  }

  // ─── Expanded sidebar ────────────────────────────────────────────────
  return (
    <div className="w-64 border-r bg-[var(--sidebar)] flex flex-col relative z-10">
      {/* Header with logo and collapse */}
      <div className="px-3 h-12 flex items-center justify-between border-b border-border/50 shrink-0">
        <Link
          to="/"
          className="flex items-center gap-2 hover:opacity-80 transition-opacity cursor-pointer"
        >
          <img src={resolvedTheme === 'dark' ? '/logoDark.svg' : '/logoLight.svg'} alt="DoubleZero" className="h-6" />
          <span className="wordmark text-lg">Data</span>
        </Link>
        <button
          onClick={() => handleSetCollapsed(true)}
          className="p-1 text-muted-foreground hover:text-foreground transition-colors"
          title="Collapse sidebar"
        >
          <PanelLeftClose className="h-4 w-4 translate-y-0.5" />
        </button>
      </div>

      {/* Scrollable content area */}
      <div className="flex-1 flex flex-col overflow-y-auto min-h-0">
        {/* Main nav - no section label */}
        <div className="px-3 pt-4">
          <div className="space-y-1">
            <Link to="/status" className={navItemClass(isStatusRoute)}>
              <Activity className="h-4 w-4" />
              Status
            </Link>

            {/* Topology with inline sub-items */}
            <Link to="/topology/map" className={isTopologyRoute ? navItemExpandedClass : navItemClass(false)}>
              <Globe className="h-4 w-4" />
              Topology
            </Link>
            {isTopologyRoute && (
              <>
                <Link to="/topology/map" className={subNavItemClass(isTopologyMap)}>
                  <Map className="h-4 w-4" />
                  Map
                </Link>
                <Link to="/topology/globe" className={subNavItemClass(isTopologyGlobe)}>
                  <Globe className="h-4 w-4" />
                  Globe
                </Link>
                {hasNeo4j && (
                  <>
                    <Link to="/topology/graph" className={subNavItemClass(isTopologyGraph)}>
                      <Network className="h-4 w-4" />
                      Graph
                    </Link>
                    <Link to="/topology/path-calculator" className={subNavItemClass(isTopologyTool)}>
                      <Wrench className="h-4 w-4" />
                      Tools
                    </Link>
                    {isTopologyTool && (
                      <>
                        <Link to="/topology/path-calculator" className={subNavItemClass(isTopologyPathCalculator, true)}>
                          <Route className="h-4 w-4" />
                          Path Calculator
                        </Link>
                        <Link to="/topology/redundancy" className={subNavItemClass(isTopologyRedundancy, true)}>
                          <Shield className="h-4 w-4" />
                          Redundancy
                        </Link>
                        <Link to="/topology/metro-connectivity" className={subNavItemClass(isTopologyMetroConnectivity, true)}>
                          <Network className="h-4 w-4" />
                          Metro Connectivity
                        </Link>
                        <Link to="/topology/maintenance" className={subNavItemClass(isTopologyMaintenance, true)}>
                          <Wrench className="h-4 w-4" />
                          Maintenance
                        </Link>
                      </>
                    )}
                  </>
                )}
              </>
            )}

            {/* Traffic with inline sub-items */}
            <Link to="/traffic/overview" className={isTrafficRoute ? navItemExpandedClass : navItemClass(false)}>
              <Network className="h-4 w-4" />
              Traffic
            </Link>
            {isTrafficRoute && (
              <>
                <Link to="/traffic/overview" className={subNavItemClass(isTrafficDashboard)}>
                  <BarChart3 className="h-4 w-4" />
                  Overview
                </Link>
                <Link to="/traffic/interfaces" className={subNavItemClass(isTrafficInterfaces)}>
                  <Network className="h-4 w-4" />
                  Interfaces
                </Link>
              </>
            )}

            {/* Performance with inline sub-items */}
            <Link to="/performance/dz-vs-internet" className={isPerformanceRoute ? navItemExpandedClass : navItemClass(false)}>
              <Gauge className="h-4 w-4" />
              Performance
            </Link>
            {isPerformanceRoute && (
              <>
                <Link to="/performance/dz-vs-internet" className={subNavItemClass(isPerformanceDzVsInternet)}>
                  <Zap className="h-4 w-4" />
                  DZ vs Internet
                </Link>
                {hasNeo4j && (
                  <Link to="/performance/path-latency" className={subNavItemClass(isPerformancePathLatency)}>
                    <Route className="h-4 w-4" />
                    Path Latency
                  </Link>
                )}
              </>
            )}
            <Link to="/timeline" className={navItemClass(isTimelineRoute)}>
              <Clock className="h-4 w-4" />
              Timeline
            </Link>
            <Link to="/outages" className={navItemClass(isOutagesRoute)}>
              <AlertTriangle className="h-4 w-4" />
              Outages
            </Link>

            {/* Chat with inline sub-items */}
            <button
              onClick={(e) => {
                if (e.metaKey || e.ctrlKey) {
                  window.open('/chat', '_blank')
                } else if (location.pathname === '/chat') {
                  window.dispatchEvent(new CustomEvent('refresh-chat-suggestions'))
                } else {
                  navigate('/chat')
                }
              }}
              className={isChatRoute ? navItemExpandedClass : navItemClass(false)}
            >
              <MessageSquare className="h-4 w-4" />
              Chat
            </button>
            {isChatRoute && (
              <>
                <button
                  onClick={(e) => {
                    if (e.metaKey || e.ctrlKey) {
                      window.open('/chat', '_blank')
                    } else {
                      navigate('/chat')
                    }
                  }}
                  className={subNavItemClass(!isChatSessions && (location.pathname === '/chat' || !!location.pathname.match(/^\/chat\/[^/]+$/)))}
                >
                  New chat
                </button>
                <Link to="/chat/sessions" className={subNavItemClass(isChatSessions)}>
                  History
                </Link>
              </>
            )}

            {/* Query with inline sub-items */}
            <button
              onClick={(e) => {
                if (e.metaKey || e.ctrlKey) {
                  window.open('/query', '_blank')
                } else {
                  navigate('/query')
                }
              }}
              className={isQueryRoute ? navItemExpandedClass : navItemClass(false)}
            >
              <Database className="h-4 w-4" />
              Query
            </button>
            {isQueryRoute && (
              <>
                <button
                  onClick={(e) => {
                    if (e.metaKey || e.ctrlKey) {
                      window.open('/query', '_blank')
                    } else {
                      navigate('/query')
                    }
                  }}
                  className={subNavItemClass(!isQuerySessions && (location.pathname === '/query' || !!location.pathname.match(/^\/query\/[^/]+$/)))}
                >
                  New query
                </button>
                <Link to="/query/sessions" className={subNavItemClass(isQuerySessions)}>
                  History
                </Link>
              </>
            )}

            <button
              onClick={() => window.dispatchEvent(new CustomEvent('open-search'))}
              className="w-full flex items-center gap-2 px-3 py-1.5 text-sm rounded transition-colors text-muted-foreground hover:text-foreground hover:bg-[var(--sidebar-active)]"
            >
              <Search className="h-4 w-4" />
              <span className="flex-1 text-left">Search</span>
              <kbd className="text-xs text-muted-foreground bg-muted px-1.5 py-0.5 rounded">⌘K</kbd>
            </button>
          </div>
        </div>

        {/* DoubleZero section - always visible */}
        <div className="px-3 pt-4">
          <div className="px-3 mb-2">
            <span className="text-[11px] font-normal text-muted-foreground/70 uppercase tracking-widest">DoubleZero</span>
          </div>
          <div className="space-y-1">
            <Link to="/dz/devices" className={navItemClass(isDevicesRoute)}>
              <Server className="h-4 w-4" />
              Devices
            </Link>
            <Link to="/dz/links" className={navItemClass(isLinksRoute)}>
              <Link2 className="h-4 w-4" />
              Links
            </Link>
            <Link to="/dz/metros" className={navItemClass(isMetrosRoute)}>
              <MapPin className="h-4 w-4" />
              Metros
            </Link>
            <Link to="/dz/contributors" className={navItemClass(isContributorsRoute)}>
              <Building2 className="h-4 w-4" />
              Contributors
            </Link>
            <Link to="/dz/users" className={navItemClass(isUsersRoute)}>
              <Users className="h-4 w-4" />
              Users
            </Link>
          </div>
        </div>

        {/* Solana section - always visible when feature-gated */}
        {hasSolana && (
          <div className="px-3 pt-4">
            <div className="px-3 mb-2">
              <span className="text-[11px] font-normal text-muted-foreground/70 uppercase tracking-widest">Solana</span>
            </div>
            <div className="space-y-1">
              <Link to="/solana/validators" className={navItemClass(isValidatorsRoute)}>
                <Landmark className="h-4 w-4" />
                Validators
              </Link>
              <Link to="/solana/gossip-nodes" className={navItemClass(isGossipNodesRoute)}>
                <Radio className="h-4 w-4" />
                Gossip Nodes
              </Link>
            </div>
          </div>
        )}

        {/* Spacer */}
        <div className="flex-1" />
      </div>

      {/* Footer */}
      <div className="px-3 py-3 border-t border-border/50 space-y-2 shrink-0">
        {updateAvailable && (
          <button
            onClick={reload}
            className="w-full flex items-center gap-2 px-3 py-2 text-sm text-blue-500 hover:text-blue-400 bg-blue-500/10 hover:bg-blue-500/20 rounded transition-colors"
            title="Click to reload and get the latest version"
          >
            <ArrowUpCircle className="h-4 w-4" />
            Update available
          </button>
        )}
        <UserPopover />
        <div className="flex rounded-lg border border-border overflow-hidden bg-card">
          <button
            onClick={() => setTheme('light')}
            className={cn(
              'flex-1 flex items-center justify-center gap-1.5 px-3 py-1.5 text-xs transition-colors',
              resolvedTheme === 'light'
                ? 'bg-muted text-foreground font-medium'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
            )}
          >
            <Sun className="h-3.5 w-3.5" />
            Light
          </button>
          <button
            onClick={() => setTheme('dark')}
            className={cn(
              'flex-1 flex items-center justify-center gap-1.5 px-3 py-1.5 text-xs border-l border-border transition-colors',
              resolvedTheme === 'dark'
                ? 'bg-muted text-foreground font-medium'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
            )}
          >
            <Moon className="h-3.5 w-3.5" />
            Dark
          </button>
        </div>
      </div>
    </div>
  )
}
