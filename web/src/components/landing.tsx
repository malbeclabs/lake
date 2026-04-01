import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import {
  Activity,
  Globe,
  Network,
  Gauge,
  Route,
  ShieldAlert,
  MessageSquare,
  Cable,
  ArrowRight,
  ArrowRightLeft,
} from 'lucide-react'
import { fetchStats } from '@/lib/api'
import { StatCard } from '@/components/stat-card'
import { useTheme } from '@/hooks/use-theme'
import { useDocumentTitle } from '@/hooks/use-document-title'

const pages = [
  {
    title: 'Status',
    description: 'Network health and device status',
    icon: Activity,
    href: '/status',
    color: 'text-emerald-600 dark:text-emerald-400',
  },
  {
    title: 'Topology',
    description: 'Interactive map of the network',
    icon: Globe,
    href: '/topology/map',
    color: 'text-blue-600 dark:text-blue-400',
  },
  {
    title: 'Traffic',
    description: 'Bandwidth and interface utilization',
    icon: Network,
    href: '/traffic/overview',
    color: 'text-teal-600 dark:text-teal-400',
  },
  {
    title: 'Link Latency',
    description: 'Latency across network links',
    icon: Gauge,
    href: '/performance/link-latency',
    color: 'text-violet-600 dark:text-violet-400',
  },
  {
    title: 'Path Latency',
    description: 'End-to-end path performance',
    icon: Route,
    href: '/performance/path-latency',
    color: 'text-amber-600 dark:text-amber-400',
  },
  {
    title: 'Incidents',
    description: 'Active link and device incidents',
    icon: ShieldAlert,
    href: '/incidents/links',
    color: 'text-red-600 dark:text-red-400',
  },
  {
    title: 'DZ vs Internet',
    description: 'DZ vs public internet performance',
    icon: ArrowRightLeft,
    href: '/performance/dz-vs-internet',
    color: 'text-orange-600 dark:text-orange-400',
  },
  {
    title: 'Chat',
    description: 'Ask questions about the network',
    icon: MessageSquare,
    href: '/chat',
    color: 'text-sky-600 dark:text-sky-400',
  },
]

export function Landing() {
  useDocumentTitle('Explore')
  const { resolvedTheme } = useTheme()

  const { data: stats } = useQuery({
    queryKey: ['stats'],
    queryFn: fetchStats,
    refetchInterval: 15_000,
    staleTime: 10_000,
  })

  return (
    <div className="flex-1 flex flex-col items-center justify-start px-8 pt-12 pb-4 overflow-auto">
      <div className="flex-1 flex flex-col items-center w-full">
      {/* Header */}
      <div className="text-center mb-8">
        <img
          src={resolvedTheme === 'dark' ? '/logoDark.svg' : '/logoLight.svg'}
          alt="DoubleZero"
          className="h-8 mx-auto mb-3"
        />
        <p className="text-muted-foreground">
          Real-time insights into the DoubleZero network
        </p>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-2 sm:grid-cols-5 gap-x-10 gap-y-8 mb-8 max-w-5xl w-full">
        {/* Row 1: Network Infrastructure */}
        <StatCard
          label="Contributors"
          value={stats?.contributors}
          format="number"
          href="/dz/contributors"
        />
        <StatCard
          label="Metros"
          value={stats?.metros}
          format="number"
          href="/dz/metros"
        />
        <StatCard
          label="Devices"
          value={stats?.devices}
          format="number"
          href="/dz/devices"
        />
        <StatCard
          label="Links"
          value={stats?.links}
          format="number"
          href="/dz/links"
        />
        <StatCard
          label="Users"
          value={stats?.users}
          format="number"
          href="/dz/users"
        />
        {/* Row 2: Solana + Traffic */}
        <StatCard
          label="Validators on DZ"
          value={stats?.validators_on_dz}
          format="number"
          href="/solana/validators"
        />
        <StatCard
          label="SOL Connected"
          value={stats?.total_stake_sol}
          format="stake"
        />
        <StatCard
          label="Stake Share"
          value={stats?.stake_share_pct}
          format="percent"
        />
        <StatCard
          label="Capacity"
          value={stats?.bandwidth_bps}
          format="bandwidth"
        />
        <StatCard
          label="User Inbound"
          value={stats?.user_inbound_bps}
          format="bandwidth"
          decimals={0}
        />
      </div>

      {/* Navigation Cards */}
      <div className="grid grid-cols-2 sm:grid-cols-3 gap-4 max-w-5xl w-full">
        {pages.map((page) => (
          <Link
            key={page.href}
            to={page.href}
            className="group rounded-xl border border-border bg-secondary/50 p-5 transition-colors hover:bg-secondary hover:border-muted-foreground/30"
          >
            <div className="flex items-center justify-between mb-3">
              <page.icon className={`h-5 w-5 ${page.color}`} />
              <ArrowRight className="h-4 w-4 text-muted-foreground/0 group-hover:text-muted-foreground transition-colors" />
            </div>
            <div className="font-medium text-sm mb-1">{page.title}</div>
            <div className="text-xs text-muted-foreground">{page.description}</div>
          </Link>
        ))}
        <Link
          to="/docs/mcp"
          className="group rounded-xl border border-border bg-secondary/50 p-5 transition-colors hover:bg-secondary hover:border-muted-foreground/30"
        >
          <div className="flex items-center justify-between mb-3">
            <Cable className="h-5 w-5 text-indigo-600 dark:text-indigo-400" />
            <ArrowRight className="h-4 w-4 text-muted-foreground/0 group-hover:text-muted-foreground transition-colors" />
          </div>
          <div className="font-medium text-sm mb-1">Connect Your Own AI</div>
          <div className="text-xs text-muted-foreground">Query DoubleZero data via MCP</div>
        </Link>
      </div>
      </div>
    </div>
  )
}
