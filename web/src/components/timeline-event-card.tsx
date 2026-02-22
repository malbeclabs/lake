import { useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import {
  AlertTriangle,
  AlertCircle,
  Info,
  ChevronDown,
  ChevronUp,
  CheckCircle2,
  Plus,
  Trash2,
  Pencil,
  Play,
  Square,
  RotateCcw,
  LogIn,
  LogOut,
  TrendingUp,
  TrendingDown,
  Clock,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import type {
  TimelineEvent,
  EntityChangeDetails,
  PacketLossEventDetails,
  InterfaceEventDetails,
  GroupedInterfaceDetails,
  ValidatorEventDetails,
  FieldChange,
  DeviceEntity,
  LinkEntity,
  MetroEntity,
  ContributorEntity,
  UserEntity,
} from '@/lib/api'

const severityDotColors: Record<string, string> = {
  info: 'border-blue-500',
  warning: 'border-amber-500 bg-amber-500',
  critical: 'border-red-500 bg-red-500',
  success: 'border-green-500',
}

const severityIcons: Record<string, typeof Info> = {
  info: Info,
  warning: AlertTriangle,
  critical: AlertCircle,
  success: CheckCircle2,
}

// Get severity color class based on count thresholds
function getSeverityColor(value: number, thresholds: { low: number; medium: number; high: number }): string {
  if (value >= thresholds.high) return 'text-red-600 font-semibold'
  if (value >= thresholds.medium) return 'text-orange-500 font-medium'
  if (value >= thresholds.low) return 'text-amber-500'
  return 'text-muted-foreground'
}

function getLossColor(pct: number): string {
  if (pct >= 5) return 'text-red-600 font-semibold'
  if (pct >= 1) return 'text-orange-500 font-medium'
  if (pct >= 0.1) return 'text-amber-500'
  return 'text-muted-foreground'
}

const errorThresholds = { low: 10, medium: 100, high: 1000 }
const discardThresholds = { low: 100, medium: 1000, high: 10000 }
const carrierThresholds = { low: 1, medium: 5, high: 10 }

import { formatTimeAgo } from './timeline-constants'

function formatValue(value: unknown, field: string): string {
  if (value === null || value === undefined) return '—'
  if (typeof value === 'number') {
    if (field.includes('bandwidth') || field.includes('bps')) {
      if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)} Gbps`
      if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)} Mbps`
      return `${value} bps`
    }
    if (field.includes('rtt') || field.includes('jitter') || field.includes('delay')) {
      if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)} ms`
      if (value >= 1_000) return `${(value / 1_000).toFixed(2)} µs`
      return `${value} ns`
    }
    return value.toLocaleString()
  }
  if (typeof value === 'string') {
    if (value.length > 20) return value.slice(0, 6) + '...' + value.slice(-3)
    return value
  }
  return String(value)
}

const fieldLabels: Record<string, string> = {
  status: 'Status',
  device_type: 'Type',
  public_ip: 'Public IP',
  contributor: 'Contributor',
  metro: 'Metro',
  max_users: 'Max Users',
  link_type: 'Type',
  tunnel_net: 'Tunnel Net',
  side_a: 'Side A',
  side_z: 'Side Z',
  committed_rtt: 'Committed RTT',
  committed_jitter: 'Committed Jitter',
  bandwidth: 'Bandwidth',
  name: 'Name',
  longitude: 'Longitude',
  latitude: 'Latitude',
  code: 'Code',
  kind: 'Kind',
  client_ip: 'Client IP',
  dz_ip: 'DZ IP',
  device: 'Device',
  tunnel_id: 'Tunnel ID',
}

function truncatePubkey(value: string): string {
  if (value.length > 20) return value.slice(0, 6) + '...' + value.slice(-3)
  return value
}

function ChangeSummary({ changes }: { changes?: FieldChange[] }) {
  if (!changes || changes.length === 0) return null
  return (
    <div className="mt-2 flex flex-wrap gap-2">
      {changes.map((change, idx) => (
        <div
          key={idx}
          className="inline-flex items-center gap-1 text-xs bg-muted/50 px-2 py-1 rounded border border-border"
        >
          <span className="text-muted-foreground">{fieldLabels[change.field] || change.field}:</span>
          <span className="text-amber-600 line-through">{formatValue(change.old_value, change.field)}</span>
          <span className="text-muted-foreground">→</span>
          <span className="text-green-600 font-medium">{formatValue(change.new_value, change.field)}</span>
        </div>
      ))}
    </div>
  )
}

function FilterButton({ children, value, field, className }: { children: React.ReactNode; value: string; field?: string; className?: string }) {
  const [searchParams, setSearchParams] = useSearchParams()

  const filterValue = field ? `${field}:${value}` : value

  const handleClick = (e: React.MouseEvent) => {
    e.preventDefault()
    e.stopPropagation()
    const currentSearch = searchParams.get('search') || ''
    const currentFilters = currentSearch ? currentSearch.split(',').map(f => f.trim()).filter(Boolean) : []
    if (!currentFilters.includes(filterValue)) {
      currentFilters.push(filterValue)
    }
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      next.set('search', currentFilters.join(','))
      return next
    })
  }

  return (
    <button
      onClick={handleClick}
      className={cn("hover:underline cursor-pointer", className)}
      title={`Filter by ${field ? field + ': ' : ''}"${value}"`}
    >
      {children}
    </button>
  )
}

function EntityLink({ to, children }: { to: string, children: React.ReactNode }) {
  return (
    <Link to={to} className="font-medium text-foreground hover:underline">
      {children}
    </Link>
  )
}

function EntityDetailsView({ entity, entityType }: { entity: DeviceEntity | LinkEntity | MetroEntity | ContributorEntity | UserEntity, entityType: string }) {
  if (entityType === 'device') {
    const d = entity as DeviceEntity
    return (
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-muted-foreground">
        <div>Type: <span className="font-medium text-foreground">{d.device_type}</span></div>
        <div>Status: <span className="font-medium text-foreground">{d.status}</span></div>
        <div>Public IP: <span className="font-mono text-foreground">{d.public_ip}</span></div>
        <div>Max Users: <span className="font-medium text-foreground">{d.max_users}</span></div>
        {d.metro_pk && d.metro_code && <div>Metro: <EntityLink to={`/dz/metros/${encodeURIComponent(d.metro_pk)}`}>{d.metro_code}</EntityLink></div>}
        {d.contributor_pk && d.contributor_code && <div>Contributor: <EntityLink to={`/dz/contributors/${encodeURIComponent(d.contributor_pk)}`}>{d.contributor_code}</EntityLink></div>}
      </div>
    )
  }
  if (entityType === 'link') {
    const d = entity as LinkEntity
    return (
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-muted-foreground">
        <div>Type: <span className="font-medium text-foreground">{d.link_type}</span></div>
        <div>Status: <span className="font-medium text-foreground">{d.status}</span></div>
        <div>Route: {d.side_a_pk ? <EntityLink to={`/dz/devices/${encodeURIComponent(d.side_a_pk)}`}>{d.side_a_code}</EntityLink> : <span className="font-medium text-foreground">{d.side_a_code}</span>} → {d.side_z_pk ? <EntityLink to={`/dz/devices/${encodeURIComponent(d.side_z_pk)}`}>{d.side_z_code}</EntityLink> : <span className="font-medium text-foreground">{d.side_z_code}</span>}</div>
        <div>Metros: {d.side_a_metro_pk ? <EntityLink to={`/dz/metros/${encodeURIComponent(d.side_a_metro_pk)}`}>{d.side_a_metro_code}</EntityLink> : <span className="font-medium text-foreground">{d.side_a_metro_code}</span>} → {d.side_z_metro_pk ? <EntityLink to={`/dz/metros/${encodeURIComponent(d.side_z_metro_pk)}`}>{d.side_z_metro_code}</EntityLink> : <span className="font-medium text-foreground">{d.side_z_metro_code}</span>}</div>
        {d.bandwidth_bps > 0 && <div>Bandwidth: <span className="font-medium text-foreground">{formatValue(d.bandwidth_bps, 'bandwidth')}</span></div>}
        {d.committed_rtt_ns > 0 && <div>Committed RTT: <span className="font-medium text-foreground">{formatValue(d.committed_rtt_ns, 'rtt')}</span></div>}
        {d.contributor_pk && d.contributor_code && <div>Contributor: <EntityLink to={`/dz/contributors/${encodeURIComponent(d.contributor_pk)}`}>{d.contributor_code}</EntityLink></div>}
        {d.tunnel_net && <div>Tunnel Net: <span className="font-mono text-foreground">{d.tunnel_net}</span></div>}
      </div>
    )
  }
  if (entityType === 'metro') {
    const d = entity as MetroEntity
    return (
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-muted-foreground">
        <div>Name: <span className="font-medium text-foreground">{d.name}</span></div>
        <div>Code: <span className="font-medium text-foreground">{d.code}</span></div>
        <div>Coordinates: <span className="font-mono text-foreground">{d.latitude.toFixed(4)}, {d.longitude.toFixed(4)}</span></div>
      </div>
    )
  }
  if (entityType === 'contributor') {
    const d = entity as ContributorEntity
    return (
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-muted-foreground">
        <div>Name: <span className="font-medium text-foreground">{d.name}</span></div>
        <div>Code: <span className="font-medium text-foreground">{d.code}</span></div>
      </div>
    )
  }
  if (entityType === 'user') {
    const d = entity as UserEntity
    return (
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-muted-foreground">
        <div className="col-span-2">Owner: <span className="font-mono text-foreground break-all">{d.owner_pubkey}</span></div>
        <div>Status: <span className="font-medium text-foreground">{d.status}</span></div>
        <div>Kind: <span className="font-medium text-foreground">{d.kind}</span></div>
        {d.device_pk && d.device_code && <div>Device: <EntityLink to={`/dz/devices/${encodeURIComponent(d.device_pk)}`}>{d.device_code}</EntityLink></div>}
        {d.client_ip && <div>Client IP: <span className="font-mono text-foreground">{d.client_ip}</span></div>}
        {d.dz_ip && <div>DZ IP: <span className="font-mono text-foreground">{d.dz_ip}</span></div>}
      </div>
    )
  }
  return null
}

function EventDetails({ event }: { event: TimelineEvent }) {
  const details = event.details
  if (!details) return null

  if (event.category === 'state_change' && 'change_type' in details) {
    const d = details as EntityChangeDetails
    return (
      <div className="text-xs text-muted-foreground mt-2 space-y-2">
        {d.entity && <EntityDetailsView entity={d.entity} entityType={event.entity_type} />}
      </div>
    )
  }

  if (event.event_type.startsWith('packet_loss') || event.event_type.startsWith('interface_')) {
    return null
  }

  if ((event.event_type.includes('validator') || event.event_type.includes('gossip_node')) && 'action' in details) {
    const d = details as ValidatorEventDetails
    const isValidator = d.kind === 'validator'
    return (
      <div className="text-xs text-muted-foreground mt-2 space-y-1">
        <div>Owner: <FilterButton value={d.owner_pubkey} field="user" className="font-mono break-all">{d.owner_pubkey}</FilterButton></div>
        {d.user_pk && <div>User: <FilterButton value={d.user_pk} field="user" className="font-mono break-all">{d.user_pk}</FilterButton></div>}
        {d.dz_ip && <div>DZ IP: <span className="font-mono">{d.dz_ip}</span></div>}
        {isValidator && d.vote_pubkey && <div>Vote Account: <FilterButton value={d.vote_pubkey} field="validator" className="font-mono break-all">{d.vote_pubkey}</FilterButton></div>}
        {!isValidator && d.node_pubkey && <div>Node Pubkey: <FilterButton value={d.node_pubkey} field="validator" className="font-mono break-all">{d.node_pubkey}</FilterButton></div>}
        {isValidator && d.stake_sol !== undefined && d.stake_sol > 0 && (
          <div className="flex flex-wrap gap-x-4 gap-y-1">
            <span>Stake: <span className="font-medium text-foreground">{d.stake_sol.toLocaleString(undefined, { maximumFractionDigits: 0 })} SOL</span></span>
            {d.stake_share_pct !== undefined && (
              <span>Network Share: <span className="font-medium text-foreground">{d.stake_share_pct.toFixed(3)}%</span></span>
            )}
          </div>
        )}
        {d.device_pk && d.device_code && <div>Device: <EntityLink to={`/dz/devices/${encodeURIComponent(d.device_pk)}`}>{d.device_code}</EntityLink></div>}
      </div>
    )
  }

  return null
}

// Date separator for the timeline
export function DateSeparator({ date }: { date: string }) {
  const d = new Date(date)
  const now = new Date()
  const yesterday = new Date(now)
  yesterday.setDate(yesterday.getDate() - 1)

  let label: string
  if (d.toDateString() === now.toDateString()) {
    label = 'Today'
  } else if (d.toDateString() === yesterday.toDateString()) {
    label = 'Yesterday'
  } else {
    label = d.toLocaleDateString(undefined, { month: 'long', day: 'numeric', year: 'numeric' })
  }

  return (
    <div className="sticky top-0 z-10 bg-background/95 backdrop-blur-sm py-2 -mx-1 px-1">
      <div className="flex items-center gap-3">
        <div className="h-px flex-1 bg-border" />
        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">{label}</span>
        <div className="h-px flex-1 bg-border" />
      </div>
    </div>
  )
}


export function TimelineEventCard({ event, isNew }: { event: TimelineEvent; isNew?: boolean }) {
  const [expanded, setExpanded] = useState(false)
  const SeverityIcon = severityIcons[event.severity]
  const hasDetails = event.details && Object.keys(event.details).length > 0
    && !event.event_type.startsWith('interface_')
    && !event.event_type.startsWith('packet_loss')

  const changeDetails = event.category === 'state_change' && event.details && 'change_type' in event.details
    ? event.details as EntityChangeDetails
    : undefined
  const changes = changeDetails?.changes
  const changeType = changeDetails?.change_type

  const validatorDetails = (event.event_type.includes('validator') || event.event_type.includes('gossip_node')) && event.details && 'action' in event.details
    ? event.details as ValidatorEventDetails
    : undefined

  const packetLossDetails = event.event_type.startsWith('packet_loss') && event.details && 'current_loss_pct' in event.details
    ? event.details as PacketLossEventDetails
    : undefined

  const singleInterfaceDetails = event.event_type.startsWith('interface_') && event.details && 'interface_name' in event.details
    ? event.details as InterfaceEventDetails
    : undefined
  const groupedInterfaceDetails = event.event_type.startsWith('interface_') && event.details && 'interfaces' in event.details
    ? event.details as GroupedInterfaceDetails
    : undefined

  const interfaceTotals = (() => {
    if (singleInterfaceDetails) {
      return {
        interfaces: [singleInterfaceDetails],
        carrierTransitions: singleInterfaceDetails.carrier_transitions || 0,
        errors: (singleInterfaceDetails.in_errors || 0) + (singleInterfaceDetails.out_errors || 0),
        discards: (singleInterfaceDetails.in_discards || 0) + (singleInterfaceDetails.out_discards || 0),
      }
    }
    if (groupedInterfaceDetails) {
      return {
        interfaces: groupedInterfaceDetails.interfaces,
        carrierTransitions: groupedInterfaceDetails.interfaces.reduce((sum, i) => sum + (i.carrier_transitions || 0), 0),
        errors: groupedInterfaceDetails.interfaces.reduce((sum, i) => sum + (i.in_errors || 0) + (i.out_errors || 0), 0),
        discards: groupedInterfaceDetails.interfaces.reduce((sum, i) => sum + (i.in_discards || 0) + (i.out_discards || 0), 0),
      }
    }
    return null
  })()

  const userEntity = event.entity_type === 'user' && changeDetails?.entity
    ? changeDetails.entity as UserEntity
    : undefined

  const isFilled = event.severity === 'critical' || event.severity === 'warning'

  return (
    <div className="flex gap-3 group">
      {/* Timeline dot + line connector */}
      <div className="flex flex-col items-center pt-1">
        <div className={cn(
          'w-2.5 h-2.5 rounded-full border-2 shrink-0',
          severityDotColors[event.severity],
          !isFilled && 'bg-background'
        )} />
      </div>

      {/* Card */}
      <div
        className={cn(
          'flex-1 min-w-0 border border-border/50 rounded-md p-3 transition-colors duration-150 hover:border-border',
          isNew && 'animate-slide-in shadow-[0_0_12px_rgba(59,130,246,0.3)] border-blue-400/40'
        )}
      >
        <div className="flex items-center gap-2 mb-1 flex-wrap">
          {(() => {
            const fieldMap: Record<string, string> = { device: 'device', link: 'link', metro: 'metro', contributor: 'contributor', validator: 'validator', gossip_node: 'validator', user: 'user' }
            const field = fieldMap[event.entity_type]
            const codeClassName = cn(
              "text-sm font-semibold text-left",
              event.entity_code.length > 20 && "font-mono max-w-[120px] sm:max-w-[200px] md:max-w-none truncate"
            )
            return field ? (
              <FilterButton value={event.entity_code} field={field} className={codeClassName}>
                {event.entity_code}
              </FilterButton>
            ) : (
              <span className={codeClassName}>{event.entity_code}</span>
            )
          })()}
          {/* State change badges */}
          {changeType === 'created' && (
            <span className="inline-flex items-center gap-1 text-[11px] px-1.5 py-0.5 rounded bg-green-500/10 text-green-600 border border-green-500/20">
              <Plus className="h-2.5 w-2.5" />
              Created
            </span>
          )}
          {changeType === 'updated' && (
            <span className="inline-flex items-center gap-1 text-[11px] px-1.5 py-0.5 rounded bg-blue-500/10 text-blue-600 border border-blue-500/20">
              <Pencil className="h-2.5 w-2.5" />
              Updated
            </span>
          )}
          {changeType === 'deleted' && (
            <span className="inline-flex items-center gap-1 text-[11px] px-1.5 py-0.5 rounded bg-red-500/10 text-red-600 border border-red-500/20">
              <Trash2 className="h-2.5 w-2.5" />
              Deleted
            </span>
          )}
          {/* Telemetry event badges */}
          {event.event_type.endsWith('_started') && (
            <span className="inline-flex items-center gap-1 text-[11px] px-1.5 py-0.5 rounded bg-amber-500/10 text-amber-600 border border-amber-500/20">
              <Play className="h-2.5 w-2.5" />
              Started
            </span>
          )}
          {event.event_type.endsWith('_stopped') && (
            <span className="inline-flex items-center gap-1 text-[11px] px-1.5 py-0.5 rounded bg-green-500/10 text-green-600 border border-green-500/20">
              <Square className="h-2.5 w-2.5" />
              Stopped
            </span>
          )}
          {event.event_type.endsWith('_recovered') && (
            <span className="inline-flex items-center gap-1 text-[11px] px-1.5 py-0.5 rounded bg-green-500/10 text-green-600 border border-green-500/20">
              <RotateCcw className="h-2.5 w-2.5" />
              Recovered
            </span>
          )}
          {/* Validator/Gossip node badges */}
          {event.event_type.includes('_joined_dz') && (
            <span className="inline-flex items-center gap-1 text-[11px] px-1.5 py-0.5 rounded bg-green-500/10 text-green-600 border border-green-500/20">
              <LogIn className="h-2.5 w-2.5" />
              Joined DZ
            </span>
          )}
          {event.event_type.includes('_left_dz') && (
            <span className="inline-flex items-center gap-1 text-[11px] px-1.5 py-0.5 rounded bg-amber-500/10 text-amber-600 border border-amber-500/20">
              <LogOut className="h-2.5 w-2.5" />
              Left DZ
            </span>
          )}
          {event.event_type.includes('_joined_solana') && (
            <span className="inline-flex items-center gap-1 text-[11px] px-1.5 py-0.5 rounded bg-green-500/10 text-green-600 border border-green-500/20">
              <LogIn className="h-2.5 w-2.5" />
              Joined Solana
            </span>
          )}
          {event.event_type.includes('_left_solana') && (
            <span className="inline-flex items-center gap-1 text-[11px] px-1.5 py-0.5 rounded bg-amber-500/10 text-amber-600 border border-amber-500/20">
              <LogOut className="h-2.5 w-2.5" />
              Left Solana
            </span>
          )}
          {event.event_type.includes('_stake_increased') && (
            <span className="inline-flex items-center gap-1 text-[11px] px-1.5 py-0.5 rounded bg-green-500/10 text-green-600 border border-green-500/20">
              <TrendingUp className="h-2.5 w-2.5" />
              Stake Up
            </span>
          )}
          {event.event_type.includes('_stake_decreased') && (
            <span className="inline-flex items-center gap-1 text-[11px] px-1.5 py-0.5 rounded bg-amber-500/10 text-amber-600 border border-amber-500/20">
              <TrendingDown className="h-2.5 w-2.5" />
              Stake Down
            </span>
          )}
          {event.event_type.includes('_stake_changed') && (
            <span className={cn(
              'inline-flex items-center gap-1 text-[11px] px-1.5 py-0.5 rounded border',
              validatorDetails?.contribution_change_lamports && validatorDetails.contribution_change_lamports > 0
                ? 'bg-green-500/10 text-green-600 border-green-500/20'
                : 'bg-amber-500/10 text-amber-600 border-amber-500/20'
            )}>
              {validatorDetails?.contribution_change_lamports && validatorDetails.contribution_change_lamports > 0
                ? <TrendingUp className="h-2.5 w-2.5" />
                : <TrendingDown className="h-2.5 w-2.5" />
              }
              DZ Stake
            </span>
          )}
          {(() => {
            const changePct = validatorDetails?.stake_share_change_pct && validatorDetails.stake_share_change_pct !== 0
              ? validatorDetails.stake_share_change_pct
              : (event.event_type.includes('_joined') || event.event_type.includes('_left'))
                ? (validatorDetails?.stake_share_pct && validatorDetails.stake_share_pct > 0
                  ? (event.event_type.includes('_joined') ? validatorDetails.stake_share_pct : -validatorDetails.stake_share_pct)
                  : null)
                : null
            if (changePct == null) return null
            return (
              <span className={cn(
                'text-[11px] font-medium',
                changePct > 0 ? 'text-green-600' : 'text-amber-600'
              )}>
                {changePct > 0 ? '+' : '−'}{Math.abs(changePct).toFixed(3)}% stake
              </span>
            )
          })()}
          {validatorDetails?.dz_total_stake_share_pct !== undefined && validatorDetails.dz_total_stake_share_pct > 0 && (
            <span className="text-[11px] text-muted-foreground">
              · {validatorDetails.dz_total_stake_share_pct.toFixed(2)}% DZ total
            </span>
          )}

          <div className="flex-1" />

          <span
            className="text-xs text-muted-foreground flex items-center gap-1"
            title={new Date(event.timestamp).toLocaleString()}
          >
            <Clock className="h-3 w-3" />
            {formatTimeAgo(event.timestamp)}
          </span>
          <SeverityIcon className={cn(
            'h-3 w-3',
            event.severity === 'critical' && 'text-red-500',
            event.severity === 'warning' && 'text-amber-500',
            event.severity === 'info' && 'text-blue-500',
            event.severity === 'success' && 'text-green-500'
          )} />
        </div>
        <div className="text-sm">{event.title}</div>
        {event.description && (
          <div className="text-xs text-muted-foreground mt-1">{event.description}</div>
        )}

        {/* Owner · User inline for validator/gossip events */}
        {validatorDetails?.owner_pubkey && (
          <div className="text-xs text-muted-foreground mt-1">
            Owner: <FilterButton value={validatorDetails.owner_pubkey} field="user" className="font-mono text-foreground">{truncatePubkey(validatorDetails.owner_pubkey)}</FilterButton>
            {validatorDetails.user_pk && (
              <span> · User: <FilterButton value={validatorDetails.user_pk} field="user" className="font-mono text-foreground">{truncatePubkey(validatorDetails.user_pk)}</FilterButton></span>
            )}
          </div>
        )}

        {/* Device connection */}
        {validatorDetails?.device_pk && validatorDetails?.device_code && (
          <div className="text-xs text-muted-foreground mt-1">
            Connected to <FilterButton value={validatorDetails.device_code} field="device" className="font-medium text-foreground">{validatorDetails.device_code}</FilterButton>
            {validatorDetails.metro_code && <span> in <FilterButton value={validatorDetails.metro_code} field="metro" className="text-foreground">{validatorDetails.metro_code}</FilterButton></span>}
          </div>
        )}
        {userEntity?.device_pk && userEntity?.device_code && (
          <div className="text-xs text-muted-foreground mt-1">
            Connected to <FilterButton value={userEntity.device_code} field="device" className="font-medium text-foreground">{userEntity.device_code}</FilterButton>
            {userEntity.metro_code && <span> in <FilterButton value={userEntity.metro_code} field="metro" className="text-foreground">{userEntity.metro_code}</FilterButton></span>}
          </div>
        )}

        {/* Packet loss */}
        {packetLossDetails && (
          <div className="text-xs text-muted-foreground mt-1 space-y-0.5">
            <div>
              Route: <FilterButton value={packetLossDetails.side_a_metro} field="metro" className="font-medium text-foreground">{packetLossDetails.side_a_metro}</FilterButton>
              {' → '}
              <FilterButton value={packetLossDetails.side_z_metro} field="metro" className="font-medium text-foreground">{packetLossDetails.side_z_metro}</FilterButton>
            </div>
            <div>
              Loss: <span className={getLossColor(packetLossDetails.current_loss_pct)}>{packetLossDetails.current_loss_pct.toFixed(2)}%</span>
              {packetLossDetails.previous_loss_pct !== undefined && (
                <span className="text-muted-foreground"> (was {packetLossDetails.previous_loss_pct.toFixed(2)}%)</span>
              )}
            </div>
          </div>
        )}

        {/* Interface events */}
        {interfaceTotals && (
          <div className="text-xs text-muted-foreground mt-1 space-y-1">
            {interfaceTotals.carrierTransitions > 0 && (
              <div>Carrier transitions: <span className={getSeverityColor(interfaceTotals.carrierTransitions, carrierThresholds)}>{interfaceTotals.carrierTransitions}</span></div>
            )}
            {interfaceTotals.errors > 0 && (
              <div>Errors: <span className={getSeverityColor(interfaceTotals.errors, errorThresholds)}>{interfaceTotals.errors}</span></div>
            )}
            {interfaceTotals.discards > 0 && (
              <div>Discards: <span className={getSeverityColor(interfaceTotals.discards, discardThresholds)}>{interfaceTotals.discards}</span></div>
            )}
            <div className="flex flex-wrap gap-1">
              {interfaceTotals.interfaces.map((intf, i) => {
                const carrier = intf.carrier_transitions || 0
                const errors = (intf.in_errors || 0) + (intf.out_errors || 0)
                const discards = (intf.in_discards || 0) + (intf.out_discards || 0)
                const count = carrier > 0 ? carrier : errors > 0 ? errors : discards
                const colorClass = carrier > 0
                  ? getSeverityColor(carrier, carrierThresholds)
                  : errors > 0
                    ? getSeverityColor(errors, errorThresholds)
                    : getSeverityColor(discards, discardThresholds)
                return (
                  <span key={i} className="bg-muted px-1.5 py-0.5 rounded text-[10px]">
                    {intf.interface_name}
                    {count > 0 && <span className={colorClass}> ({count})</span>}
                  </span>
                )
              })}
            </div>
          </div>
        )}

        <ChangeSummary changes={changes} />

        {hasDetails && (
          <>
            <button
              onClick={() => setExpanded(!expanded)}
              className="flex items-center gap-1 text-xs text-muted-foreground mt-2 hover:text-foreground transition-colors"
            >
              {expanded ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
              {expanded ? 'Hide' : 'Show'} details
            </button>
            {expanded && <EventDetails event={event} />}
          </>
        )}
      </div>
    </div>
  )
}
