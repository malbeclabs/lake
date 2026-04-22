// web/src/components/ops/CreateIncidentModal.tsx
import { useState, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useCreateOpsTicket, useOpsAssignees } from '@/hooks/use-ops-tickets'
import { useAuth } from '@/contexts/AuthContext'
import { fetchLinks, fetchDevices } from '@/lib/api'
import type { OpsTicketSeverity, OpsTicketType, OpsTicketStatus } from '@/lib/ops-api'

const ISSUE_REASON_LABELS: Record<string, string> = {
  packet_loss: 'packet loss',
  high_latency: 'high latency',
  high_utilization: 'high utilization',
  interface_errors: 'interface errors',
  fcs_errors: 'FCS errors',
  discards: 'discards',
  carrier_transitions: 'carrier transitions',
  missing_adjacency: 'ISIS adjacency down',
  isis_overload: 'ISIS overload',
  isis_unreachable: 'ISIS unreachable',
  drained: 'drained',
  no_data: 'no data',
}

const SEVERITIES: { value: OpsTicketSeverity; label: string }[] = [
  { value: 'sev1', label: 'sev1 — critical' },
  { value: 'sev2', label: 'sev2 — major' },
  { value: 'sev3', label: 'sev3 — minor' },
]

interface CreateIncidentModalProps {
  entityCode?: string          // e.g. "lax-tve2:sea-tve2" — omit for blank mode
  entityType?: 'link' | 'device'
  entityPk?: string
  contributorCode?: string
  contributorPk?: string       // Solana pubkey of the entity's contributor
  downSince?: string
  issueReasons?: string[]      // pass from timeline row for smart defaults
  onClose: () => void
  onSuccess: () => void
}

function deriveEntityContextDefaults(
  entityCode: string,
  entityType: 'link' | 'device',
  issueReasons?: string[]
): { title: string; description: string } {
  const kind = entityType === 'link' ? 'Link' : 'Device'
  if (!issueReasons || issueReasons.length === 0) {
    return {
      title: `${entityCode} is down`,
      description: `${kind} is currently down. Please investigate.`,
    }
  }
  if (issueReasons.length === 1) {
    const label = ISSUE_REASON_LABELS[issueReasons[0]] ?? issueReasons[0]
    return {
      title: `${entityCode} — ${label}`,
      description: `${kind} is experiencing ${label}. Please investigate.`,
    }
  }
  const labels = issueReasons.map(r => ISSUE_REASON_LABELS[r] ?? r).join(', ')
  return {
    title: `${entityCode} — multiple issues`,
    description: `${kind} is experiencing: ${labels}. Please investigate.`,
  }
}

// Returns a datetime-local string in UTC so the input always displays UTC time.
function toUTCDatetimeLocal(isoStr?: string): string {
  const d = isoStr ? new Date(isoStr) : new Date()
  const pad = (n: number) => n.toString().padStart(2, '0')
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`
}

// Interprets a datetime-local string as UTC and returns an ISO string.
function utcDatetimeLocalToISO(s: string): string {
  // datetime-local format is "YYYY-MM-DDTHH:MM" — append :00Z to force UTC interpretation
  return new Date(s + ':00Z').toISOString()
}

export function CreateIncidentModal({
  entityCode,
  entityType,
  entityPk,
  contributorCode,
  downSince,
  issueReasons,
  onClose,
  onSuccess,
}: CreateIncidentModalProps) {
  const { user } = useAuth()
  const { mutate: createTicket, isPending, error } = useCreateOpsTicket()

  const defaults = entityCode && entityType
    ? deriveEntityContextDefaults(entityCode, entityType, issueReasons)
    : { title: '', description: '' }

  const [title, setTitle] = useState(defaults.title)
  const [description, setDescription] = useState(defaults.description)
  const [severity, setSeverity] = useState<OpsTicketSeverity>('sev3')
  const [submitError, setSubmitError] = useState<string | null>(null)

  const isBlankMode = !entityCode

  // Entity-context mode: "started at" datetime (prefilled, editable)
  const [entityStartAt, setEntityStartAt] = useState(() => toUTCDatetimeLocal(downSince))

  // Blank mode state
  const [ticketType, setTicketType] = useState<OpsTicketType>('incident')
  const [entityScope, setEntityScope] = useState<'link' | 'device' | 'both' | null>(null)
  const [linkQuery, setLinkQuery] = useState('')
  const [selectedLink, setSelectedLink] = useState<{ pk: string; code: string } | null>(null)
  const [linkDropdownOpen, setLinkDropdownOpen] = useState(false)
  const [deviceQuery, setDeviceQuery] = useState('')
  const [selectedDevice, setSelectedDevice] = useState<{ pk: string; code: string } | null>(null)
  const [deviceDropdownOpen, setDeviceDropdownOpen] = useState(false)
  const [blankStatus, setBlankStatus] = useState<OpsTicketStatus>('open')
  const [startAt, setStartAt] = useState('')
  const [endAt, setEndAt] = useState('')
  const [assigneeId, setAssigneeId] = useState('')
  const { data: assigneesData } = useOpsAssignees()
  const assignees = assigneesData?.assignees ?? []

  // Auto-prefill assignee from contributorCode once assignees load (entity-context mode only)
  useEffect(() => {
    if (!isBlankMode && !assigneeId && contributorCode && assignees.length > 0) {
      const match = assignees.find(a => a.value === contributorCode)
      if (match) setAssigneeId(match.value)
    }
  }, [assignees, contributorCode, isBlankMode, assigneeId])

  // Fetch all links/devices for entity search — blank mode only
  const { data: linksData } = useQuery({
    queryKey: ['links-for-ops-create'],
    queryFn: () => fetchLinks(1000),
    enabled: isBlankMode,
    staleTime: 5 * 60 * 1000,
  })
  const { data: devicesData } = useQuery({
    queryKey: ['devices-for-ops-create'],
    queryFn: () => fetchDevices(1000),
    enabled: isBlankMode,
    staleTime: 5 * 60 * 1000,
  })

  const allLinks = linksData?.items ?? []
  const allDevices = devicesData?.items ?? []

  const filteredLinks = linkQuery
    ? allLinks.filter(l => l.code.toLowerCase().includes(linkQuery.toLowerCase()))
    : allLinks

  const filteredDevices = deviceQuery
    ? allDevices.filter(d => d.code.toLowerCase().includes(deviceQuery.toLowerCase()))
    : allDevices

  const incidentStatuses: OpsTicketStatus[] = ['open', 'acknowledged', 'investigating', 'mitigating', 'monitoring']
  const maintenanceStatuses: OpsTicketStatus[] = ['planned', 'in-progress']
  const statusOptions = ticketType === 'incident' ? incidentStatuses : maintenanceStatuses

  // Reset status when type changes to keep it valid
  useEffect(() => {
    setBlankStatus(ticketType === 'incident' ? 'open' : 'planned')
  }, [ticketType])

  // Close on Escape
  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose() }
    document.addEventListener('keydown', handler)
    return () => document.removeEventListener('keydown', handler)
  }, [onClose])

  const reporterFirstName = user?.display_name
    ? user.display_name.split(' ')[0]
    : ''
  const reporterEmail = user?.email ?? ''

  function validateBlankMode(): string | null {
    if (!assigneeId) return 'Contributor is required'
    if (!title.trim()) return 'Title is required'
    if (title.length > 100) return 'Title must be 100 characters or fewer'
    if (!description.trim()) return 'Description is required'
    if (description.length > 500) return 'Description must be 500 characters or fewer'
    if (!entityScope) return 'Select an entity scope (Link, Device, or Both)'
    if ((entityScope === 'link' || entityScope === 'both') && !selectedLink) return 'Select a link'
    if ((entityScope === 'device' || entityScope === 'both') && !selectedDevice) return 'Select a device'
    if (ticketType === 'maintenance' && !startAt) return 'Start time is required for maintenance'
    return null
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setSubmitError(null)

    if (isBlankMode) {
      const err = validateBlankMode()
      if (err) { setSubmitError(err); return }

      const linkPks = (entityScope === 'link' || entityScope === 'both') && selectedLink ? [selectedLink.pk] : []
      const devicePks = (entityScope === 'device' || entityScope === 'both') && selectedDevice ? [selectedDevice.pk] : []
      const selectedAssignee = assignees.find(a => a.value === assigneeId)

      createTicket(
        {
          type: ticketType,
          title,
          description,
          severity,
          status: blankStatus,
          affected_link_pubkey: linkPks,
          device_pubkey: devicePks,
          contributor_pubkey: selectedAssignee?.pubkey ?? null,
          ...(ticketType === 'maintenance' && startAt ? { start_at: utcDatetimeLocalToISO(startAt) } : {}),
          ...(ticketType === 'maintenance' && endAt ? { end_at: utcDatetimeLocalToISO(endAt) } : {}),
        },
        { onSuccess: () => { onSuccess(); onClose() } }
      )
      return
    }

    // Entity-context mode
    if (!assigneeId) { setSubmitError('Contributor is required'); return }

    const selectedAssigneeEntity = assignees.find(a => a.value === assigneeId)
    const linkPkField: string[] = entityType === 'link' && entityPk ? [entityPk] : []
    const devicePkField: string[] = entityType === 'device' && entityPk ? [entityPk] : []

    createTicket(
      {
        type: 'incident' as OpsTicketType,
        title,
        description,
        severity,
        status: 'open',
        affected_link_pubkey: linkPkField,
        device_pubkey: devicePkField,
        contributor_pubkey: selectedAssigneeEntity?.pubkey ?? null,
        ...(entityStartAt ? { start_at: utcDatetimeLocalToISO(entityStartAt) } : {}),
      },
      { onSuccess: () => { onSuccess(); onClose() } }
    )
  }

  return (
    <div
      className="fixed inset-0 bg-black/65 flex items-center justify-center z-50"
      onClick={(e) => { if (e.target === e.currentTarget) onClose() }}
    >
      <div className="bg-background border border-border w-full max-w-[480px] p-6 shadow-2xl relative">
        <button
          className="absolute top-4 right-4 text-muted-foreground hover:text-foreground text-lg leading-none px-1.5"
          onClick={onClose}
        >
          ×
        </button>

        <h2 className="font-serif font-normal text-[17px] mb-1">Create incident</h2>
        <p className="text-xs text-muted-foreground mb-5">
          {entityCode
            ? 'Review or edit the defaults, then click Open incident.'
            : 'Fill in the details below to create a new ticket.'}
        </p>

        {/* Context strip — only shown when opening for a specific entity */}
        {entityCode && (
          <div className="flex flex-col border border-border mb-4 text-xs text-muted-foreground">
            <div className="flex-1 px-2.5 py-1.5">
              <div className="text-[10px] uppercase tracking-wide mb-0.5">
                {entityType === 'link' ? 'Link' : 'Device'}
              </div>
              <span className="font-mono text-sm text-foreground">{entityCode}</span>
            </div>
          </div>
        )}

        <form onSubmit={handleSubmit}>
          {/* Entity-context mode: assignee + started at */}
          {!isBlankMode && (
            <>
              <div className="mb-3.5">
                <label className="block text-[11px] uppercase tracking-wide text-muted-foreground mb-1.5">
                  Contributor <span className="normal-case tracking-normal text-red-400">*</span>
                </label>
                <select
                  value={assigneeId}
                  onChange={e => setAssigneeId(e.target.value)}
                  className="w-full text-[13px] px-2.5 py-1.5 bg-muted/30 border border-border/60 text-foreground outline-none focus:border-border"
                  required
                >
                  <option value="">Select contributor…</option>
                  {assignees.map(a => (
                    <option key={a.value} value={a.value}>{a.label}</option>
                  ))}
                </select>
              </div>
              <div className="mb-3.5">
                <label className="block text-[11px] uppercase tracking-wide text-muted-foreground mb-1.5">
                  Started at <span className="normal-case tracking-normal">(UTC)</span>
                </label>
                <input
                  type="datetime-local"
                  value={entityStartAt}
                  onChange={e => setEntityStartAt(e.target.value)}
                  className="w-full text-[13px] px-2.5 py-1.5 bg-muted/30 border border-border/60 text-foreground outline-none focus:border-border"
                />
              </div>
            </>
          )}

          {/* Blank mode: type + entity scope + entity search */}
          {isBlankMode && (
            <>
              {/* Assignee — required, first field */}
              <div className="mb-3.5">
                <label className="block text-[11px] uppercase tracking-wide text-muted-foreground mb-1.5">
                  Contributor <span className="normal-case tracking-normal text-red-400">*</span>
                </label>
                <select
                  value={assigneeId}
                  onChange={e => setAssigneeId(e.target.value)}
                  className="w-full text-[13px] px-2.5 py-1.5 bg-muted/30 border border-border/60 text-foreground outline-none focus:border-border"
                  required
                >
                  <option value="">Select contributor…</option>
                  {assignees.map(a => (
                    <option key={a.value} value={a.value}>{a.label}</option>
                  ))}
                </select>
              </div>

              {/* Type toggle */}
              <div className="mb-3.5">
                <label className="block text-[11px] uppercase tracking-wide text-muted-foreground mb-1.5">
                  Type
                </label>
                <div className="flex border border-border/60 overflow-hidden">
                  {(['incident', 'maintenance'] as OpsTicketType[]).map(t => (
                    <button
                      key={t}
                      type="button"
                      onClick={() => setTicketType(t)}
                      className={`flex-1 text-[12px] py-1.5 transition-colors ${
                        ticketType === t
                          ? 'bg-foreground/10 text-foreground font-medium'
                          : 'text-muted-foreground hover:text-foreground'
                      }`}
                    >
                      {t}
                    </button>
                  ))}
                </div>
              </div>

              {/* Entity scope toggle */}
              <div className="mb-3.5">
                <label className="block text-[11px] uppercase tracking-wide text-muted-foreground mb-1.5">
                  Entity scope
                </label>
                <div className="flex border border-border/60 overflow-hidden">
                  {(['link', 'device', 'both'] as const).map(scope => (
                    <button
                      key={scope}
                      type="button"
                      onClick={() => setEntityScope(scope)}
                      className={`flex-1 text-[12px] py-1.5 capitalize transition-colors ${
                        entityScope === scope
                          ? 'bg-foreground/10 text-foreground font-medium'
                          : 'text-muted-foreground hover:text-foreground'
                      }`}
                    >
                      {scope}
                    </button>
                  ))}
                </div>
              </div>

              {/* Link search */}
              {(entityScope === 'link' || entityScope === 'both') && (
                <div className="mb-3.5 relative">
                  <label className="block text-[11px] uppercase tracking-wide text-muted-foreground mb-1.5">
                    Link
                  </label>
                  <input
                    type="text"
                    value={selectedLink ? selectedLink.code : linkQuery}
                    onChange={e => {
                      setSelectedLink(null)
                      setLinkQuery(e.target.value)
                      setLinkDropdownOpen(true)
                    }}
                    onFocus={() => setLinkDropdownOpen(true)}
                    onBlur={() => setTimeout(() => setLinkDropdownOpen(false), 150)}
                    placeholder="Search by link code…"
                    className="w-full text-[13px] px-2.5 py-1.5 bg-muted/30 border border-border/60 text-foreground outline-none focus:border-border font-mono"
                  />
                  {linkDropdownOpen && filteredLinks.length > 0 && !selectedLink && (
                    <ul className="absolute z-10 w-full mt-0.5 border border-border bg-background shadow-lg max-h-40 overflow-y-auto">
                      {filteredLinks.slice(0, 20).map(l => (
                        <li
                          key={l.pk}
                          className="px-2.5 py-1.5 text-[12px] font-mono cursor-pointer hover:bg-muted/40 text-foreground"
                          onMouseDown={() => {
                            setSelectedLink({ pk: l.pk, code: l.code })
                            setLinkQuery('')
                            setLinkDropdownOpen(false)
                          }}
                        >
                          {l.code}
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              )}

              {/* Device search */}
              {(entityScope === 'device' || entityScope === 'both') && (
                <div className="mb-3.5 relative">
                  <label className="block text-[11px] uppercase tracking-wide text-muted-foreground mb-1.5">
                    Device
                  </label>
                  <input
                    type="text"
                    value={selectedDevice ? selectedDevice.code : deviceQuery}
                    onChange={e => {
                      setSelectedDevice(null)
                      setDeviceQuery(e.target.value)
                      setDeviceDropdownOpen(true)
                    }}
                    onFocus={() => setDeviceDropdownOpen(true)}
                    onBlur={() => setTimeout(() => setDeviceDropdownOpen(false), 150)}
                    placeholder="Search by device code…"
                    className="w-full text-[13px] px-2.5 py-1.5 bg-muted/30 border border-border/60 text-foreground outline-none focus:border-border font-mono"
                  />
                  {deviceDropdownOpen && filteredDevices.length > 0 && !selectedDevice && (
                    <ul className="absolute z-10 w-full mt-0.5 border border-border bg-background shadow-lg max-h-40 overflow-y-auto">
                      {filteredDevices.slice(0, 20).map(d => (
                        <li
                          key={d.pk}
                          className="px-2.5 py-1.5 text-[12px] font-mono cursor-pointer hover:bg-muted/40 text-foreground"
                          onMouseDown={() => {
                            setSelectedDevice({ pk: d.pk, code: d.code })
                            setDeviceQuery('')
                            setDeviceDropdownOpen(false)
                          }}
                        >
                          {d.code}
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              )}
            </>
          )}

          <div className="mb-3.5">
            <label className="block text-[11px] uppercase tracking-wide text-muted-foreground mb-1.5">
              Title
            </label>
            <input
              type="text"
              value={title}
              onChange={e => setTitle(e.target.value)}
              className="w-full text-[13px] px-2.5 py-1.5 bg-muted/30 border border-border/60 text-foreground outline-none focus:border-border"
              required
            />
          </div>

          <div className="mb-3.5">
            <label className="block text-[11px] uppercase tracking-wide text-muted-foreground mb-1.5">
              Description
            </label>
            <textarea
              value={description}
              onChange={e => setDescription(e.target.value)}
              rows={3}
              className="w-full text-[13px] px-2.5 py-1.5 bg-muted/30 border border-border/60 text-foreground outline-none focus:border-border resize-y leading-relaxed"
            />
          </div>

          <div className="flex gap-3 mb-3.5">
            <div className="flex-1">
              <label className="block text-[11px] uppercase tracking-wide text-muted-foreground mb-1.5">
                Severity
              </label>
              <select
                value={severity}
                onChange={e => setSeverity(e.target.value as OpsTicketSeverity)}
                className="w-full text-[13px] px-2.5 py-1.5 bg-muted/30 border border-border/60 text-foreground outline-none focus:border-border"
                required
              >
                {SEVERITIES.map(s => (
                  <option key={s.value} value={s.value}>{s.label}</option>
                ))}
              </select>
            </div>
            <div className="flex-1">
              <label className="block text-[11px] uppercase tracking-wide text-muted-foreground mb-1.5">
                Status
              </label>
              {isBlankMode ? (
                <select
                  value={blankStatus}
                  onChange={e => setBlankStatus(e.target.value as OpsTicketStatus)}
                  className="w-full text-[13px] px-2.5 py-1.5 bg-muted/30 border border-border/60 text-foreground outline-none focus:border-border"
                >
                  {statusOptions.map(s => (
                    <option key={s} value={s}>{s}</option>
                  ))}
                </select>
              ) : (
                <div className="text-[13px] px-2.5 py-1.5 bg-muted/40 border border-border text-muted-foreground">
                  open
                </div>
              )}
            </div>
          </div>

          {/* Maintenance time fields — blank mode only */}
          {isBlankMode && ticketType === 'maintenance' && (
            <div className="flex gap-3 mb-3.5">
              <div className="flex-1">
                <label className="block text-[11px] uppercase tracking-wide text-muted-foreground mb-1.5">
                  Start time <span className="normal-case tracking-normal">(UTC)</span>
                </label>
                <input
                  type="datetime-local"
                  value={startAt}
                  onChange={e => setStartAt(e.target.value)}
                  className="w-full text-[13px] px-2.5 py-1.5 bg-muted/30 border border-border/60 text-foreground outline-none focus:border-border"
                  required
                />
              </div>
              <div className="flex-1">
                <label className="block text-[11px] uppercase tracking-wide text-muted-foreground mb-1.5">
                  End time{' '}
                  <span className="normal-case tracking-normal text-muted-foreground/60">(UTC, optional)</span>
                </label>
                <input
                  type="datetime-local"
                  value={endAt}
                  onChange={e => setEndAt(e.target.value)}
                  className="w-full text-[13px] px-2.5 py-1.5 bg-muted/30 border border-border/60 text-foreground outline-none focus:border-border"
                />
              </div>
            </div>
          )}

          <div className="mb-3.5">
            <label className="block text-[11px] uppercase tracking-wide text-muted-foreground mb-1.5">
              Reporter
            </label>
            <div className="text-[13px] px-2.5 py-1.5 bg-muted/40 border border-border text-muted-foreground">
              {reporterFirstName} · {reporterEmail}
            </div>
          </div>

          {(error || submitError) && (
            <p className="text-xs text-red-400 mb-3">
              {submitError ?? 'Failed to create incident. Please try again.'}
            </p>
          )}

          <div className="flex justify-end gap-2 mt-5 pt-4 border-t border-border">
            <button
              type="button"
              onClick={onClose}
              className="text-xs px-3.5 py-1.5 border border-border text-muted-foreground hover:text-foreground transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isPending}
              className="text-xs font-medium px-3.5 py-1.5 border border-red-500/50 bg-red-500/12 text-red-300 hover:bg-red-500/20 hover:border-red-500/75 transition-colors disabled:opacity-50"
            >
              {isPending
                ? (isBlankMode ? 'Creating…' : 'Opening…')
                : (isBlankMode
                    ? (ticketType === 'maintenance' ? 'Schedule maintenance' : 'Open incident')
                    : 'Open incident')}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
