import { useState, useEffect } from 'react'
import { Sun, Moon, Monitor, Wallet, Infinity as InfinityIcon, MessageSquare, Trash2, ExternalLink } from 'lucide-react'
import { useTheme } from '@/hooks/use-theme'
import { useAuth } from '@/contexts/AuthContext'
import { getSlackInstallations, removeSlackInstallation, confirmSlackInstallation, type SlackInstallation } from '@/lib/api'
import { fetchConfig } from '@/lib/api'
import { ConfirmDialog } from '@/components/confirm-dialog'

const themeOptions = [
  { value: 'light' as const, label: 'Light', icon: Sun, description: 'Always use light mode' },
  { value: 'dark' as const, label: 'Dark', icon: Moon, description: 'Always use dark mode' },
  { value: 'system' as const, label: 'System', icon: Monitor, description: 'Follow your system preference' },
]

// Constants matching backend defaults
const MIN_SOL_THRESHOLD = 1.0
const WALLET_PREMIUM_LIMIT = 25
const LAMPORTS_PER_SOL = 1_000_000_000

function formatSOL(lamports: number): string {
  return (lamports / LAMPORTS_PER_SOL).toFixed(2)
}

export function SettingsPage() {
  const { theme, setTheme } = useTheme()
  const { user, quota } = useAuth()
  const [slackEnabled, setSlackEnabled] = useState(false)
  const [installations, setInstallations] = useState<SlackInstallation[]>([])
  const [loadingInstallations, setLoadingInstallations] = useState(false)
  const [disconnecting, setDisconnecting] = useState<SlackInstallation | null>(null)
  const [pendingTakeover, setPendingTakeover] = useState<{ pendingId: string; team: string } | null>(null)

  const [slackMessage, setSlackMessage] = useState<{ type: 'success' | 'error' | 'warning'; text: string } | null>(null)

  const isDomainUser = user?.account_type === 'domain'
  const isWalletUser = user?.account_type === 'wallet'

  // Handle Slack OAuth redirect query params
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const slackParam = params.get('slack')
    if (slackParam === 'installed') {
      setSlackMessage({ type: 'success', text: 'Slack workspace connected successfully.' })
      // Refresh installations list
      getSlackInstallations().then(setInstallations).catch(() => {})
      window.history.replaceState({}, '', window.location.pathname)
    } else if (slackParam === 'confirm_takeover') {
      const team = params.get('team') || 'this workspace'
      const pendingId = params.get('pending_id') || ''
      if (pendingId) {
        setPendingTakeover({ pendingId, team })
      }
      window.history.replaceState({}, '', window.location.pathname)
    } else if (slackParam === 'error') {
      const reason = params.get('reason') || 'unknown'
      setSlackMessage({ type: 'error', text: `Slack connection failed: ${reason}` })
      window.history.replaceState({}, '', window.location.pathname)
    }
  }, [])

  useEffect(() => {
    fetchConfig().then(config => {
      setSlackEnabled(!!config.slackEnabled)
    })
  }, [])

  useEffect(() => {
    if (slackEnabled && isDomainUser) {
      setLoadingInstallations(true)
      getSlackInstallations()
        .then(setInstallations)
        .catch(() => setInstallations([]))
        .finally(() => setLoadingInstallations(false))
    }
  }, [slackEnabled, isDomainUser])

  const handleConfirmTakeover = async (pendingId: string) => {
    try {
      await confirmSlackInstallation(pendingId)
      setSlackMessage({ type: 'success', text: 'Slack workspace connected successfully.' })
      getSlackInstallations().then(setInstallations).catch(() => {})
    } catch {
      setSlackMessage({ type: 'error', text: 'Failed to confirm installation. The request may have expired.' })
    } finally {
      setPendingTakeover(null)
    }
  }

  const handleDisconnect = async (teamId: string) => {
    try {
      await removeSlackInstallation(teamId)
      setInstallations(prev => prev.filter(i => i.team_id !== teamId))
    } catch {
      // ignore
    } finally {
      setDisconnecting(null)
    }
  }

  const solBalance = user?.sol_balance ?? 0
  const solBalanceSOL = solBalance / LAMPORTS_PER_SOL
  const isPremiumWallet = isWalletUser && solBalanceSOL >= MIN_SOL_THRESHOLD
  const isUnlimited = quota?.limit === null

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-2xl mx-auto px-6 py-8">
        <h1 className="text-2xl font-semibold text-foreground mb-8">Settings</h1>

        {/* Quota Section */}
        <section className="mb-10">
          <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wide mb-4">
            Usage
          </h2>
          <div className="bg-card border border-border rounded-lg overflow-hidden">
            {/* Current Quota */}
            <div className="px-4 py-3">
              <div className="flex items-center justify-between">
                <div>
                  <div className="text-sm font-medium text-foreground">Daily Questions</div>
                  <div className="text-xs text-muted-foreground">
                    {isUnlimited ? 'Unlimited access' : `Resets at midnight UTC`}
                  </div>
                </div>
                <div className="text-right">
                  {isUnlimited ? (
                    <div className="flex items-center gap-1 text-sm font-medium text-foreground">
                      <InfinityIcon className="h-4 w-4" />
                      <span>Unlimited</span>
                    </div>
                  ) : (
                    <div className="text-sm font-medium text-foreground">
                      {quota?.remaining ?? 0} / {quota?.limit ?? 0} remaining
                    </div>
                  )}
                </div>
              </div>

              {/* Progress bar for limited users */}
              {!isUnlimited && quota?.limit && (
                <div className="mt-2">
                  <div className="h-2 bg-muted rounded-full overflow-hidden">
                    <div
                      className="h-full bg-primary transition-all"
                      style={{ width: `${((quota.remaining ?? 0) / quota.limit) * 100}%` }}
                    />
                  </div>
                </div>
              )}
            </div>

            {/* Wallet Balance (for wallet users) */}
            {isWalletUser && (
              <div className="px-4 py-3 border-t border-border">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Wallet className="h-4 w-4 text-muted-foreground" />
                    <div>
                      <div className="text-sm font-medium text-foreground">SOL Balance</div>
                      <div className="text-xs text-muted-foreground">
                        {user?.wallet_address?.slice(0, 4)}...{user?.wallet_address?.slice(-4)}
                      </div>
                    </div>
                  </div>
                  <div className="text-sm font-medium text-foreground">
                    {formatSOL(solBalance)} SOL
                  </div>
                </div>

                {/* Upgrade hint for non-premium wallet users */}
                {!isPremiumWallet && (
                  <div className="mt-3 p-3 bg-muted/50 rounded-md">
                    <p className="text-xs text-muted-foreground">
                      Hold at least {MIN_SOL_THRESHOLD} SOL to unlock {WALLET_PREMIUM_LIMIT} questions per day.
                    </p>
                  </div>
                )}
              </div>
            )}
          </div>
        </section>

        {/* Slack Integration Section */}
        {slackEnabled && isDomainUser && (
          <section className="mb-10">
            <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wide mb-4">
              Slack Integration
            </h2>
            {slackMessage && (
              <div className={`mb-3 px-4 py-3 rounded-lg text-sm ${
                slackMessage.type === 'success' ? 'bg-green-500/10 text-green-700 dark:text-green-400 border border-green-500/20' :
                slackMessage.type === 'warning' ? 'bg-yellow-500/10 text-yellow-700 dark:text-yellow-400 border border-yellow-500/20' :
                'bg-destructive/10 text-destructive border border-destructive/20'
              }`}>
                {slackMessage.text}
              </div>
            )}
            <div className="bg-card border border-border rounded-lg overflow-hidden">
              {/* Installed workspaces */}
              {!loadingInstallations && installations.map((inst, idx) => (
                <div
                  key={inst.team_id}
                  className={`px-4 py-3 flex items-center justify-between ${idx !== 0 ? 'border-t border-border' : ''}`}
                >
                  <div className="flex items-center gap-3">
                    <div className="p-2 rounded-md bg-muted text-muted-foreground">
                      <MessageSquare className="h-4 w-4" />
                    </div>
                    <div>
                      <div className="text-sm font-medium text-foreground">{inst.team_name || inst.team_id}</div>
                      <div className="text-xs text-muted-foreground">
                        Installed {new Date(inst.installed_at).toLocaleDateString()}
                      </div>
                    </div>
                  </div>
                  <button
                    onClick={() => setDisconnecting(inst)}
                    className="p-2 rounded-md text-muted-foreground hover:text-destructive hover:bg-destructive/10 transition-colors"
                    title="Disconnect workspace"
                  >
                    <Trash2 className="h-4 w-4" />
                  </button>
                </div>
              ))}

              {/* Add to Slack button */}
              <div className={`px-4 py-3 ${installations.length > 0 ? 'border-t border-border' : ''}`}>
                <button
                  onClick={async () => {
                    const res = await fetch('/api/slack/oauth/start', {
                      headers: { 'Authorization': `Bearer ${localStorage.getItem('lake_auth_token') || ''}` },
                    })
                    if (res.ok) {
                      const data = await res.json()
                      window.location.href = data.url
                    }
                  }}
                  className="inline-flex items-center gap-2 px-4 py-2 rounded-md bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 transition-colors"
                >
                  <ExternalLink className="h-4 w-4" />
                  Add to Slack
                </button>
              </div>
            </div>
          </section>
        )}

        {/* Theme Section */}
        <section className="mb-10">
          <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wide mb-4">
            Appearance
          </h2>
          <div className="bg-card border border-border rounded-lg overflow-hidden">
            {themeOptions.map((option, idx) => (
              <button
                key={option.value}
                onClick={() => setTheme(option.value)}
                className={`w-full flex items-center gap-4 px-4 py-3 text-left transition-colors hover:bg-muted/50 ${
                  idx !== 0 ? 'border-t border-border' : ''
                } ${theme === option.value ? 'bg-muted/30' : ''}`}
              >
                <div className={`p-2 rounded-md ${theme === option.value ? 'bg-primary/10 text-primary' : 'bg-muted text-muted-foreground'}`}>
                  <option.icon className="h-4 w-4" />
                </div>
                <div className="flex-1">
                  <div className="text-sm font-medium text-foreground">{option.label}</div>
                  <div className="text-xs text-muted-foreground">{option.description}</div>
                </div>
                {theme === option.value && (
                  <div className="w-2 h-2 rounded-full bg-primary" />
                )}
              </button>
            ))}
          </div>
        </section>
      </div>

      <ConfirmDialog
        isOpen={disconnecting !== null}
        title="Disconnect Slack workspace"
        message={`This will uninstall the app from ${disconnecting?.team_name || disconnecting?.team_id || 'this workspace'}. You can reconnect later by clicking "Add to Slack".`}
        confirmLabel="Disconnect"
        onConfirm={() => disconnecting && handleDisconnect(disconnecting.team_id)}
        onCancel={() => setDisconnecting(null)}
      />

      <ConfirmDialog
        isOpen={pendingTakeover !== null}
        title="Workspace already connected"
        message={`${pendingTakeover?.team || 'This workspace'} is already connected by another user. Do you want to take over the connection? The previous user will lose access to manage it.`}
        confirmLabel="Take over"
        onConfirm={() => pendingTakeover && handleConfirmTakeover(pendingTakeover.pendingId)}
        onCancel={() => setPendingTakeover(null)}
      />
    </div>
  )
}
