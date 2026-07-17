import { describe, it, expect, vi, beforeEach, type Mock } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { IssuesSyncAction } from './IssuesSyncAction'
import type { IssuePreviewItem, SyncedIssue } from '@/lib/api'

vi.mock('@/lib/api', () => ({
  previewPlanIssues: vi.fn(),
  syncPlanIssues: vi.fn(),
}))
import { previewPlanIssues, syncPlanIssues } from '@/lib/api'

const mockPreview = previewPlanIssues as unknown as Mock
const mockSync = syncPlanIssues as unknown as Mock

const REPO = 'malbeclabs/infra'

const previewItems: IssuePreviewItem[] = [
  {
    kind: 'contributor',
    contributor_pk: 'c-jump',
    contributor_code: 'jump_',
    is_parent: false,
    action: 'create',
    title: 't1',
    body: 'b1',
    repo: REPO,
  },
  {
    kind: 'parent',
    contributor_pk: '',
    contributor_code: '',
    is_parent: true,
    action: 'update',
    title: 'parent',
    body: 'pb',
    existing_issue_number: 7,
    existing_issue_url: `https://github.com/${REPO}/issues/7`,
    repo: REPO,
  },
]

const syncedItems: SyncedIssue[] = [
  {
    kind: 'contributor',
    contributor_pk: 'c-jump',
    contributor_code: 'jump_',
    is_parent: false,
    action: 'created',
    title: 't1',
    issue_number: 42,
    issue_url: `https://github.com/${REPO}/issues/42`,
    repo: REPO,
  },
  {
    kind: 'parent',
    contributor_pk: '',
    contributor_code: '',
    is_parent: true,
    action: 'updated',
    title: 'parent',
    issue_number: 7,
    issue_url: `https://github.com/${REPO}/issues/7`,
    repo: REPO,
  },
]

interface Deferred<T> {
  promise: Promise<T>
  resolve: (value: T) => void
  reject: (reason?: unknown) => void
}
function createDeferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

function renderAction(planStatus: string) {
  const client = new QueryClient({
    defaultOptions: { mutations: { retry: false }, queries: { retry: false } },
  })
  return render(
    <QueryClientProvider client={client}>
      <IssuesSyncAction planId="plan-1" planStatus={planStatus} />
    </QueryClientProvider>
  )
}

beforeEach(() => {
  mockPreview.mockReset()
  mockSync.mockReset()
})

describe('IssuesSyncAction', () => {
  it('gates the action to approved plans', () => {
    renderAction('draft')
    const btn = screen.getByRole('button', { name: /create \/ sync issues/i })
    expect(btn).toBeDisabled()
  })

  it('enables the action once the plan is approved', () => {
    renderAction('approved')
    const btn = screen.getByRole('button', { name: /create \/ sync issues/i })
    expect(btn).toBeEnabled()
  })

  it('previews on open and does NOT sync automatically', async () => {
    mockPreview.mockResolvedValue({ repo: REPO, issues: previewItems })
    renderAction('approved')

    fireEvent.click(screen.getByRole('button', { name: /create \/ sync issues/i }))

    // Preview is fetched when the dialog opens.
    await waitFor(() => expect(mockPreview).toHaveBeenCalledTimes(1))
    expect(mockPreview).toHaveBeenCalledWith('plan-1')
    // The create/update summary renders from the preview.
    await screen.findByText(/1 to create, 1 to update/i)
    // Nothing is written to GitHub without an explicit confirm.
    expect(mockSync).not.toHaveBeenCalled()
  })

  it('syncs only after an explicit confirm and then renders the issue links', async () => {
    mockPreview.mockResolvedValue({ repo: REPO, issues: previewItems })
    mockSync.mockResolvedValue({ repo: REPO, issues: syncedItems })
    renderAction('approved')

    fireEvent.click(screen.getByRole('button', { name: /create \/ sync issues/i }))
    const confirm = await screen.findByRole('button', { name: /confirm:/i })
    expect(mockSync).not.toHaveBeenCalled()

    fireEvent.click(confirm)

    await waitFor(() => expect(mockSync).toHaveBeenCalledTimes(1))
    expect(mockSync).toHaveBeenCalledWith('plan-1')

    // Resulting issue links render.
    const link = await screen.findByRole('link', { name: /#42/ })
    expect(link).toHaveAttribute('href', `https://github.com/${REPO}/issues/42`)
    expect(screen.getByRole('link', { name: /#7/ })).toBeInTheDocument()
  })

  it('does not fire a second sync if confirm is triggered twice', async () => {
    mockPreview.mockResolvedValue({ repo: REPO, issues: previewItems })
    const deferred = createDeferred<{ repo: string; issues: SyncedIssue[] }>()
    mockSync.mockReturnValue(deferred.promise)
    renderAction('approved')

    fireEvent.click(screen.getByRole('button', { name: /create \/ sync issues/i }))
    const confirm = await screen.findByRole('button', { name: /confirm:/i })

    fireEvent.click(confirm)
    await waitFor(() => expect(mockSync).toHaveBeenCalledTimes(1))
    // Button is now disabled (pending); a second attempt must not sync again.
    fireEvent.click(confirm)
    expect(mockSync).toHaveBeenCalledTimes(1)

    deferred.resolve({ repo: REPO, issues: syncedItems })
    await screen.findByRole('link', { name: /#42/ })
    expect(mockSync).toHaveBeenCalledTimes(1)
  })

  it('shows a clear message when GitHub is not configured (503)', async () => {
    mockPreview.mockRejectedValue(new Error('GitHub integration not configured'))
    renderAction('approved')

    fireEvent.click(screen.getByRole('button', { name: /create \/ sync issues/i }))

    expect(
      await screen.findByText(/github integration not configured/i)
    ).toBeInTheDocument()
    expect(mockSync).not.toHaveBeenCalled()
  })
})
