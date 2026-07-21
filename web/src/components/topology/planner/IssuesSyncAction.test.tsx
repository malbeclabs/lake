import { describe, it, expect, vi, beforeEach, type Mock } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { IssuesSyncAction } from './IssuesSyncAction'
import type { IssuePreviewItem } from '@/lib/api'

vi.mock('@/lib/api', () => ({
  previewPlanIssues: vi.fn(),
}))
import { previewPlanIssues } from '@/lib/api'

const mockPreview = previewPlanIssues as unknown as Mock

const REPO = 'malbeclabs/infra'

const contributorItem: IssuePreviewItem = {
  kind: 'contributor',
  contributor_pk: 'c-jump',
  contributor_code: 'jump_',
  is_parent: false,
  action: 'create',
  title: 't1',
  body: 'b1\nline2',
  labels: ['topology-plan', 'plan:Test plan'],
  repo: REPO,
}

const decomItem: IssuePreviewItem = {
  kind: 'device_decom',
  contributor_pk: '',
  contributor_code: '',
  is_parent: false,
  action: 'create',
  title: 'Decommission device sea-dz01',
  body: 'decom body',
  labels: ['contributor-decommission', 'plan:Test plan'],
  entity_pk: 'dev-b-pk',
  repo: REPO,
}

const writeText = vi.fn().mockResolvedValue(undefined)
Object.defineProperty(navigator, 'clipboard', {
  value: { writeText },
  configurable: true,
  writable: true,
})

function renderAction(changeCount: number) {
  const client = new QueryClient({
    defaultOptions: { mutations: { retry: false }, queries: { retry: false } },
  })
  return render(
    <QueryClientProvider client={client}>
      <IssuesSyncAction planId="plan-1" changeCount={changeCount} />
    </QueryClientProvider>
  )
}

beforeEach(() => {
  mockPreview.mockReset()
  writeText.mockClear()
})

describe('IssuesSyncAction', () => {
  it('disables the action when the plan has no changes', () => {
    renderAction(0)
    const btn = screen.getByRole('button', { name: /create issues/i })
    expect(btn).toBeDisabled()
  })

  it('enables the action once the plan has at least one change', () => {
    renderAction(1)
    const btn = screen.getByRole('button', { name: /create issues/i })
    expect(btn).toBeEnabled()
  })

  it('previews on open (token-free: no confirm/sync step exists)', async () => {
    mockPreview.mockResolvedValue({ repo: REPO, issues: [contributorItem, decomItem] })
    renderAction(2)

    fireEvent.click(screen.getByRole('button', { name: /create issues/i }))

    await waitFor(() => expect(mockPreview).toHaveBeenCalledTimes(1))
    expect(mockPreview).toHaveBeenCalledWith('plan-1')
    await screen.findByText('t1')

    expect(screen.queryByRole('button', { name: /confirm/i })).not.toBeInTheDocument()
  })

  it('renders a kind chip for both contributor and decom issues, body collapsed by default', async () => {
    mockPreview.mockResolvedValue({ repo: REPO, issues: [contributorItem, decomItem] })
    renderAction(2)

    fireEvent.click(screen.getByRole('button', { name: /create issues/i }))
    await screen.findByText('t1')

    expect(screen.getByText('contributor')).toBeInTheDocument()
    expect(screen.getByText('decom')).toBeInTheDocument()
    expect(screen.getByText('Decommission device sea-dz01')).toBeInTheDocument()

    // Bodies are collapsed until "Show body" is clicked.
    expect(screen.queryByText(/line2/)).not.toBeInTheDocument()
    const showButtons = screen.getAllByRole('button', { name: /show body/i })
    fireEvent.click(showButtons[0])
    expect(await screen.findByText(/line2/)).toBeInTheDocument()
  })

  it('copies title+body to the clipboard via the Copy button', async () => {
    mockPreview.mockResolvedValue({ repo: REPO, issues: [contributorItem] })
    renderAction(1)

    fireEvent.click(screen.getByRole('button', { name: /create issues/i }))
    const copyBtn = await screen.findByRole('button', { name: /^copy$/i })
    fireEvent.click(copyBtn)

    await waitFor(() => expect(writeText).toHaveBeenCalledWith('t1\n\nb1\nline2'))
    expect(await screen.findByText('Copied')).toBeInTheDocument()
  })

  it('builds a well-formed Open-in-GitHub link with encoded title/body/labels', async () => {
    mockPreview.mockResolvedValue({ repo: REPO, issues: [contributorItem] })
    renderAction(1)

    fireEvent.click(screen.getByRole('button', { name: /create issues/i }))
    const link = await screen.findByRole('link', { name: /open in github/i })

    const expected =
      `https://github.com/${REPO}/issues/new?title=${encodeURIComponent(contributorItem.title)}` +
      `&body=${encodeURIComponent(contributorItem.body)}&labels=${encodeURIComponent(contributorItem.labels.join(','))}`
    expect(link).toHaveAttribute('href', expected)
    expect(link).toHaveAttribute('target', '_blank')
    expect(link).toHaveAttribute('rel', 'noreferrer')
  })

  it('shows an error message when the preview call fails', async () => {
    mockPreview.mockRejectedValue(new Error('failed to preview issues'))
    renderAction(1)

    fireEvent.click(screen.getByRole('button', { name: /create issues/i }))

    expect(await screen.findByText(/failed to preview issues/i)).toBeInTheDocument()
  })
})
