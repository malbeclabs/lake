import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import type {
  EdgeMulticastChannelInstance,
  EdgeMulticastSequenceHealth,
} from '@/lib/api'

import { TooltipProvider } from '@/components/ui/tooltip'

import { PublisherSequenceCell } from './edge-multicast-page'

// A gapped badge says data was lost. Until `gap_confined_nodes` it could not say whose loss it was,
// so a line losing 1% at one recorder printed exactly what a line losing it at every recorder
// printed — and on mainnet 2026-09-26 both Kalshi perps paths read `gapped` at cmh and was while
// dub recorded the same channel instances clean, which is a statement about those two recorders.
//
// The note is rendered beside the magnitude and not inside the badge: the badge grades the series,
// this attributes it, and folding the two would make a fault look smaller for being somebody
// else's.

const WINDOW = { endMs: 1_700_000_000_000, secs: 900 }

function instance(node: string, over: Partial<EdgeMulticastChannelInstance> = {}) {
  return {
    publisher_source_ip: '148.51.121.69',
    capture_source: 'tob_edge_kalshi_perps',
    channel_id: 1,
    node,
    location_code: node.slice(4, 7),
    messages: 50_000,
    gap_books: 0,
    resets: 0,
    snapshot_cycles: 0,
    snapshot_cycles_measured: false,
    gaps_measured: true,
    last_seen: new Date(WINDOW.endMs).toISOString(),
    status: 'ok',
    ...over,
  } as EdgeMulticastChannelInstance
}

function sequence(over: Partial<EdgeMulticastSequenceHealth>): EdgeMulticastSequenceHealth {
  return { status: 'gapped', gapped: 0, stalled: 0, instances: [], ...over } as EdgeMulticastSequenceHealth
}

function cell(health: EdgeMulticastSequenceHealth) {
  render(
    <TooltipProvider>
      <PublisherSequenceCell sequence={health} gapWindow={WINDOW} asOfAge={30} />
    </TooltipProvider>,
  )
}

describe('PublisherSequenceCell attribution note', () => {
  it('names the recorders the loss is confined to, beside the count', () => {
    cell(
      sequence({
        gapped: 2,
        gap_nodes: 3,
        gap_confined_nodes: ['aws-cmh-mn-recorder1', 'aws-was-mn-recorder1'],
        instances: [
          instance('aws-cmh-mn-recorder1', { status: 'gapped', gap_books: 20 }),
          instance('aws-was-mn-recorder1', { status: 'gapped', gap_books: 20 }),
          instance('aws-dub-mn-recorder1'),
        ],
      }),
    )
    expect(screen.getByText('gapped')).toBeInTheDocument()
    expect(screen.getByText('2/3')).toBeInTheDocument()
    expect(screen.getByText('at cmh and was')).toBeInTheDocument()
  })

  it('prefers the location code and falls back to the node id, as the loss strip does', () => {
    cell(
      sequence({
        gapped: 1,
        gap_nodes: 2,
        gap_confined_nodes: ['recorder-with-no-site'],
        instances: [
          instance('recorder-with-no-site', {
            status: 'gapped',
            gap_books: 3,
            location_code: undefined,
          }),
          instance('aws-dub-mn-recorder1'),
        ],
      }),
    )
    expect(screen.getByText('at recorder-with-no-site')).toBeInTheDocument()
  })

  // The finding the note must never appear over. Every vantage losing the same series is the path
  // losing it, and there is no recorder to charge it to.
  it('says nothing when the payload confines the loss to nobody', () => {
    cell(
      sequence({
        gapped: 2,
        gap_nodes: 2,
        instances: [
          instance('aws-cmh-mn-recorder1', { status: 'gapped', gap_books: 20 }),
          instance('aws-dub-mn-recorder1', { status: 'gapped', gap_books: 20 }),
        ],
      }),
    )
    expect(screen.queryByText(/^at /)).not.toBeInTheDocument()
  })

  // Past two names the count is the information. A sports line compares dozens of capture sources
  // across every recorder that holds them.
  it('caps the names and counts the rest', () => {
    cell(
      sequence({
        gapped: 3,
        gap_nodes: 4,
        gap_confined_nodes: ['aws-cmh-mn-recorder1', 'aws-dub-mn-recorder1', 'aws-was-mn-recorder1'],
        instances: [
          instance('aws-cmh-mn-recorder1', { status: 'gapped', gap_books: 2 }),
          instance('aws-dub-mn-recorder1', { status: 'gapped', gap_books: 2 }),
          instance('aws-was-mn-recorder1', { status: 'gapped', gap_books: 2 }),
          instance('aws-nrt-mn-recorder1'),
        ],
      }),
    )
    expect(screen.getByText('at cmh, dub and 1 more')).toBeInTheDocument()
  })
})
