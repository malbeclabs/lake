import { apiFetch } from '@/lib/api'

// Shared utility functions for topology components

// Format bandwidth for display
export function formatBandwidth(bps: number): string {
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(1)} Tbps`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(1)} Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)} Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(1)} Kbps`
  return `${bps.toFixed(0)} bps`
}

// Format traffic rate for display
export function formatTrafficRate(bps: number | undefined | null): string {
  if (bps == null || bps <= 0) return 'N/A'
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(2)} Tbps`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(2)} Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(2)} Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(2)} Kbps`
  return `${bps.toFixed(0)} bps`
}

// Format rate for chart axis (compact)
export function formatChartAxisRate(bps: number): string {
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(1)}T`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(1)}G`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)}M`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(1)}K`
  return `${bps.toFixed(0)}`
}

// Format rate for chart tooltip (full)
export function formatChartTooltipRate(bps: number): string {
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(2)} Tbps`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(2)} Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(2)} Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(2)} Kbps`
  return `${bps.toFixed(0)} bps`
}

// Format packets per second for chart axis (compact)
export function formatChartAxisPps(pps: number): string {
  if (pps >= 1e9) return `${(pps / 1e9).toFixed(1)}G`
  if (pps >= 1e6) return `${(pps / 1e6).toFixed(1)}M`
  if (pps >= 1e3) return `${(pps / 1e3).toFixed(1)}K`
  return `${pps.toFixed(0)}`
}

// Format packets per second for chart tooltip (full)
export function formatChartTooltipPps(pps: number): string {
  if (pps >= 1e9) return `${(pps / 1e9).toFixed(2)} Gpps`
  if (pps >= 1e6) return `${(pps / 1e6).toFixed(2)} Mpps`
  if (pps >= 1e3) return `${(pps / 1e3).toFixed(2)} Kpps`
  return `${pps.toFixed(0)} pps`
}

// Format bits per second to human readable (compact, no space)
export function formatBps(bps: number): string {
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(1)}Tbps`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(1)}Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)}Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(0)}Kbps`
  return `${bps.toFixed(0)}bps`
}

// Format stake in SOL (compact display)
export function formatStake(stakeSol: number): string {
  if (stakeSol >= 1_000_000) return `${(stakeSol / 1_000_000).toFixed(1)}M SOL`
  if (stakeSol >= 1_000) return `${(stakeSol / 1_000).toFixed(0)}K SOL`
  return `${stakeSol.toFixed(0)} SOL`
}

// Bucket size type matching the traffic dashboard
export type BucketSize = 'auto' | '10 SECOND' | '30 SECOND' | '1 MINUTE' | '5 MINUTE' | '10 MINUTE' | '30 MINUTE' | '1 HOUR'

// Metric type for traffic charts
export type TrafficMetric = 'throughput' | 'packets'

// Resolve auto bucket to an effective bucket size label based on time range preset.
// This mirrors the backend's calculateBucketSize() function and matches the traffic dashboard.
export function resolveAutoBucket(preset: TimeRangePreset): BucketSize {
  switch (preset) {
    case '15m': return '10 SECOND'
    case '30m': return '10 SECOND'
    case '1h': return '10 SECOND'
    case '3h': return '30 SECOND'
    case '6h': return '1 MINUTE'
    case '12h': return '1 MINUTE'
    case '24h': return '5 MINUTE'
    case '2d': return '10 MINUTE'
    case '7d': return '30 MINUTE'
    default: return '5 MINUTE'
  }
}

export const bucketLabels: Record<BucketSize, string> = {
  'auto': 'Auto',
  '10 SECOND': '10s',
  '30 SECOND': '30s',
  '1 MINUTE': '1m',
  '5 MINUTE': '5m',
  '10 MINUTE': '10m',
  '30 MINUTE': '30m',
  '1 HOUR': '1h',
}

/** Convert a BucketSize to seconds */
export function bucketSizeToSeconds(b: BucketSize): number {
  switch (b) {
    case '10 SECOND': return 10
    case '30 SECOND': return 30
    case '1 MINUTE': return 60
    case '5 MINUTE': return 300
    case '10 MINUTE': return 600
    case '30 MINUTE': return 1800
    case '1 HOUR': return 3600
    default: return 300
  }
}

/** Convert a TimeRangePreset to seconds */
export function presetToSeconds(preset: TimeRangePreset): number {
  switch (preset) {
    case '15m': return 900
    case '30m': return 1800
    case '1h': return 3600
    case '3h': return 10800
    case '6h': return 21600
    case '12h': return 43200
    case '24h': return 86400
    case '2d': return 172800
    case '7d': return 604800
    default: return 86400
  }
}

/** Append time range, bucket, and metric query params */
function appendTrafficParams(params: URLSearchParams, timeRange?: TimeRange, bucket?: BucketSize, metric?: TrafficMetric) {
  if (timeRange) {
    if (timeRange.preset === 'custom' && timeRange.from && timeRange.to) {
      params.set('from', timeRange.from)
      params.set('to', timeRange.to)
    } else if (timeRange.preset !== 'custom') {
      params.set('range', timeRange.preset)
    }
  }
  if (bucket && bucket !== 'auto') {
    params.set('bucket', bucket)
  }
  if (metric && metric !== 'throughput') {
    params.set('metric', metric)
  }
}

// Fetch traffic history for a link, device, or validator
export async function fetchTrafficHistory(
  type: 'link' | 'device' | 'validator',
  pk: string,
  timeRange?: TimeRange,
  bucket?: BucketSize,
  metric?: TrafficMetric
): Promise<{ time: string; avgIn: number; avgOut: number; peakIn: number; peakOut: number }[]> {
  const params = new URLSearchParams({ type, pk })
  appendTrafficParams(params, timeRange, bucket, metric)

  if (bucket && bucket !== 'auto') {
    params.set('bucket', bucket)
  }

  const res = await apiFetch(`/api/topology/traffic?${params.toString()}`)
  if (!res.ok) return []
  const data = await res.json()
  return data.points || []
}

// Per-interface traffic data point
export interface InterfaceTrafficPoint {
  time: string
  intf: string
  avgIn: number
  avgOut: number
  peakIn: number
  peakOut: number
}

// Fetch per-interface traffic history for a link or device
export async function fetchTrafficHistoryByInterface(
  type: 'link' | 'device',
  pk: string,
  timeRange?: TimeRange,
  bucket?: BucketSize,
  metric?: TrafficMetric
): Promise<InterfaceTrafficPoint[]> {
  const params = new URLSearchParams({ type, pk, breakdown: 'interface' })
  appendTrafficParams(params, timeRange, bucket, metric)

  if (bucket && bucket !== 'auto') {
    params.set('bucket', bucket)
  }

  const res = await apiFetch(`/api/topology/traffic?${params.toString()}`)
  if (!res.ok) return []
  const data = await res.json()
  return data.interfaces || []
}

// Latency data point for charts
export interface LatencyDataPoint {
  time: string
  avgRttMs: number
  p95RttMs: number
  avgJitter: number
  lossPct: number
  avgRttAtoZMs?: number
  p95RttAtoZMs?: number
  avgRttZtoAMs?: number
  p95RttZtoAMs?: number
  jitterAtoZMs?: number
  jitterZtoAMs?: number
}

// Time range options for latency charts
export type TimeRangePreset = '15m' | '30m' | '1h' | '3h' | '6h' | '12h' | '24h' | '2d' | '7d' | 'custom'

export interface TimeRange {
  preset: TimeRangePreset
  from?: string // yyyy-mm-dd-hh:mm:ss
  to?: string   // yyyy-mm-dd-hh:mm:ss
}

// Fetch latency history for a link with optional time range
export async function fetchLatencyHistory(
  pk: string,
  timeRange?: TimeRange,
  bucket?: BucketSize
): Promise<LatencyDataPoint[]> {
  const params = new URLSearchParams({ pk })

  if (timeRange) {
    if (timeRange.preset === 'custom' && timeRange.from && timeRange.to) {
      params.set('from', timeRange.from)
      params.set('to', timeRange.to)
    } else if (timeRange.preset !== 'custom') {
      params.set('range', timeRange.preset)
    }
  }
  if (bucket && bucket !== 'auto') {
    params.set('bucket', bucket)
  }

  const res = await apiFetch(`/api/topology/link-latency?${params.toString()}`)
  if (!res.ok) return []
  const data = await res.json()
  return data.points || []
}
