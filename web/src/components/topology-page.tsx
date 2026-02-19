import { useState, useEffect, useCallback, lazy, Suspense, Component, type ReactNode } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { useQuery } from '@tanstack/react-query'
import { fetchTopology } from '@/lib/api'
import { TopologyMap } from '@/components/topology-map'
import { TopologyGraph } from '@/components/topology-graph'
import { TopologyProvider } from '@/components/topology'
import { Globe, MonitorX } from 'lucide-react'

// Lazy-load the globe to avoid importing three.js/WebGL modules when GPU is unavailable
const TopologyGlobe = lazy(() => import('@/components/topology-globe').then(m => ({ default: m.TopologyGlobe })))

// Only show loading indicator after this delay to avoid flash on fast loads
const LOADING_DELAY_MS = 300

function isWebGLAvailable(): boolean {
  try {
    const canvas = document.createElement('canvas')
    return !!(canvas.getContext('webgl2') || canvas.getContext('webgl'))
  } catch {
    return false
  }
}

function GlobeUnsupported() {
  return (
    <div className="absolute inset-0 z-0 flex items-center justify-center bg-background">
      <div className="flex flex-col items-center gap-3 max-w-md text-center px-4">
        <MonitorX className="h-10 w-10 text-muted-foreground" />
        <div className="text-sm font-medium text-foreground">3D Globe Unavailable</div>
        <div className="text-sm text-muted-foreground">
          Your browser does not support WebGL, which is required for the 3D globe view.
          Try enabling hardware acceleration in your browser settings, or use the{' '}
          <a href="/topology/map" className="underline text-foreground hover:text-foreground/80">map view</a> instead.
        </div>
      </div>
    </div>
  )
}

interface GlobeErrorBoundaryState {
  hasError: boolean
}

class GlobeErrorBoundary extends Component<{ children: ReactNode }, GlobeErrorBoundaryState> {
  state: GlobeErrorBoundaryState = { hasError: false }

  static getDerivedStateFromError(): GlobeErrorBoundaryState {
    return { hasError: true }
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="absolute inset-0 z-0 flex items-center justify-center bg-background">
          <div className="flex flex-col items-center gap-3 max-w-md text-center px-4">
            <MonitorX className="h-10 w-10 text-muted-foreground" />
            <div className="text-sm font-medium text-foreground">3D Globe Unavailable</div>
            <div className="text-sm text-muted-foreground">
              Something went wrong loading the 3D globe. Your browser or device may not support the required graphics features.
              Try the{' '}
              <a href="/topology/map" className="underline text-foreground hover:text-foreground/80">map view</a> instead.
            </div>
          </div>
        </div>
      )
    }
    return this.props.children
  }
}

type ViewMode = 'map' | 'graph' | 'globe'

interface TopologyPageProps {
  view: ViewMode
}

function TopologyLoading() {
  return (
    <div className="absolute inset-0 z-0 flex items-center justify-center bg-background">
      <div className="flex flex-col items-center gap-3">
        <Globe className="h-10 w-10 text-muted-foreground animate-pulse" />
        <div className="text-sm text-muted-foreground">Loading network topology...</div>
      </div>
    </div>
  )
}

export function TopologyPage({ view }: TopologyPageProps) {
  useDocumentTitle('Topology')
  const [searchParams, setSearchParams] = useSearchParams()

  // Get selected device from URL (shared between views)
  const selectedDevicePK = searchParams.get('type') === 'device' ? searchParams.get('id') : null

  const { data, isLoading, error } = useQuery({
    queryKey: ['topology'],
    queryFn: fetchTopology,
    refetchInterval: 60000, // Refresh every minute
  })

  // Handle device selection from graph view
  const handleGraphDeviceSelect = useCallback((devicePK: string | null) => {
    setSearchParams(prev => {
      if (devicePK === null) {
        prev.delete('type')
        prev.delete('id')
      } else {
        prev.set('type', 'device')
        prev.set('id', devicePK)
      }
      return prev
    })
  }, [setSearchParams])

  // Delay showing loading indicator to avoid flash on fast loads
  const [showLoading, setShowLoading] = useState(false)
  useEffect(() => {
    if (isLoading) {
      const timer = setTimeout(() => setShowLoading(true), LOADING_DELAY_MS)
      return () => clearTimeout(timer)
    }
    setShowLoading(false)
  }, [isLoading])

  if (isLoading && (view === 'map' || view === 'globe')) {
    // Only show loading indicator after delay to avoid flash
    return showLoading ? <TopologyLoading /> : null
  }

  if (error && (view === 'map' || view === 'globe')) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-destructive">
          Failed to load topology: {error instanceof Error ? error.message : 'Unknown error'}
        </div>
      </div>
    )
  }

  return (
    <TopologyProvider view={view}>
      <div className="absolute inset-0 z-0 bg-background" data-topology-container>
        {view === 'map' && data && (
          <TopologyMap
            metros={data.metros}
            devices={data.devices}
            links={data.links}
            validators={data.validators}
          />
        )}

        {view === 'graph' && (
          <TopologyGraph
            selectedDevicePK={selectedDevicePK}
            onDeviceSelect={handleGraphDeviceSelect}
          />
        )}

        {view === 'globe' && data && (
          isWebGLAvailable() ? (
            <GlobeErrorBoundary>
              <Suspense fallback={<TopologyLoading />}>
                <TopologyGlobe
                  metros={data.metros}
                  devices={data.devices}
                  links={data.links}
                  validators={data.validators}
                />
              </Suspense>
            </GlobeErrorBoundary>
          ) : (
            <GlobeUnsupported />
          )
        )}
      </div>
    </TopologyProvider>
  )
}
