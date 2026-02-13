declare module 'react-globe.gl' {
  import type { Object3D, Scene, Camera, WebGLRenderer } from 'three'

  interface GlobeInstance {
    pointOfView: (pov?: { lat?: number; lng?: number; altitude?: number }, transitionMs?: number) => GlobeInstance | { lat: number; lng: number; altitude: number }
    scene: () => Scene
    camera: () => Camera
    renderer: () => WebGLRenderer
    controls: () => { autoRotate: boolean; autoRotateSpeed: number; enabled: boolean }
    pauseAnimation: () => void
    resumeAnimation: () => void
  }

  interface GlobeProps {
    // Dimensions
    width?: number
    height?: number

    // Globe appearance
    globeImageUrl?: string
    bumpImageUrl?: string
    backgroundImageUrl?: string
    showGlobe?: boolean
    showAtmosphere?: boolean
    atmosphereColor?: string
    atmosphereAltitude?: number

    // Points layer
    pointsData?: object[]
    pointLat?: string | ((d: object) => number)
    pointLng?: string | ((d: object) => number)
    pointAltitude?: string | number | ((d: object) => number)
    pointRadius?: string | number | ((d: object) => number)
    pointColor?: string | ((d: object) => string)
    pointLabel?: string | ((d: object) => string)
    pointResolution?: number
    onPointClick?: (point: object, event: MouseEvent, coords: { lat: number; lng: number; altitude: number }) => void
    onPointHover?: (point: object | null, prevPoint: object | null) => void

    // Arcs layer
    arcsData?: object[]
    arcStartLat?: string | ((d: object) => number)
    arcStartLng?: string | ((d: object) => number)
    arcEndLat?: string | ((d: object) => number)
    arcEndLng?: string | ((d: object) => number)
    arcColor?: string | ((d: object) => string | string[])
    arcAltitude?: string | number | ((d: object) => number | null)
    arcAltitudeAutoScale?: number
    arcStroke?: string | number | ((d: object) => number | null)
    arcDashLength?: string | number | ((d: object) => number)
    arcDashGap?: string | number | ((d: object) => number)
    arcDashAnimateTime?: string | number | ((d: object) => number)
    arcLabel?: string | ((d: object) => string)
    onArcClick?: (arc: object, event: MouseEvent, coords: { lat: number; lng: number; altitude: number }) => void
    onArcHover?: (arc: object | null, prevArc: object | null) => void

    // Rings layer
    ringsData?: object[]
    ringLat?: string | ((d: object) => number)
    ringLng?: string | ((d: object) => number)
    ringAltitude?: number
    ringColor?: string | ((d: object) => string | string[])
    ringMaxRadius?: string | number | ((d: object) => number)
    ringPropagationSpeed?: string | number | ((d: object) => number)
    ringRepeatPeriod?: string | number | ((d: object) => number)

    // Interaction
    enablePointerInteraction?: boolean
    animateIn?: boolean

    // Ref
    ref?: React.Ref<GlobeInstance>
  }

  const Globe: React.ForwardRefExoticComponent<GlobeProps & React.RefAttributes<GlobeInstance>>
  export default Globe
  export type { GlobeInstance, GlobeProps }
}
