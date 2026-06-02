// djb2 hash → stable hue angle [0, 360) for a contributor name
export function contributorHue(name: string): number {
  let hash = 5381
  for (let i = 0; i < name.length; i++) {
    hash = ((hash << 5) + hash) + name.charCodeAt(i)
    hash = hash >>> 0  // keep unsigned 32-bit
  }
  return hash % 360
}

export function evBg(hue: number, alpha = 30): string {
  return `oklch(0.62 0.14 ${hue} / ${alpha}%)`
}

export function evBorderColor(hue: number, alpha = 90): string {
  return `oklch(0.68 0.16 ${hue} / ${alpha}%)`
}

export function evText(hue: number): string {
  return `oklch(0.78 0.14 ${hue})`
}

export function dotColor(hue: number): string {
  return `oklch(0.65 0.15 ${hue})`
}
