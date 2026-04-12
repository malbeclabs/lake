import { useState } from 'react'
import { Copy, Check } from 'lucide-react'

interface CopyableTextProps {
  text: string
  className?: string
  iconPosition?: 'left' | 'right'
  children?: React.ReactNode
}

export function CopyableText({ text, className = '', iconPosition = 'right', children }: CopyableTextProps) {
  const [copied, setCopied] = useState(false)

  const handleCopy = () => {
    navigator.clipboard.writeText(text)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const icon = (
    <button
      onClick={handleCopy}
      className="inline-flex items-center justify-center h-4 w-4 text-muted-foreground opacity-0 group-hover:opacity-100 hover:text-foreground transition-opacity cursor-pointer"
      title={copied ? 'Copied!' : 'Copy'}
    >
      {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
    </button>
  )

  return (
    <span className={`inline-flex items-center gap-1 group ${className}`}>
      {iconPosition === 'left' && icon}
      {children ?? <span>{text}</span>}
      {iconPosition === 'right' && icon}
    </span>
  )
}
