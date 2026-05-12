import { Link } from 'react-router-dom'
import { ArrowRight, Cable, Code, ExternalLink } from 'lucide-react'

export function DocsIndexPage() {
  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-3xl mx-auto px-8 py-12">
        <h1 className="text-2xl font-semibold mb-2">Developer Docs</h1>
        <p className="text-muted-foreground mb-8">
          Programmatic access to DoubleZero Data for applications and AI agents.
        </p>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <a
            href="/api/v1/docs"
            target="_blank"
            rel="noopener noreferrer"
            className="group rounded-xl border border-border bg-secondary/50 p-5 transition-colors hover:bg-secondary hover:border-muted-foreground/30"
          >
            <div className="flex items-center justify-between mb-3">
              <Code className="h-5 w-5 text-emerald-600 dark:text-emerald-400" />
              <ExternalLink className="h-4 w-4 text-muted-foreground/0 group-hover:text-muted-foreground transition-colors" />
            </div>
            <div className="font-medium text-sm mb-1">REST API v1</div>
            <div className="text-xs text-muted-foreground">
              Network telemetry, Solana validators, and the shred subscription program. OpenAPI reference.
            </div>
          </a>

          <Link
            to="/docs/mcp"
            className="group rounded-xl border border-border bg-secondary/50 p-5 transition-colors hover:bg-secondary hover:border-muted-foreground/30"
          >
            <div className="flex items-center justify-between mb-3">
              <Cable className="h-5 w-5 text-indigo-600 dark:text-indigo-400" />
              <ArrowRight className="h-4 w-4 text-muted-foreground/0 group-hover:text-muted-foreground transition-colors" />
            </div>
            <div className="font-medium text-sm mb-1">Connect Your Own AI</div>
            <div className="text-xs text-muted-foreground">
              Use DoubleZero Data from Claude, Cursor, or any MCP-compatible client.
            </div>
          </Link>
        </div>
      </div>
    </div>
  )
}
