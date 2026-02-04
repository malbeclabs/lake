import { Copy, Check, ExternalLink } from 'lucide-react'
import { useState, useMemo } from 'react'

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)

  const handleCopy = async () => {
    await navigator.clipboard.writeText(text)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <button
      onClick={handleCopy}
      className="p-1.5 rounded hover:bg-secondary transition-colors text-muted-foreground hover:text-foreground"
      title="Copy to clipboard"
    >
      {copied ? <Check className="h-4 w-4 text-green-500" /> : <Copy className="h-4 w-4" />}
    </button>
  )
}

function CodeBlock({ children, copyText }: { children: string; copyText?: string }) {
  return (
    <div className="relative group">
      <pre className="bg-secondary rounded-lg p-4 overflow-x-auto text-sm">
        <code>{children}</code>
      </pre>
      <div className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity">
        <CopyButton text={copyText ?? children} />
      </div>
    </div>
  )
}

export function MCPDocsPage() {
  const mcpUrl = useMemo(() => {
    // In dev, Vite runs on 5173 but API is on 8080 - external clients need the API URL
    if (window.location.hostname === 'localhost' && window.location.port === '5173') {
      return 'http://localhost:8080/api/mcp'
    }
    return `${window.location.origin}/api/mcp`
  }, [])

  const mcpJsonConfig = useMemo(() => `{
  "mcpServers": {
    "doublezero": {
      "type": "http",
      "url": "${mcpUrl}"
    }
  }
}`, [mcpUrl])
  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-3xl mx-auto px-8 py-12">
        <h1 className="text-2xl font-semibold mb-2">Connect Your Own AI</h1>
        <p className="text-muted-foreground mb-8">
          Use DoubleZero Data with your own AI assistant via the{' '}
          <a
            href="https://modelcontextprotocol.io"
            target="_blank"
            rel="noopener noreferrer"
            className="text-accent hover:underline inline-flex items-center gap-1"
          >
            Model Context Protocol (MCP)
            <ExternalLink className="h-3 w-3" />
          </a>
        </p>

        {/* Endpoint */}
        <section className="mb-10">
          <h2 className="text-lg font-medium mb-3">Endpoint</h2>
          <div className="flex items-center gap-2 bg-secondary rounded-lg px-4 py-3">
            <code className="flex-1 text-sm">{mcpUrl}</code>
            <CopyButton text={mcpUrl} />
          </div>
        </section>

        {/* Tools */}
        <section className="mb-10">
          <h2 className="text-lg font-medium mb-3">Available Tools</h2>
          <div className="border border-border rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-secondary">
                <tr>
                  <th className="text-left px-4 py-2 font-medium">Tool</th>
                  <th className="text-left px-4 py-2 font-medium">Description</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                <tr>
                  <td className="px-4 py-2 font-mono text-xs">execute_sql</td>
                  <td className="px-4 py-2 text-muted-foreground">Query ClickHouse for metrics, validators, and network data</td>
                </tr>
                <tr>
                  <td className="px-4 py-2 font-mono text-xs">execute_cypher</td>
                  <td className="px-4 py-2 text-muted-foreground">Query Neo4j for topology, paths, and connectivity</td>
                </tr>
                <tr>
                  <td className="px-4 py-2 font-mono text-xs">get_schema</td>
                  <td className="px-4 py-2 text-muted-foreground">Get database schema (tables, columns, types)</td>
                </tr>
                <tr>
                  <td className="px-4 py-2 font-mono text-xs">read_docs</td>
                  <td className="px-4 py-2 text-muted-foreground">Read DoubleZero documentation</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>

        {/* Claude Desktop */}
        <section className="mb-10">
          <h2 className="text-lg font-medium mb-3">Claude Desktop</h2>
          <ol className="list-decimal list-inside space-y-2 text-muted-foreground mb-4">
            <li>Open Claude Desktop and go to <span className="text-foreground">Settings</span></li>
            <li>Click <span className="text-foreground">Manage Connectors</span></li>
            <li>Click <span className="text-foreground">Add Custom Connector</span></li>
            <li>Enter the endpoint URL above</li>
          </ol>
        </section>

        {/* Code Editors & IDEs */}
        <section className="mb-10">
          <h2 className="text-lg font-medium mb-3">Code Editors & IDEs</h2>
          <p className="text-muted-foreground mb-3">
            Works with Claude Code, Cursor, Windsurf, Continue, and other MCP-compatible tools. Add a <code className="bg-secondary px-1.5 py-0.5 rounded text-sm">.mcp.json</code> file to your project root:
          </p>
          <CodeBlock copyText={mcpJsonConfig}>{mcpJsonConfig}</CodeBlock>
        </section>

        {/* Other MCP Clients */}
        <section className="mb-10">
          <h2 className="text-lg font-medium mb-3">Other MCP Clients</h2>
          <p className="text-muted-foreground">
            Any MCP-compatible client can connect using the endpoint URL. The server uses{' '}
            <a
              href="https://modelcontextprotocol.io/docs/concepts/transports#streamable-http"
              target="_blank"
              rel="noopener noreferrer"
              className="text-accent hover:underline"
            >
              Streamable HTTP transport
            </a>.
          </p>
        </section>
      </div>
    </div>
  )
}
