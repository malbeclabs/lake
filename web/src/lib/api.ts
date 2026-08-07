export interface TableInfo {
  name: string
  database: string
  engine: string
  type: 'table' | 'view'
  columns?: string[]
}

export interface CatalogResponse {
  tables: TableInfo[]
}

export interface QueryResponse {
  columns: string[]
  rows: unknown[][]
  row_count: number
  elapsed_ms: number
  error?: string
}

// Cypher query response uses map rows instead of array rows
export interface CypherQueryResponse {
  columns: string[]
  rows: Record<string, unknown>[]
  row_count: number
  elapsed_ms: number
  error?: string
}

export type QueryMode = 'sql' | 'cypher' | 'auto'

// Auth token storage key
const AUTH_TOKEN_KEY = 'lake_auth_token'
const ANONYMOUS_ID_KEY = 'lake_anonymous_id'

// Get the auth token from localStorage
export function getAuthToken(): string | null {
  return localStorage.getItem(AUTH_TOKEN_KEY)
}

// Set the auth token in localStorage
export function setAuthToken(token: string): void {
  localStorage.setItem(AUTH_TOKEN_KEY, token)
}

// Clear the auth token from localStorage
export function clearAuthToken(): void {
  localStorage.removeItem(AUTH_TOKEN_KEY)
}

// Get or generate an anonymous ID for unauthenticated users
export function getAnonymousId(): string {
  let id = localStorage.getItem(ANONYMOUS_ID_KEY)
  if (!id) {
    id = crypto.randomUUID()
    localStorage.setItem(ANONYMOUS_ID_KEY, id)
  }
  return id
}

// Get auth headers if token is present
function getAuthHeaders(): Record<string, string> {
  const token = getAuthToken()
  if (token) {
    return { Authorization: `Bearer ${token}` }
  }
  return {}
}

// Public config from API
export interface AppConfig {
  googleClientId?: string
  sentryDsn?: string
  sentryEnvironment?: string
  slackEnabled?: boolean
  env?: string
  availableEnvs?: string[]
  features?: Record<string, boolean>
}

// Environment storage key
const DZ_ENV_KEY = 'dz_env'

// Get the current environment from localStorage
export function getEnv(): string {
  return localStorage.getItem(DZ_ENV_KEY) || 'mainnet-beta'
}

// Set the current environment in localStorage and invalidate config cache
export function setEnv(env: string): void {
  localStorage.setItem(DZ_ENV_KEY, env)
  cachedConfig = null // Force re-fetch on next config request
}

// Cached config (fetched once at startup)
let cachedConfig: AppConfig | null = null

// Fetch public config from API
export async function fetchConfig(): Promise<AppConfig> {
  if (cachedConfig) {
    return cachedConfig
  }

  const response = await fetch('/api/config', {
    headers: { 'X-DZ-Env': getEnv() },
  })
  if (!response.ok) {
    console.warn('Failed to fetch config, using defaults')
    return {}
  }

  cachedConfig = await response.json()
  return cachedConfig!
}

// Simple fetch wrapper that adds auth + env headers to all API requests.
// Use this instead of bare fetch() for any /api/ call.
export function apiFetch(url: string, options?: RequestInit): Promise<Response> {
  const authHeaders = getAuthHeaders()
  return fetch(url, {
    ...options,
    headers: {
      'X-DZ-Env': getEnv(),
      ...authHeaders,
      ...options?.headers,
    },
  })
}

// Retry configuration
const RETRY_CONFIG = {
  maxRetries: 3,
  baseDelayMs: 500,
  maxDelayMs: 5000,
}

// Check if an error is retryable (network errors, 502/503/504)
function isRetryableError(error: unknown, status?: number): boolean {
  // Network errors (fetch failed)
  if (error instanceof TypeError && error.message.includes('fetch')) {
    return true
  }
  // Server temporarily unavailable
  if (status && [502, 503, 504].includes(status)) {
    return true
  }
  return false
}

// Sleep helper
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

// Retry wrapper for fetch calls with automatic auth headers
// forwardSourceParam appends ?source=raw to API URLs when the browser URL has it.
function forwardSourceParam(url: string): string {
  const browserSource = new URLSearchParams(window.location.search).get('source')
  if (browserSource) {
    const sep = url.includes('?') ? '&' : '?'
    return `${url}${sep}source=${encodeURIComponent(browserSource)}`
  }
  return url
}

async function fetchWithRetry(
  url: string,
  options?: RequestInit,
  config = RETRY_CONFIG
): Promise<Response> {
  url = forwardSourceParam(url)
  let lastError: unknown
  let lastStatus: number | undefined

  // Add auth and env headers to all requests
  const authHeaders = getAuthHeaders()
  const mergedOptions: RequestInit = {
    ...options,
    headers: {
      'X-DZ-Env': getEnv(),
      ...authHeaders,
      ...options?.headers,
    },
  }

  for (let attempt = 0; attempt <= config.maxRetries; attempt++) {
    try {
      const res = await fetch(url, mergedOptions)

      // Don't retry on successful responses or client errors (4xx)
      if (res.ok || (res.status >= 400 && res.status < 500)) {
        return res
      }

      // Server error - might be retryable
      lastStatus = res.status
      if (!isRetryableError(null, res.status) || attempt === config.maxRetries) {
        return res
      }
    } catch (err) {
      lastError = err
      // If not retryable or last attempt, throw
      if (!isRetryableError(err) || attempt === config.maxRetries) {
        throw err
      }
    }

    // Exponential backoff with jitter
    const delay = Math.min(
      config.baseDelayMs * Math.pow(2, attempt) + Math.random() * 100,
      config.maxDelayMs
    )
    await sleep(delay)
  }

  // Should not reach here, but handle edge case
  if (lastError) throw lastError
  throw new Error(`Request failed with status ${lastStatus}`)
}

export async function fetchCatalog(): Promise<CatalogResponse> {
  const res = await fetchWithRetry('/api/catalog')
  if (!res.ok) {
    throw new Error('Failed to fetch catalog')
  }
  return res.json()
}

// SQL query execution (uses new /api/sql/query endpoint)
export async function executeSqlQuery(query: string, env?: string): Promise<QueryResponse> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  if (env) {
    headers['X-DZ-Env'] = env
  }
  const res = await fetchWithRetry('/api/sql/query', {
    method: 'POST',
    headers,
    body: JSON.stringify({ query }),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || 'Failed to execute query')
  }
  return res.json()
}

// Cypher query execution
export async function executeCypherQuery(query: string, env?: string): Promise<CypherQueryResponse> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  if (env) {
    headers['X-DZ-Env'] = env
  }
  const res = await fetchWithRetry('/api/cypher/query', {
    method: 'POST',
    headers,
    body: JSON.stringify({ query }),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || 'Failed to execute Cypher query')
  }
  return res.json()
}

// Backward compatibility alias
export const executeQuery = executeSqlQuery

export interface GenerateResponse {
  sql: string
  error?: string
}

export interface HistoryMessage {
  role: 'user' | 'assistant'
  content: string
}

export async function generateSQL(prompt: string, currentQuery?: string, history?: HistoryMessage[]): Promise<GenerateResponse> {
  const res = await fetchWithRetry('/api/generate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ prompt, currentQuery, history }),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || 'Failed to generate SQL')
  }
  return res.json()
}

export interface StreamCallbacks {
  onToken: (token: string) => void
  onStatus: (status: { provider?: string; status?: string; attempt?: number; error?: string }) => void
  onDone: (result: GenerateResponse) => void
  onError: (error: string) => void
}

// SQL generation stream (uses new /api/sql/generate/stream endpoint)
export async function generateSqlStream(
  prompt: string,
  currentQuery: string | undefined,
  history: HistoryMessage[] | undefined,
  callbacks: StreamCallbacks
): Promise<void> {
  await generateQueryStream('/api/sql/generate/stream', prompt, currentQuery, history, callbacks)
}

// Cypher generation stream
export async function generateCypherStream(
  prompt: string,
  currentQuery: string | undefined,
  history: HistoryMessage[] | undefined,
  callbacks: StreamCallbacks
): Promise<void> {
  await generateQueryStream('/api/cypher/generate/stream', prompt, currentQuery, history, callbacks)
}

// Auto-detection stream callbacks
export interface AutoStreamCallbacks {
  onMode: (mode: 'sql' | 'cypher') => void
  onToken: (token: string) => void
  onStatus: (status: { provider?: string; status?: string; attempt?: number; error?: string }) => void
  onDone: (result: GenerateResponse) => void
  onError: (error: string) => void
}

// Auto-detection generation stream
export async function generateAutoStream(
  prompt: string,
  currentQuery: string | undefined,
  callbacks: AutoStreamCallbacks
): Promise<void> {
  // Retry initial connection with backoff
  let res: Response

  for (let attempt = 0; attempt <= RETRY_CONFIG.maxRetries; attempt++) {
    try {
      res = await apiFetch('/api/auto/generate/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt, currentQuery }),
      })

      if (res.ok) break
      if (res.status >= 400 && res.status < 500) break // Don't retry client errors

      if (!isRetryableError(null, res.status) || attempt === RETRY_CONFIG.maxRetries) break
    } catch (err) {
      if (!isRetryableError(err) || attempt === RETRY_CONFIG.maxRetries) {
        callbacks.onError('Connection failed. Please check your network and try again.')
        return
      }
    }

    const delay = Math.min(
      RETRY_CONFIG.baseDelayMs * Math.pow(2, attempt) + Math.random() * 100,
      RETRY_CONFIG.maxDelayMs
    )
    await sleep(delay)
  }

  if (!res!) {
    callbacks.onError('Connection failed. Please check your network and try again.')
    return
  }

  if (!res.ok) {
    const text = await res.text()
    callbacks.onError(text || 'Failed to generate query')
    return
  }

  const reader = res.body?.getReader()
  if (!reader) {
    callbacks.onError('Streaming not supported')
    return
  }

  const decoder = new TextDecoder()
  let buffer = ''

  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break

      buffer += decoder.decode(value, { stream: true })
      const lines = buffer.split('\n')
      buffer = lines.pop() || ''

      for (let i = 0; i < lines.length; i++) {
        const line = lines[i]
        if (line.startsWith('event: ')) {
          const eventType = line.slice(7)
          const nextLine = lines[i + 1]
          if (nextLine?.startsWith('data: ')) {
            const data = nextLine.slice(6)
            i++ // Skip the data line we just processed
            switch (eventType) {
              case 'mode':
                try {
                  const modeData = JSON.parse(data)
                  callbacks.onMode(modeData.mode)
                } catch {
                  // Ignore parse errors
                }
                break
              case 'token':
                callbacks.onToken(data)
                break
              case 'status':
                try {
                  callbacks.onStatus(JSON.parse(data))
                } catch {
                  callbacks.onStatus({ status: data })
                }
                break
              case 'done':
                try {
                  callbacks.onDone(JSON.parse(data))
                } catch {
                  callbacks.onError('Invalid response')
                }
                break
              case 'error':
                callbacks.onError(data)
                break
            }
          }
        }
      }
    }
  } catch (err) {
    // Connection was interrupted mid-stream
    if (err instanceof TypeError || (err instanceof Error && err.message.includes('network'))) {
      callbacks.onError('Connection lost. Please try again.')
    } else {
      callbacks.onError(err instanceof Error ? err.message : 'Stream error')
    }
  }
}

// Generic query generation stream (internal helper)
async function generateQueryStream(
  endpoint: string,
  prompt: string,
  currentQuery: string | undefined,
  history: HistoryMessage[] | undefined,
  callbacks: StreamCallbacks
): Promise<void> {
  // Retry initial connection with backoff
  let res: Response

  for (let attempt = 0; attempt <= RETRY_CONFIG.maxRetries; attempt++) {
    try {
      res = await apiFetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt, currentQuery, history }),
      })

      if (res.ok) break
      if (res.status >= 400 && res.status < 500) break // Don't retry client errors

      if (!isRetryableError(null, res.status) || attempt === RETRY_CONFIG.maxRetries) break
    } catch (err) {
      if (!isRetryableError(err) || attempt === RETRY_CONFIG.maxRetries) {
        callbacks.onError('Connection failed. Please check your network and try again.')
        return
      }
    }

    const delay = Math.min(
      RETRY_CONFIG.baseDelayMs * Math.pow(2, attempt) + Math.random() * 100,
      RETRY_CONFIG.maxDelayMs
    )
    await sleep(delay)
  }

  if (!res!) {
    callbacks.onError('Connection failed. Please check your network and try again.')
    return
  }

  if (!res.ok) {
    const text = await res.text()
    callbacks.onError(text || 'Failed to generate query')
    return
  }

  const reader = res.body?.getReader()
  if (!reader) {
    callbacks.onError('Streaming not supported')
    return
  }

  const decoder = new TextDecoder()
  let buffer = ''

  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break

      buffer += decoder.decode(value, { stream: true })
      const lines = buffer.split('\n')
      buffer = lines.pop() || ''

      for (let i = 0; i < lines.length; i++) {
        const line = lines[i]
        if (line.startsWith('event: ')) {
          const eventType = line.slice(7)
          const nextLine = lines[i + 1]
          if (nextLine?.startsWith('data: ')) {
            const data = nextLine.slice(6)
            i++ // Skip the data line we just processed
            switch (eventType) {
              case 'token':
                callbacks.onToken(data)
                break
              case 'status':
                try {
                  callbacks.onStatus(JSON.parse(data))
                } catch {
                  callbacks.onStatus({ status: data })
                }
                break
              case 'done':
                try {
                  callbacks.onDone(JSON.parse(data))
                } catch {
                  callbacks.onError('Invalid response')
                }
                break
              case 'error':
                callbacks.onError(data)
                break
            }
          }
        }
      }
    }
  } catch (err) {
    // Connection was interrupted mid-stream
    if (err instanceof TypeError || (err instanceof Error && err.message.includes('network'))) {
      callbacks.onError('Connection lost. Please try again.')
    } else {
      callbacks.onError(err instanceof Error ? err.message : 'Stream error')
    }
  }
}

// Backward compatibility alias
export async function generateSQLStream(
  prompt: string,
  currentQuery: string | undefined,
  history: HistoryMessage[] | undefined,
  callbacks: StreamCallbacks
): Promise<void> {
  return generateSqlStream(prompt, currentQuery, history, callbacks)
}

export interface ChatMessage {
  id?: string // Unique message ID for deduplication (optional for backward compat)
  role: 'user' | 'assistant'
  content: string
  // Environment this message was sent/received in
  env?: string
  // Workflow data (only present on assistant messages)
  workflowData?: ChatWorkflowData
  // SQL queries for history transmission (extracted from workflowData for backend)
  executedQueries?: string[]
  // Status for streaming persistence (only present during/after streaming)
  status?: 'streaming' | 'complete' | 'error'
  // Workflow ID for durable persistence (only present on streaming messages)
  workflowId?: string
}

// Generate a unique message ID
export function generateMessageId(): string {
  return crypto.randomUUID()
}

// Ensure a message has an ID (for migration of old data)
export function ensureMessageId(message: ChatMessage): ChatMessage {
  if (message.id) return message
  return { ...message, id: generateMessageId() }
}

export interface DataQuestion {
  question: string
  rationale: string
}

export interface GeneratedQuery {
  question: string
  sql: string
  explanation: string
}

export interface ExecutedQuery {
  question: string
  sql: string
  columns: string[]
  rows: unknown[][]
  count: number
  error?: string
}

// Processing step types for unified timeline
export interface ThinkingStep {
  type: 'thinking'
  id: string
  content: string
}

export interface SqlQueryStep {
  type: 'sql_query'
  id: string
  question: string
  sql: string
  status: 'running' | 'completed' | 'error'
  rows?: number
  columns?: string[]
  data?: unknown[][]
  error?: string
  env?: string
}

export interface CypherQueryStep {
  type: 'cypher_query'
  id: string
  question: string
  cypher: string
  status: 'running' | 'completed' | 'error'
  rows?: number
  columns?: string[]
  data?: unknown[][]
  nodes?: unknown[]
  edges?: unknown[]
  error?: string
  env?: string
}

export interface ReadDocsStep {
  type: 'read_docs'
  id: string
  page: string
  status: 'running' | 'completed' | 'error'
  content?: string
  error?: string
  env?: string
}

export interface SynthesizingStep {
  type: 'synthesizing'
  id: string
}

export type ProcessingStep = ThinkingStep | SqlQueryStep | CypherQueryStep | ReadDocsStep | SynthesizingStep

export interface ChatWorkflowData {
  dataQuestions: DataQuestion[]
  generatedQueries: GeneratedQuery[]
  executedQueries: ExecutedQuery[]
  followUpQuestions?: string[]
  // Processing timeline for unified display
  processingSteps?: ProcessingStep[]
}

// Server-side workflow step (matches WorkflowStep in Go)
export interface ServerWorkflowStep {
  id: string
  type: 'thinking' | 'sql_query' | 'cypher_query' | 'read_docs'
  // For thinking steps
  content?: string
  // For sql_query steps
  question?: string
  sql?: string
  status?: 'running' | 'completed' | 'error'
  columns?: string[]
  rows?: unknown[][]
  count?: number
  error?: string
  // For cypher_query steps
  cypher?: string
  nodes?: unknown[]
  edges?: unknown[]
  // For read_docs steps
  page?: string
  // Environment this step was executed in
  env?: string
}

export interface ChatResponse {
  answer: string
  dataQuestions?: DataQuestion[]
  generatedQueries?: GeneratedQuery[]
  executedQueries?: ExecutedQuery[]
  followUpQuestions?: string[]
  thinking_steps?: string[]  // Server sends snake_case (legacy)
  steps?: ServerWorkflowStep[]  // Unified steps in execution order
  error?: string
}

// Convert server workflow steps to client processing steps format
export function serverStepsToProcessingSteps(steps: ServerWorkflowStep[]): ProcessingStep[] {
  return steps.map(step => {
    switch (step.type) {
      case 'thinking':
        return {
          type: 'thinking' as const,
          id: step.id,
          content: step.content ?? '',
        }
      case 'sql_query':
        return {
          type: 'sql_query' as const,
          id: step.id,
          question: step.question ?? '',
          sql: step.sql ?? '',
          status: step.status ?? 'completed',
          rows: step.count,
          columns: step.columns,
          data: step.rows,
          error: step.error,
        }
      case 'cypher_query':
        return {
          type: 'cypher_query' as const,
          id: step.id,
          question: step.question ?? '',
          cypher: step.cypher ?? '',
          status: step.status ?? 'completed',
          rows: step.count,
          columns: step.columns,
          data: step.rows,
          nodes: step.nodes,
          edges: step.edges,
          error: step.error,
        }
      case 'read_docs':
        return {
          type: 'read_docs' as const,
          id: step.id,
          page: step.page ?? '',
          status: step.status ?? 'completed',
          content: step.content,
          error: step.error,
        }
    }
  })
}

export async function sendChatMessage(
  message: string,
  history: ChatMessage[],
  signal?: AbortSignal
): Promise<ChatResponse> {
  const res = await fetchWithRetry('/api/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message, history }),
    signal,
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || 'Failed to send message')
  }
  return res.json()
}

export interface ChatStreamCallbacks {
  // Processing events
  onThinking: (data: { id: string; content: string }) => void
  // SQL query events
  onSqlStarted: (data: { id: string; question: string; sql: string }) => void
  onSqlDone: (data: { id: string; question: string; sql: string; rows: number; error: string; env?: string }) => void
  // Cypher query events
  onCypherStarted?: (data: { id: string; question: string; cypher: string }) => void
  onCypherDone?: (data: { id: string; question: string; cypher: string; rows: number; error: string; env?: string }) => void
  // ReadDocs events
  onReadDocsStarted?: (data: { id: string; page: string }) => void
  onReadDocsDone?: (data: { id: string; page: string; content: string; error: string }) => void
  // Workflow events
  onWorkflowStarted?: (data: { workflow_id: string }) => void
  onSynthesizing?: () => void
  // Completion events
  onDone: (response: ChatResponse) => void
  onError: (error: string) => void
  onRetrying?: (attempt: number, maxAttempts: number) => void
}

// Stream retry configuration (separate from connection retry)
const STREAM_RETRY_CONFIG = {
  maxRetries: 2,  // Retry up to 2 times on mid-stream failure
  baseDelayMs: 1000,
  maxDelayMs: 3000,
}

export async function sendChatMessageStream(
  message: string,
  history: ChatMessage[],
  callbacks: ChatStreamCallbacks,
  signal: AbortSignal | undefined,
  sessionId: string
): Promise<void> {
  // Outer retry loop for mid-stream failures (e.g., deploy, pod restart)
  for (let streamAttempt = 0; streamAttempt <= STREAM_RETRY_CONFIG.maxRetries; streamAttempt++) {
    if (signal?.aborted) return

    // Notify UI of retry attempt (skip first attempt)
    if (streamAttempt > 0) {
      console.log(`[SSE] Retrying stream (attempt ${streamAttempt + 1}/${STREAM_RETRY_CONFIG.maxRetries + 1})`)
      callbacks.onRetrying?.(streamAttempt, STREAM_RETRY_CONFIG.maxRetries)
      const delay = Math.min(
        STREAM_RETRY_CONFIG.baseDelayMs * Math.pow(2, streamAttempt - 1) + Math.random() * 200,
        STREAM_RETRY_CONFIG.maxDelayMs
      )
      await sleep(delay)
    }

    // Inner retry loop for initial connection
    let res: Response | undefined
    for (let attempt = 0; attempt <= RETRY_CONFIG.maxRetries; attempt++) {
      if (signal?.aborted) return

      try {
        const chatBody: {
          message: string
          history: typeof history
          session_id: string
          anonymous_id?: string
        } = { message, history, session_id: sessionId }
        if (!getAuthToken()) {
          chatBody.anonymous_id = getAnonymousId()
        }
        res = await apiFetch('/api/chat/stream', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
          body: JSON.stringify(chatBody),
          signal,
        })

        if (res.ok) break
        if (res.status >= 400 && res.status < 500) break // Don't retry client errors

        if (!isRetryableError(null, res.status) || attempt === RETRY_CONFIG.maxRetries) break
      } catch (err) {
        if (err instanceof Error && err.name === 'AbortError') return
        if (!isRetryableError(err) || attempt === RETRY_CONFIG.maxRetries) {
          // On last stream attempt, show error
          if (streamAttempt === STREAM_RETRY_CONFIG.maxRetries) {
            callbacks.onError('Connection failed. Please check your network and try again.')
            return
          }
          // Otherwise, break to outer loop for stream retry
          break
        }
      }

      const delay = Math.min(
        RETRY_CONFIG.baseDelayMs * Math.pow(2, attempt) + Math.random() * 100,
        RETRY_CONFIG.maxDelayMs
      )
      await sleep(delay)
    }

    if (!res) {
      // Connection completely failed, try stream retry
      if (streamAttempt < STREAM_RETRY_CONFIG.maxRetries) continue
      callbacks.onError('Connection failed. Please check your network and try again.')
      return
    }

    if (!res.ok) {
      const text = await res.text()
      callbacks.onError(text || 'Failed to send message')
      return
    }

    const reader = res.body?.getReader()
    if (!reader) {
      callbacks.onError('Streaming not supported')
      return
    }

    const decoder = new TextDecoder()
    let buffer = ''
    let currentEvent = ''
    let streamCompleted = false  // Track if stream finished successfully

    const processLines = (lines: string[]) => {
      for (const line of lines) {
        if (line.startsWith('event: ')) {
          currentEvent = line.slice(7).trim()
          if (currentEvent === 'done') {
            console.log('[SSE] Got done event line, waiting for data line')
          }
        } else if (line.startsWith('data:') && currentEvent) {
          if (currentEvent === 'done') {
            console.log('[SSE] Got data line for done event, length:', line.length, 'preview:', line.slice(0, 100))
          }
          const data = line.startsWith('data: ') ? line.slice(6) : line.slice(5)
          if (!data.trim()) {
            continue
          }
          try {
            const parsed = JSON.parse(data)
            switch (currentEvent) {
              case 'thinking':
                callbacks.onThinking(parsed)
                break
              // SQL query events
              case 'sql_started':
                callbacks.onSqlStarted(parsed)
                break
              case 'sql_done':
                callbacks.onSqlDone(parsed)
                break
              // Cypher query events
              case 'cypher_started':
                callbacks.onCypherStarted?.(parsed)
                break
              case 'cypher_done':
                callbacks.onCypherDone?.(parsed)
                break
              // ReadDocs events
              case 'read_docs_started':
                callbacks.onReadDocsStarted?.(parsed)
                break
              case 'read_docs_done':
                callbacks.onReadDocsDone?.(parsed)
                break
              case 'workflow_started':
                callbacks.onWorkflowStarted?.(parsed)
                break
              case 'synthesizing':
                callbacks.onSynthesizing?.()
                break
              case 'status':
              case 'heartbeat':
                // Ignore legacy status events and heartbeats
                break
              case 'done':
                streamCompleted = true
                callbacks.onDone(parsed)
                break
              case 'error':
                streamCompleted = true
                callbacks.onError(parsed.error || 'Unknown error')
                break
            }
          } catch (e) {
            console.error('[SSE] Parse error for event', currentEvent, e, 'data:', data.slice(0, 200))
          }
          currentEvent = ''
        }
      }
    }

    try {
      while (true) {
        const { done, value } = await reader.read()
        if (done) {
          if (buffer.trim()) {
            console.log('[SSE] Stream ended, processing remaining buffer:', buffer.slice(0, 200))
            const lines = buffer.split('\n')
            processLines(lines)
          } else {
            console.log('[SSE] Stream ended, buffer empty')
          }
          break
        }

        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop() || ''
        processLines(lines)
      }
      console.log('[SSE] Stream processing complete, currentEvent:', currentEvent, 'streamCompleted:', streamCompleted)

      // If stream ended without done/error event, it was interrupted
      if (!streamCompleted) {
        console.log('[SSE] Stream ended unexpectedly without completion event')
        if (streamAttempt < STREAM_RETRY_CONFIG.maxRetries) continue
        callbacks.onError('Connection lost. Please try again.')
      }
      return  // Stream completed normally or with server error
    } catch (err) {
      if (err instanceof Error && err.name === 'AbortError') {
        return
      }
      // Connection was interrupted mid-stream
      const isNetworkError = err instanceof TypeError || (err instanceof Error && err.message.includes('network'))
      console.log('[SSE] Stream error:', err, 'isNetworkError:', isNetworkError, 'attempt:', streamAttempt)

      if (isNetworkError && streamAttempt < STREAM_RETRY_CONFIG.maxRetries) {
        // Retry the entire stream
        continue
      }

      // Final attempt failed
      if (isNetworkError) {
        callbacks.onError('Connection lost. Please try again.')
      } else {
        callbacks.onError(err instanceof Error ? err.message : 'Stream error')
      }
      return
    }
  }
}

export interface GenerateTitleResponse {
  title: string
  error?: string
}

export interface SessionQueryInfo {
  prompt: string
  sql: string
}

export async function generateSessionTitle(
  queries: SessionQueryInfo[],
  signal?: AbortSignal
): Promise<GenerateTitleResponse> {
  // Use the complete endpoint with a specific prompt to generate a title
  const queryDescriptions = queries.slice(0, 3).map((q, i) => {
    if (q.prompt) {
      return `${i + 1}. Question: "${q.prompt}"
   SQL: ${q.sql.slice(0, 200)}${q.sql.length > 200 ? '...' : ''}`
    } else {
      return `${i + 1}. SQL: ${q.sql.slice(0, 300)}${q.sql.length > 300 ? '...' : ''}`
    }
  }).join('\n\n')

  const message = `Generate a very brief title (2-4 words) in sentence case (only first word capitalized) for this data session based on these queries:

${queryDescriptions}

Examples: "Sales by region", "User signups", "Revenue trends", "Order analysis".

Respond with ONLY the title. No quotes, no explanation.`

  const res = await fetchWithRetry('/api/complete', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message }),
    signal,
  })

  if (!res.ok) {
    const text = await res.text()
    return { title: '', error: text || 'Failed to generate title' }
  }

  const data: { response: string; error?: string } = await res.json()
  if (data.error) {
    return { title: '', error: data.error }
  }

  // Clean up the response - remove quotes, trim, take first line
  const title = data.response
    .replace(/^["']|["']$/g, '')
    .split('\n')[0]
    .trim()
    .slice(0, 60)

  return { title }
}

export async function generateChatSessionTitle(
  messages: ChatMessage[],
  signal?: AbortSignal
): Promise<GenerateTitleResponse> {
  // Use the first few user messages to generate a title
  const userMessages = messages
    .filter(m => m.role === 'user')
    .slice(0, 3)
    .map((m, i) => `${i + 1}. "${m.content.slice(0, 200)}${m.content.length > 200 ? '...' : ''}"`)
    .join('\n')

  const message = `Generate a very brief title (2-4 words) in sentence case (only first word capitalized) for this chat conversation based on these user messages:

${userMessages}

Context: This is a data analytics chat for DoubleZero (abbreviated as "DZ"), a network of dedicated high-performance links delivering low-latency connectivity globally. "DZ" always means "DoubleZero".

Examples: "Sales analysis help", "Database questions", "Revenue report", "User data query", "DZ vs internet".

Respond with ONLY the title. No quotes, no explanation.`

  const res = await fetchWithRetry('/api/complete', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message }),
    signal,
  })

  if (!res.ok) {
    const text = await res.text()
    return { title: '', error: text || 'Failed to generate title' }
  }

  const data: { response: string; error?: string } = await res.json()
  if (data.error) {
    return { title: '', error: data.error }
  }

  // Clean up the response - remove quotes, trim, take first line
  const title = data.response
    .replace(/^["']|["']$/g, '')
    .split('\n')[0]
    .trim()
    .slice(0, 60)

  return { title }
}

// Visualization recommendation types
export interface VisualizationRecommendRequest {
  columns: string[]
  sampleRows: unknown[][]
  rowCount: number
  query: string
}

export interface VisualizationRecommendResponse {
  recommended: boolean
  chartType?: 'bar' | 'line' | 'pie' | 'area' | 'scatter'
  xAxis?: string
  yAxis?: string[]
  reasoning?: string
  error?: string
}

export async function recommendVisualization(
  request: VisualizationRecommendRequest,
  signal?: AbortSignal
): Promise<VisualizationRecommendResponse> {
  const res = await fetchWithRetry('/api/visualize/recommend', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
    signal,
  })

  if (!res.ok) {
    const text = await res.text()
    return { recommended: false, error: text || 'Failed to get recommendation' }
  }

  return res.json()
}

// Stats for landing page
export interface StatsResponse {
  validators_on_dz: number
  total_stake_sol: number
  stake_share_pct: number
  users: number
  devices: number
  links: number
  contributors: number
  metros: number
  bandwidth_bps: number
  user_inbound_bps: number
  fetched_at: string
  error?: string
}

export async function fetchStats(): Promise<StatsResponse> {
  const res = await fetchWithRetry('/api/stats')
  if (!res.ok) {
    throw new Error('Failed to fetch stats')
  }
  return res.json()
}

// Status/health types
export interface SystemHealth {
  database: boolean
  database_msg?: string
  last_ingested?: string
}

export interface NetworkSummary {
  validators_on_dz: number
  total_stake_sol: number
  stake_share_pct: number
  stake_share_delta: number
  users: number
  max_users: number
  devices: number
  links: number
  contributors: number
  metros: number
  facilities: number
  bandwidth_bps: number
  user_inbound_bps: number
  devices_by_status: Record<string, number>
  links_by_status: Record<string, number>
}

export interface LinkIssue {
  code: string
  link_type: string
  contributor: string
  side_z_contributor: string
  issue: string
  value: number
  threshold: number
  side_a_metro: string
  side_z_metro: string
  since: string
  is_down: boolean
  bandwidth_bps: number
}

export interface LinkMetric {
  pk: string
  code: string
  link_type: string
  contributor: string
  side_z_contributor: string
  bandwidth_bps: number
  in_bps: number
  out_bps: number
  utilization_in: number
  utilization_out: number
  side_a_metro: string
  side_z_metro: string
  link_topologies: string[]
  unicast_drained: boolean
}

export interface LinkHealth {
  total: number
  healthy: number
  degraded: number
  unhealthy: number
  down: number
  issues: LinkIssue[]
  high_util_links: LinkMetric[]
  top_util_links: LinkMetric[]
}

export interface PerformanceMetrics {
  avg_latency_us: number
  p95_latency_us: number
  min_latency_us: number
  max_latency_us: number
  avg_loss_percent: number
  avg_jitter_us: number
  total_in_bps: number
  total_out_bps: number
}

export interface InterfaceIssue {
  device_pk: string
  device_code: string
  device_type: string
  contributor: string
  metro: string
  interface_name: string
  interface_type?: string
  cyoa_type?: string
  link_pk?: string
  link_code?: string
  link_type?: string
  link_side?: string
  in_errors: number
  out_errors: number
  in_fcs_errors: number
  in_discards: number
  out_discards: number
  carrier_transitions: number
  first_seen: string
  last_seen: string
}

export interface InterfaceHealth {
  issues: InterfaceIssue[]
}

export interface InterfaceIssuesResponse {
  issues: InterfaceIssue[]
  time_range: string
}

export interface NonActivatedDevice {
  pk: string
  code: string
  device_type: string
  metro: string
  status: string
  since: string
}

export interface NonActivatedLink {
  pk: string
  code: string
  link_type: string
  side_a_metro: string
  side_z_metro: string
  status: string
  since: string
  active_incident_types?: string[]
  bandwidth_bps: number
  link_topologies: string[]
  unicast_drained: boolean
}

export interface ISISDeviceIssue {
  code: string
  device_type: string
  metro: string
  issue: string // "overload", "unreachable"
  since: string
}

export interface InfrastructureAlerts {
  devices: NonActivatedDevice[]
  links: NonActivatedLink[]
  isis_devices: ISISDeviceIssue[]
}

export interface DeviceUtilization {
  pk: string
  code: string
  device_type: string
  contributor: string
  metro: string
  current_users: number
  max_users: number
  utilization: number
}

export interface StatusResponse {
  status: 'healthy' | 'degraded' | 'unhealthy'
  timestamp: string
  system: SystemHealth
  network: NetworkSummary
  links: LinkHealth
  interfaces: InterfaceHealth
  alerts: InfrastructureAlerts
  performance: PerformanceMetrics
  top_device_util: DeviceUtilization[]
  error?: string
}

export async function fetchStatus(): Promise<StatusResponse> {
  const res = await fetchWithRetry('/api/status')
  if (!res.ok) {
    throw new Error('Failed to fetch status')
  }
  return res.json()
}

// Link history types for status timeline
export interface LinkHourStatus {
  hour: string
  status: 'healthy' | 'degraded' | 'unhealthy' | 'no_data' | 'disabled'
  collecting?: boolean
  drain_status?: string
  avg_latency_us: number
  avg_loss_pct: number
  samples: number
  // Per-side latency/loss metrics (direction: A→Z vs Z→A)
  side_a_latency_us?: number
  side_a_loss_pct?: number
  side_a_samples?: number
  side_z_latency_us?: number
  side_z_loss_pct?: number
  side_z_samples?: number
  // Per-side interface issues (errors, discards, carrier transitions)
  side_a_in_errors?: number
  side_a_out_errors?: number
  side_a_in_fcs_errors?: number
  side_a_in_discards?: number
  side_a_out_discards?: number
  side_a_carrier_transitions?: number
  side_z_in_errors?: number
  side_z_out_errors?: number
  side_z_in_fcs_errors?: number
  side_z_in_discards?: number
  side_z_out_discards?: number
  side_z_carrier_transitions?: number
  // Utilization (traffic rate / capacity)
  utilization_in_pct?: number
  utilization_out_pct?: number
  // Device-specific: no latency probes detected
  no_probes?: boolean
  // ISIS state
  isis_down?: boolean
}

export interface LinkHistory {
  pk: string
  code: string
  link_type: string
  contributor: string
  side_z_contributor: string
  side_a_metro: string
  side_z_metro: string
  side_a_device: string
  side_z_device: string
  bandwidth_bps: number
  committed_rtt_us: number
  is_down?: boolean
  drain_status?: string
  provisioning?: boolean
  hours: LinkHourStatus[]
  issue_reasons: string[] // "packet_loss", "high_latency", "no_data", "interface_errors", "fcs_errors", "discards", "carrier_transitions", "down"
}

export interface LinkHistoryResponse {
  links: LinkHistory[]
  time_range: string      // "24h", "3d", "7d"
  bucket_minutes: number  // Size of each bucket in minutes
  bucket_count: number    // Number of buckets
}

export async function fetchLinkHistory(timeRange?: string, buckets?: number, filter?: string): Promise<LinkHistoryResponse> {
  const params = new URLSearchParams()
  if (timeRange) params.set('range', timeRange)
  if (buckets) params.set('buckets', buckets.toString())
  if (filter) params.set('filter', filter)
  const url = `/api/status/link-history${params.toString() ? '?' + params.toString() : ''}`
  const res = await fetchWithRetry(url)
  if (!res.ok) {
    throw new Error('Failed to fetch link history')
  }
  return res.json()
}

// Single link history response
export interface SingleLinkHistoryResponse {
  pk: string
  code: string
  committed_rtt_us: number
  hours: LinkHourStatus[]
  time_range: string
  bucket_minutes: number
  bucket_count: number
}

export async function fetchSingleLinkHistory(linkPk: string, timeRange?: string, buckets?: number, startTime?: number, endTime?: number): Promise<SingleLinkHistoryResponse> {
  const params = new URLSearchParams()
  if (timeRange) params.set('range', timeRange)
  if (buckets) params.set('buckets', buckets.toString())
  if (startTime) params.set('start_time', startTime.toString())
  if (endTime) params.set('end_time', endTime.toString())
  const url = `/api/status/links/${encodeURIComponent(linkPk)}/history${params.toString() ? '?' + params.toString() : ''}`
  const res = await fetchWithRetry(url)
  if (!res.ok) {
    throw new Error('Failed to fetch link history')
  }
  return res.json()
}

// Device history types for status timeline
export interface DeviceHourStatus {
  hour: string
  status: 'healthy' | 'degraded' | 'unhealthy' | 'no_data' | 'disabled'
  collecting?: boolean
  drain_status?: string
  current_users: number
  max_users: number
  utilization_pct: number
  in_errors: number
  out_errors: number
  in_fcs_errors: number
  in_discards: number
  out_discards: number
  carrier_transitions: number
  no_probes?: boolean
  // ISIS state
  isis_overload?: boolean
  isis_unreachable?: boolean
}

export interface DeviceHistory {
  pk: string
  code: string
  device_type: string
  contributor: string
  metro: string
  max_users: number
  hours: DeviceHourStatus[]
  issue_reasons: string[] // "interface_errors", "high_utilization", "drained", "no_data"
}

export interface DeviceHistoryResponse {
  devices: DeviceHistory[]
  time_range: string      // "24h", "3d", "7d"
  bucket_minutes: number  // Size of each bucket in minutes
  bucket_count: number    // Number of buckets
}

export async function fetchDeviceHistory(timeRange?: string, buckets?: number, filter?: string): Promise<DeviceHistoryResponse> {
  const params = new URLSearchParams()
  if (timeRange) params.set('range', timeRange)
  if (buckets) params.set('buckets', buckets.toString())
  if (filter) params.set('filter', filter)
  const url = `/api/status/device-history${params.toString() ? '?' + params.toString() : ''}`
  const res = await fetchWithRetry(url)
  if (!res.ok) {
    throw new Error('Failed to fetch device history')
  }
  return res.json()
}

// Single device history response
export interface SingleDeviceHistoryResponse {
  pk: string
  code: string
  device_type: string
  contributor: string
  metro: string
  max_users: number
  hours: DeviceHourStatus[]
  issue_reasons: string[]
  time_range: string
  bucket_minutes: number
  bucket_count: number
}

export async function fetchSingleDeviceHistory(devicePk: string, timeRange?: string, buckets?: number): Promise<SingleDeviceHistoryResponse> {
  const params = new URLSearchParams()
  if (timeRange) params.set('range', timeRange)
  if (buckets) params.set('buckets', buckets.toString())
  const url = `/api/status/devices/${encodeURIComponent(devicePk)}/history${params.toString() ? '?' + params.toString() : ''}`
  const res = await fetchWithRetry(url)
  if (!res.ok) {
    throw new Error('Failed to fetch device history')
  }
  return res.json()
}

export async function fetchInterfaceIssues(timeRange?: string): Promise<InterfaceIssuesResponse> {
  const params = new URLSearchParams()
  if (timeRange) params.set('range', timeRange)
  const url = `/api/status/interface-issues${params.toString() ? '?' + params.toString() : ''}`
  const res = await fetchWithRetry(url)
  if (!res.ok) {
    throw new Error('Failed to fetch interface issues')
  }
  return res.json()
}

// Device interface history types
export interface InterfaceHourStatus {
  hour: string
  in_errors: number
  out_errors: number
  in_fcs_errors: number
  in_discards: number
  out_discards: number
  carrier_transitions: number
}

export interface InterfaceHistory {
  interface_name: string
  link_pk?: string
  link_code?: string
  link_type?: string
  link_side?: string
  hours: InterfaceHourStatus[]
}

export interface DeviceInterfaceHistoryResponse {
  interfaces: InterfaceHistory[]
  time_range: string
  bucket_minutes: number
  bucket_count: number
}

export async function fetchDeviceInterfaceHistory(devicePk: string, timeRange?: string, buckets?: number): Promise<DeviceInterfaceHistoryResponse> {
  const params = new URLSearchParams()
  if (timeRange) params.set('range', timeRange)
  if (buckets) params.set('buckets', buckets.toString())
  const url = `/api/status/devices/${devicePk}/interface-history${params.toString() ? '?' + params.toString() : ''}`
  const res = await fetchWithRetry(url)
  if (!res.ok) {
    throw new Error('Failed to fetch device interface history')
  }
  return res.json()
}

// Topology types
export interface TopologyMetro {
  pk: string
  code: string
  name: string
  latitude: number
  longitude: number
}

export interface DeviceInterface {
  name: string
  ip: string
  status: string
  interface_type?: string
  cyoa_type?: string
  dia_type?: string
  loopback_type?: string
  routing_mode?: string
  bandwidth?: number
  cir?: number
  mtu?: number
  vlan_id?: number
  node_segment_idx?: number
  user_tunnel_endpoint?: boolean
}

export interface TopologyDevice {
  pk: string
  code: string
  status: string
  device_type: string
  metro_pk: string
  contributor_pk: string
  contributor_code: string
  user_count: number
  unicast_users_count: number
  multicast_subscribers_count: number
  multicast_publishers_count: number
  max_unicast_users: number
  max_multicast_subscribers: number
  max_multicast_publishers: number
  validator_count: number
  stake_sol: number
  stake_share: number
  interfaces: DeviceInterface[]
}

export interface TopologyLink {
  pk: string
  code: string
  status: string
  link_type: string
  bandwidth_bps: number
  side_a_pk: string
  side_a_code: string
  side_a_iface_name: string
  side_a_ip: string
  side_z_pk: string
  side_z_code: string
  side_z_iface_name: string
  side_z_ip: string
  contributor_pk: string
  contributor_code: string
  side_a_contributor_pk: string
  side_a_contributor_code: string
  side_z_contributor_pk: string
  side_z_contributor_code: string
  latency_us: number
  jitter_us: number
  latency_a_to_z_us: number
  jitter_a_to_z_us: number
  latency_z_to_a_us: number
  jitter_z_to_a_us: number
  loss_percent: number
  sample_count: number
  in_bps: number
  out_bps: number
  committed_rtt_ns: number
  isis_delay_override_ns: number
  link_topologies?: string[]
  unicast_drained?: boolean
}

export interface TopologyValidator {
  vote_pubkey: string
  node_pubkey: string
  device_pk: string
  tunnel_id: number
  latitude: number
  longitude: number
  city: string
  country: string
  stake_sol: number
  stake_share: number
  commission: number
  version: string
  gossip_ip: string
  gossip_port: number
  tpu_quic_ip: string
  tpu_quic_port: number
  in_bps: number
  out_bps: number
}

export interface TopologyResponse {
  metros: TopologyMetro[]
  devices: TopologyDevice[]
  links: TopologyLink[]
  validators: TopologyValidator[]
  error?: string
}

export async function fetchTopology(): Promise<TopologyResponse> {
  const res = await fetchWithRetry('/api/topology')
  if (!res.ok) {
    throw new Error('Failed to fetch topology')
  }
  return res.json()
}

export interface TopologyInfo {
  pk: string
  name: string
  admin_group_bit: number
  flex_algo_number: number
  color: number
  constraint: string
  link_count: number
}

export interface TopologiesResponse {
  topologies: TopologyInfo[]
  total_link_count: number
  drained_link_count: number
  untagged_link_count: number
}

export async function fetchTopologies(): Promise<TopologiesResponse> {
  const res = await fetchWithRetry('/api/topologies')
  if (!res.ok) {
    throw new Error('Failed to fetch topologies')
  }
  return res.json()
}

export interface TopologyLinkMetricsEntry {
  latency_us: number
  jitter_us: number
  latency_a_to_z_us: number
  jitter_a_to_z_us: number
  latency_z_to_a_us: number
  jitter_z_to_a_us: number
  loss_percent: number
  sample_count: number
  in_bps: number
  out_bps: number
}

export interface TopologyLinkMetricsResponse {
  metrics: Record<string, TopologyLinkMetricsEntry>
  error?: string
}

export async function fetchTopologyLinkMetrics(): Promise<TopologyLinkMetricsResponse> {
  const res = await fetchWithRetry('/api/topology/link-metrics')
  if (!res.ok) {
    throw new Error('Failed to fetch topology link metrics')
  }
  return res.json()
}

export interface TopologyValidatorsResponse {
  validators: TopologyValidator[]
  error?: string
}

export async function fetchTopologyValidators(): Promise<TopologyValidatorsResponse> {
  const res = await fetchWithRetry('/api/topology/validators')
  if (!res.ok) {
    throw new Error('Failed to fetch topology validators')
  }
  return res.json()
}

// ISIS Topology types (graph view)
export interface ISISNodeData {
  id: string
  label: string
  status: string
  deviceType: string
  metroPK?: string
  systemId?: string
  routerId?: string
}

export interface ISISNode {
  data: ISISNodeData
}

export interface ISISEdgeData {
  id: string
  source: string
  target: string
  metric?: number
  adjSids?: number[]
  neighborAddr?: string
}

export interface ISISEdge {
  data: ISISEdgeData
}

export interface ISISTopologyResponse {
  nodes: ISISNode[]
  edges: ISISEdge[]
  error?: string
}

export async function fetchISISTopology(): Promise<ISISTopologyResponse> {
  const res = await fetchWithRetry('/api/topology/isis')
  if (!res.ok) {
    throw new Error('Failed to fetch ISIS topology')
  }
  return res.json()
}

// Path finding types
export interface PathHop {
  devicePK: string
  deviceCode: string
  status: string
  deviceType: string
}

export interface PathResponse {
  path: PathHop[]
  totalMetric: number
  hopCount: number
  error?: string
}

export async function fetchISISPath(fromPK: string, toPK: string): Promise<PathResponse> {
  const res = await apiFetch(`/api/topology/path?from=${encodeURIComponent(fromPK)}&to=${encodeURIComponent(toPK)}`)
  if (!res.ok) {
    throw new Error('Failed to fetch path')
  }
  return res.json()
}

// Multi-path types
export interface MultiPathHop {
  devicePK: string
  deviceCode: string
  status: string
  deviceType: string
  metroPK?: string
  metroCode?: string
  edgeMetric?: number
  edgeMeasuredMs?: number   // measured RTT in ms to reach this hop
  edgeJitterMs?: number     // measured jitter in ms
  edgeLossPct?: number      // packet loss percentage
  edgeSampleCount?: number  // number of samples for confidence
}

export interface SinglePath {
  path: MultiPathHop[]
  totalMetric: number
  hopCount: number
  measuredLatencyMs?: number  // sum of measured RTT along path
  totalSamples?: number       // min samples across hops
}

// PathService selects which IS-IS topology a path is resolved through:
// 'unicast' constrains to flex-algo 128 (topology-tagged links); 'multicast' uses algo 0 (all links).
export type PathService = 'unicast' | 'multicast'

export interface MultiPathResponse {
  paths: SinglePath[]
  from: string
  to: string
  error?: string
}

export async function fetchISISPaths(fromPK: string, toPK: string, k: number = 5, service?: PathService): Promise<MultiPathResponse> {
  let url = `/api/topology/paths?from=${encodeURIComponent(fromPK)}&to=${encodeURIComponent(toPK)}&k=${k}`
  if (service) {
    url += `&service=${encodeURIComponent(service)}`
  }
  const res = await apiFetch(url)
  if (!res.ok) {
    throw new Error('Failed to fetch paths')
  }
  return res.json()
}

// Metro device paths types
export interface MetroDevicePairPath {
  sourceDevicePK: string
  sourceDeviceCode: string
  targetDevicePK: string
  targetDeviceCode: string
  bestPath: SinglePath
}

export interface MetroDevicePathsResponse {
  fromMetroPK: string
  fromMetroCode: string
  toMetroPK: string
  toMetroCode: string

  // Aggregate summary
  sourceDeviceCount: number
  targetDeviceCount: number
  totalPairs: number
  minHops: number
  maxHops: number
  minLatencyMs: number
  maxLatencyMs: number
  avgLatencyMs: number

  // All device pairs with their best path
  devicePairs: MetroDevicePairPath[]

  error?: string
}

export async function fetchMetroDevicePaths(
  fromMetroPK: string,
  toMetroPK: string,
  service?: PathService,
): Promise<MetroDevicePathsResponse> {
  let url = `/api/topology/metro-device-paths?from=${encodeURIComponent(fromMetroPK)}&to=${encodeURIComponent(toMetroPK)}`
  if (service) {
    url += `&service=${encodeURIComponent(service)}`
  }
  const res = await apiFetch(url)
  if (!res.ok) {
    throw new Error('Failed to fetch metro device paths')
  }
  return res.json()
}

// Multicast group types
export interface MulticastGroupListItem {
  pk: string
  code: string
  multicast_ip: string
  max_bandwidth: number
  status: string
  publisher_count: number
  subscriber_count: number
}

export interface MulticastMember {
  user_pk: string
  mode: 'P' | 'S' | 'P+S'
  device_pk: string
  device_code: string
  metro_pk: string
  metro_code: string
  metro_name: string
  client_ip: string
  dz_ip: string
  status: string
  owner_pubkey: string
  tunnel_id: number
  traffic_bps: number
  traffic_pps: number
  is_leader: boolean
  node_pubkey: string
  vote_pubkey: string
  stake_sol: number
  last_leader_slot: number | null
  next_leader_slot: number | null
  current_slot: number
}

export interface MulticastGroupMetadata extends MulticastGroupListItem {
  has_shred_stats: boolean
}

export interface MulticastGroupDetail extends MulticastGroupMetadata {
  members: MulticastMember[]
}

export interface MulticastMembersResponse extends PaginatedResponse<MulticastMember> {
  publisher_count: number
  subscriber_count: number
}

export async function fetchMulticastGroups(
  limit = 100,
  offset = 0,
  sortBy?: string,
  sortDir?: 'asc' | 'desc',
  filters?: string[]
): Promise<PaginatedResponse<MulticastGroupListItem>> {
  const params = new URLSearchParams()
  params.set('limit', String(limit))
  params.set('offset', String(offset))
  if (sortBy) params.set('sort_by', sortBy)
  if (sortDir) params.set('sort_dir', sortDir)
  if (filters) filters.forEach(f => params.append('filters', f))
  const res = await apiFetch(`/api/dz/multicast-groups?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch multicast groups')
  }
  return res.json()
}

export async function fetchMulticastGroupMetadata(pkOrCode: string): Promise<MulticastGroupMetadata> {
  const res = await apiFetch(`/api/dz/multicast-groups/${encodeURIComponent(pkOrCode)}`)
  if (!res.ok) {
    throw new Error('Failed to fetch multicast group')
  }
  const group = await res.json() as MulticastGroupListItem & { has_shred_stats?: boolean }
  return { ...group, has_shred_stats: group.has_shred_stats ?? false }
}

export async function fetchMulticastGroupMemberSnapshot(
  pkOrCode: string,
  limit = 1000,
): Promise<MulticastMember[]> {
  const encoded = encodeURIComponent(pkOrCode)
  const [pubRes, subRes] = await Promise.all([
    apiFetch(`/api/dz/multicast-groups/${encoded}/members?tab=publishers&limit=${limit}`),
    apiFetch(`/api/dz/multicast-groups/${encoded}/members?tab=subscribers&limit=${limit}`),
  ])
  let members: MulticastMember[] = []
  if (pubRes.ok) {
    const pubData = await pubRes.json() as MulticastMembersResponse
    members = pubData.items
  }
  if (subRes.ok) {
    const subData = await subRes.json() as MulticastMembersResponse
    // Add subscribers that aren't already in the list (P+S members appear in both)
    const existingPKs = new Set(members.map(m => m.user_pk))
    for (const m of subData.items) {
      if (!existingPKs.has(m.user_pk)) {
        members.push(m)
      }
    }
  }
  return members
}

export async function fetchMulticastGroup(pkOrCode: string): Promise<MulticastGroupDetail> {
  const [group, members] = await Promise.all([
    fetchMulticastGroupMetadata(pkOrCode),
    fetchMulticastGroupMemberSnapshot(pkOrCode),
  ])
  return { ...group, members }
}

export async function fetchMulticastGroupMembers(
  pkOrCode: string,
  limit = 100,
  offset = 0,
  sortBy?: string,
  sortDir?: 'asc' | 'desc',
  tab?: string,
  filters?: string[]
): Promise<MulticastMembersResponse> {
  const params = new URLSearchParams({ limit: String(limit), offset: String(offset) })
  if (sortBy) params.set('sort_by', sortBy)
  if (sortDir) params.set('sort_dir', sortDir)
  if (tab) params.set('tab', tab)
  if (filters) {
    for (const f of filters) params.append('filters', f)
  }
  const res = await apiFetch(`/api/dz/multicast-groups/${encodeURIComponent(pkOrCode)}/members?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch multicast group members')
  }
  return res.json()
}

// Multicast tree path types
export interface MulticastTreeHop {
  devicePK: string
  deviceCode: string
  deviceType: string
  edgeMetric?: number
}

export interface MulticastTreePath {
  publisherDevicePK: string
  publisherDeviceCode: string
  subscriberDevicePK: string
  subscriberDeviceCode: string
  path: MulticastTreeHop[]
  totalMetric: number
  hopCount: number
}

export interface MulticastTreeResponse {
  groupCode: string
  groupPK: string
  publisherCount: number
  subscriberCount: number
  paths: MulticastTreePath[]
  error?: string
}

export async function fetchMulticastTreePaths(pkOrCode: string, publisherDevicePKs?: string[]): Promise<MulticastTreeResponse> {
  const params = new URLSearchParams()
  if (publisherDevicePKs && publisherDevicePKs.length > 0) {
    params.set('publishers', publisherDevicePKs.join(','))
  }
  const qs = params.toString()
  const res = await apiFetch(`/api/dz/multicast-groups/${encodeURIComponent(pkOrCode)}/tree-paths${qs ? `?${qs}` : ''}`)
  if (!res.ok) {
    throw new Error('Failed to fetch multicast tree paths')
  }
  return res.json()
}

// Multicast tree segment types (aggregated server-side)
export interface MulticastAggSegment {
  fromPK: string
  toPK: string
  publisherPKs: string[]
}

export interface MulticastTreeSegmentsResponse {
  groupCode: string
  groupPK: string
  publisherCount: number
  subscriberCount: number
  segments: MulticastAggSegment[]
  error?: string
}

export async function fetchMulticastTreeSegments(pkOrCode: string, publisherDevicePKs?: string[]): Promise<MulticastTreeSegmentsResponse> {
  const params = new URLSearchParams()
  if (publisherDevicePKs && publisherDevicePKs.length > 0) {
    params.set('publishers', publisherDevicePKs.join(','))
  }
  const qs = params.toString()
  const res = await apiFetch(`/api/dz/multicast-groups/${encodeURIComponent(pkOrCode)}/tree-segments${qs ? `?${qs}` : ''}`)
  if (!res.ok) {
    throw new Error('Failed to fetch multicast tree segments')
  }
  return res.json()
}

// Multicast group traffic types
export interface MulticastTrafficPoint {
  time: string
  device_pk: string
  tunnel_id: number
  mode: string
  in_bps: number
  out_bps: number
  in_pps: number
  out_pps: number
}

export async function fetchMulticastGroupTraffic(pkOrCode: string, timeRange?: string, bucket?: string): Promise<MulticastTrafficPoint[]> {
  const params = new URLSearchParams()
  if (timeRange) params.set('time_range', timeRange)
  if (bucket && bucket !== 'auto') params.set('bucket', bucket)
  const qs = params.toString()
  const res = await apiFetch(`/api/dz/multicast-groups/${encodeURIComponent(pkOrCode)}/traffic${qs ? `?${qs}` : ''}`)
  if (!res.ok) {
    throw new Error('Failed to fetch multicast group traffic')
  }
  return res.json()
}

export interface MulticastMemberCountPoint {
  time: string
  publisher_count: number
  subscriber_count: number
}

export async function fetchMulticastGroupMemberCounts(pkOrCode: string, timeRange?: string): Promise<MulticastMemberCountPoint[]> {
  const params = new URLSearchParams()
  if (timeRange) params.set('time_range', timeRange)
  const qs = params.toString()
  const res = await apiFetch(`/api/dz/multicast-groups/${encodeURIComponent(pkOrCode)}/member-counts${qs ? `?${qs}` : ''}`)
  if (!res.ok) {
    throw new Error('Failed to fetch multicast group member counts')
  }
  return res.json()
}

export interface ShredStatsPoint {
  time: string
  dz_user_pubkey: string
  unique_shreds: number
  total_packets: number
  data_shreds: number
  coding_shreds: number
  slots: number
  leader_slots: number
  repair_slots: number
  [key: string]: string | number
}

export async function fetchMulticastGroupShredStats(pkOrCode: string, timeRange?: string, bucket?: string): Promise<ShredStatsPoint[]> {
  const params = new URLSearchParams()
  if (timeRange) params.set('time_range', timeRange)
  if (bucket && bucket !== 'auto') params.set('bucket', bucket)
  const qs = params.toString()
  const res = await apiFetch(`/api/dz/multicast-groups/${encodeURIComponent(pkOrCode)}/shred-stats${qs ? `?${qs}` : ''}`)
  if (!res.ok) {
    throw new Error('Failed to fetch multicast group shred stats')
  }
  return res.json()
}

// Multicast health (onchain ↔ dataplane reconciliation) types

export type MulticastHealthStatus = 'healthy' | 'degraded' | 'unhealthy' | 'disconnected' | 'unknown'

export interface MulticastHealthStatusCounts {
  healthy: number
  degraded: number
  unhealthy: number
  disconnected: number
  unknown: number
  total: number
}

export interface MulticastHealthCounts {
  mroutes: MulticastHealthStatusCounts
  users: MulticastHealthStatusCounts
  paths: MulticastHealthStatusCounts
}

export interface MulticastHealthGroupRef {
  pk: string
  code: string
  multicast_ip: string
  status: string
  publisher_count: number
  subscriber_count: number
}

export interface MulticastHealthGroupSummary {
  group: MulticastHealthGroupRef
  source_available: boolean
  generated_at: string
  counts: MulticastHealthCounts
}

// Rate is a presence-only signal now (see the health_rate_presence_only
// migration): 'active' = non-zero traffic observed on the tunnel, 'idle' =
// registered but 0 bps, 'unknown' = no counter data. It never gates
// health_status — that comes from the control plane.
export type MulticastRateStatus = 'active' | 'idle' | 'unknown'

export type MulticastRateStatusReason = 'active' | 'idle' | 'no_data'

export interface MulticastHealthUserItem {
  user_pk: string
  user_owner_pubkey: string
  user_dz_ip: string
  user_tunnel_id: number
  user_device_pk: string
  user_device_code: string
  multicast_group_pk: string
  multicast_group_code: string
  group_address: string
  mode: 'P' | 'S' | 'P+S' | string
  expected_tunnel_position: string
  publisher_iif_observed: boolean
  subscriber_oif_observed: boolean
  reconciled: boolean
  // Combined (CP × rate) verdict. The CP-only verdict lives on
  // control_plane_status, the rate-only verdict on rate_status.
  health_status: MulticastHealthStatus
  control_plane_status: MulticastHealthStatus
  rate_status: MulticastRateStatus
  rate_status_reason: MulticastRateStatusReason
  rate_bucket_ts?: string
  observed_bps_5m?: number
  expected_bps_5m?: number
  mismatch_reason?: string
}

export interface MulticastHealthGroupUsersResponse {
  group: MulticastHealthGroupRef
  generated_at: string
  items: MulticastHealthUserItem[]
  total: number
  limit: number
  offset: number
}

export interface MulticastHealthUserResponse {
  user_pk: string
  user_owner_pubkey: string
  user_dz_ip: string
  user_tunnel_id: number
  user_device_pk: string
  user_device_code: string
  generated_at: string
  items: MulticastHealthUserItem[]
}

export interface MulticastHealthPathItem {
  multicast_group_pk?: string
  multicast_group_code?: string
  group_address?: string
  publisher_user_pk: string
  publisher_owner_pubkey: string
  publisher_dz_ip: string
  publisher_tunnel_id: number
  publisher_device_pk: string
  publisher_device_code: string
  subscriber_user_pk: string
  subscriber_owner_pubkey: string
  subscriber_dz_ip: string
  subscriber_tunnel_id: number
  subscriber_device_pk: string
  subscriber_device_code: string
  publisher_endpoint_observed: boolean
  subscriber_endpoint_observed: boolean
  endpoints_reconciled: boolean
  health_status: MulticastHealthStatus
  verification_method: 'endpoints_only' | 'full_path' | string
  missing_endpoint_reasons?: string[]
}

export interface MulticastHealthGroupPathsResponse {
  group: MulticastHealthGroupRef
  generated_at: string
  items: MulticastHealthPathItem[]
  total: number
  limit: number
  offset: number
}

// One faulting endpoint behind the unhealthy per-path fan-out. affected_pairs
// is how many (publisher, subscriber) pairs this endpoint drags down. A user
// broken as both publisher and subscriber of the group reports the combined
// 'publisher+subscriber' role.
export interface MulticastHealthPathRootCause {
  faulting_role: 'publisher' | 'subscriber' | 'publisher+subscriber'
  user_pk: string
  owner_pubkey: string
  dz_ip: string
  tunnel_id: number
  device_pk: string
  device_code: string
  endpoint_status: 'disconnected' | 'unhealthy'
  affected_pairs: number
}

export interface MulticastHealthPathRootCausesResponse {
  group: MulticastHealthGroupRef
  generated_at: string
  items: MulticastHealthPathRootCause[]
  total: number
}

export async function fetchMulticastGroupHealth(pkOrCode: string): Promise<MulticastHealthGroupSummary> {
  const res = await apiFetch(`/api/dz/multicast-groups/${encodeURIComponent(pkOrCode)}/health`)
  if (!res.ok) {
    throw new Error('Failed to fetch multicast group health')
  }
  return res.json()
}

export async function fetchMulticastGroupHealthUsers(
  pkOrCode: string,
  limit?: number,
  offset?: number,
  search?: string,
): Promise<MulticastHealthGroupUsersResponse> {
  const params = new URLSearchParams()
  if (limit !== undefined) params.set('limit', String(limit))
  if (offset !== undefined) params.set('offset', String(offset))
  if (search) params.set('search', search)
  const qs = params.toString()
  const res = await apiFetch(`/api/dz/multicast-groups/${encodeURIComponent(pkOrCode)}/health/users${qs ? `?${qs}` : ''}`)
  if (!res.ok) {
    throw new Error('Failed to fetch multicast group health users')
  }
  return res.json()
}

export async function fetchMulticastGroupHealthPaths(
  pkOrCode: string,
  limit?: number,
  offset?: number,
  search?: string,
): Promise<MulticastHealthGroupPathsResponse> {
  const params = new URLSearchParams()
  if (limit !== undefined) params.set('limit', String(limit))
  if (offset !== undefined) params.set('offset', String(offset))
  if (search) params.set('search', search)
  const qs = params.toString()
  const res = await apiFetch(`/api/dz/multicast-groups/${encodeURIComponent(pkOrCode)}/health/paths${qs ? `?${qs}` : ''}`)
  if (!res.ok) {
    throw new Error('Failed to fetch multicast group health paths')
  }
  return res.json()
}

export async function fetchMulticastGroupHealthPathRootCauses(
  pkOrCode: string,
): Promise<MulticastHealthPathRootCausesResponse> {
  const res = await apiFetch(`/api/dz/multicast-groups/${encodeURIComponent(pkOrCode)}/health/path-root-causes`)
  if (!res.ok) {
    throw new Error('Failed to fetch multicast group path root causes')
  }
  return res.json()
}

export async function fetchUserHealth(pk: string): Promise<MulticastHealthUserResponse> {
  const res = await apiFetch(`/api/dz/users/${encodeURIComponent(pk)}/health`)
  if (!res.ok) {
    throw new Error('Failed to fetch user health')
  }
  return res.json()
}

export type MulticastDeliveryFreshnessStatus = 'fresh' | 'stale' | 'missing' | 'unavailable'

export interface MulticastDeliverySourceFreshness {
  available: boolean
  status: MulticastDeliveryFreshnessStatus | string
  row_count: number
  latest_snapshot_ts?: string
  latest_ingested_at?: string
  age_seconds?: number
  note?: string
}

export interface MulticastDeliveryFreshness {
  mroute: MulticastDeliverySourceFreshness
  msdp_peers: MulticastDeliverySourceFreshness
  msdp_pim_sa_cache: MulticastDeliverySourceFreshness
  msdp_sa_cache: MulticastDeliverySourceFreshness
  pim_neighbors: MulticastDeliverySourceFreshness
}

export interface MulticastEntityHealthStatusCounts {
  healthy: number
  degraded: number
  unhealthy: number
  disconnected: number
  unknown: number
  total: number
}

export interface MulticastDeliveryEntityGroup {
  group_pk: string
  group_code: string
  group_address: string
  source_count: number
  mroute_count: number
  oif_count: number
  health_counts: MulticastEntityHealthStatusCounts
}

export interface MulticastDeliveryRole {
  role: string
  label: string
  description: string
  group_count: number
  source_count: number
  mroute_count: number
  oif_count: number
  health_counts: MulticastEntityHealthStatusCounts
}

export interface MulticastDeliveryDirection {
  direction: 'a_to_z' | 'z_to_a' | 'unknown' | string
  label: string
  group_count: number
  source_count: number
  branch_count: number
}

export interface MulticastDeliveryMroute {
  mroute_id: string
  entity_id: string
  snapshot_ts: string
  ingested_at: string
  age_seconds: number
  freshness_status: string
  device_pk: string
  device_code: string
  device_status: string
  device_type: string
  metro_code: string
  contributor_code: string
  vrf: string
  mode: string
  group_address: string
  multicast_group_pk?: string
  multicast_group_code?: string
  source_address: string
  route_flags: string
  register_in_oif_list: boolean
  rpf_interface: string
  rpf_rib: string
  rpf_prefix: string
  rpf_preference: number
  rpf_metric: number
  rpf_neighbor: string
  rpf_attached: boolean
  rpf_has_block: boolean
  oif_list: string
  oif_count: number
  creation_time: string
  publisher_user_pk: string
  publisher_device_pk: string
  publisher_device_code: string
  publisher_metro_code: string
  publisher_contributor_code: string
  publisher_tunnel_id: number
  publisher_owner_pubkey: string
  publisher_dz_ip: string
  source_match_status: string
}

export interface MulticastDeliveryOIF {
  mroute_id: string
  entity_id: string
  snapshot_ts: string
  age_seconds: number
  freshness_status: string
  device_pk: string
  device_code: string
  group_address: string
  multicast_group_pk?: string
  multicast_group_code?: string
  source_address: string
  publisher_user_pk: string
  publisher_device_pk: string
  publisher_device_code?: string
  oif_name: string
  oif_kind: string
  observed_delivery_role: string
  link_pk?: string
  link_code?: string
  link_side?: string
  peer_device_pk?: string
  peer_device_code?: string
  peer_interface_name?: string
  link_type?: string
  bandwidth_bps?: number
  link_topologies?: string
  unicast_drained?: boolean
  interface_type?: string
  routing_mode?: string
  interface_bandwidth?: number
  interface_mtu?: number
  user_tunnel_endpoint?: boolean
  subscriber_user_pk?: string
  subscriber_device_pk?: string
  subscriber_device_code?: string
  subscriber_tunnel_id?: number
  subscriber_owner_pubkey?: string
  subscriber_dz_ip?: string
  subscriber_client_ip?: string
}

export interface MulticastDeliveryLinkBranch extends MulticastDeliveryOIF {
  direction: 'a_to_z' | 'z_to_a' | 'unknown' | string
}

export interface MulticastDeliveryAnomaly {
  id: string
  severity: string
  kind: string
  scope: string
  object_ids: Record<string, string>
  message: string
}

export interface DeviceMulticastDeliverySummary {
  group_count: number
  source_count: number
  mroute_count: number
  routes_with_oifs: number
  oif_count: number
  underlay_oif_count: number
  subscriber_tunnel_oif_count: number
  msdp_peer_count: number
  msdp_sa_count: number
  user_health_counts: MulticastEntityHealthStatusCounts
  endpoint_health_counts: MulticastEntityHealthStatusCounts
  anomaly_count: number
}

export interface LinkMulticastDeliverySummary {
  group_count: number
  source_count: number
  branch_count: number
  a_to_z_count: number
  z_to_a_count: number
  unknown_direction_count: number
  reporting_device_count: number
  related_group_health_counts: MulticastEntityHealthStatusCounts
  anomaly_count: number
}

export interface DeviceMulticastDeliveryResponse {
  device: Pick<Device, 'pk' | 'code' | 'status' | 'device_type' | 'contributor_pk' | 'contributor_code' | 'metro_pk' | 'metro_code'>
  source_available: boolean
  generated_at: string
  freshness: MulticastDeliveryFreshness
  coverage_note: string
  health_context_note: string
  summary: DeviceMulticastDeliverySummary
  groups: MulticastDeliveryEntityGroup[]
  roles: MulticastDeliveryRole[]
  health_users: MulticastHealthUserItem[]
  health_user_total: number
  endpoint_health_items: MulticastHealthPathItem[]
  endpoint_health_total: number
  endpoint_limit: number
  endpoint_offset: number
  routes: MulticastDeliveryMroute[]
  oifs: MulticastDeliveryOIF[]
  route_total: number
  oif_total: number
  limit: number
  offset: number
  anomalies: MulticastDeliveryAnomaly[]
}

export interface LinkMulticastDeliveryResponse {
  link: Pick<Link, 'pk' | 'code' | 'status' | 'link_type' | 'side_a_pk' | 'side_a_code' | 'side_a_iface_name' | 'side_z_pk' | 'side_z_code' | 'side_z_iface_name' | 'contributor_pk' | 'contributor_code'>
  source_available: boolean
  generated_at: string
  freshness: MulticastDeliveryFreshness
  coverage_note: string
  health_context_note: string
  summary: LinkMulticastDeliverySummary
  groups: MulticastDeliveryEntityGroup[]
  branches: MulticastDeliveryLinkBranch[]
  directions: MulticastDeliveryDirection[]
  branch_total: number
  limit: number
  offset: number
  anomalies: MulticastDeliveryAnomaly[]
}

export interface FetchMulticastDeliveryParams {
  limit?: number
  offset?: number
  endpointLimit?: number
  endpointOffset?: number
  group?: string
  source?: string
  endpointIp?: string
  health?: MulticastHealthStatus
  oifKind?: string
  role?: string
  direction?: 'a_to_z' | 'z_to_a' | 'unknown'
}

function multicastDeliverySearchParams(params: FetchMulticastDeliveryParams = {}): URLSearchParams {
  const search = new URLSearchParams()
  if (params.limit !== undefined) search.set('limit', String(params.limit))
  if (params.offset !== undefined) search.set('offset', String(params.offset))
  if (params.endpointLimit !== undefined) search.set('endpoint_limit', String(params.endpointLimit))
  if (params.endpointOffset !== undefined) search.set('endpoint_offset', String(params.endpointOffset))
  if (params.group) search.set('group', params.group)
  if (params.source) search.set('source', params.source)
  if (params.endpointIp) search.set('endpoint_ip', params.endpointIp)
  if (params.health) search.set('health', params.health)
  if (params.oifKind) search.set('oif_kind', params.oifKind)
  if (params.role) search.set('role', params.role)
  if (params.direction) search.set('direction', params.direction)
  return search
}

export async function fetchDeviceMulticastDelivery(
  pk: string,
  params: FetchMulticastDeliveryParams = {},
): Promise<DeviceMulticastDeliveryResponse> {
  const search = multicastDeliverySearchParams(params)
  const qs = search.toString()
  const res = await apiFetch(`/api/dz/devices/${encodeURIComponent(pk)}/multicast-delivery${qs ? `?${qs}` : ''}`)
  if (!res.ok) {
    throw new Error('Failed to fetch device multicast delivery')
  }
  return res.json()
}

export async function fetchLinkMulticastDelivery(
  pk: string,
  params: FetchMulticastDeliveryParams = {},
): Promise<LinkMulticastDeliveryResponse> {
  const search = multicastDeliverySearchParams(params)
  const qs = search.toString()
  const res = await apiFetch(`/api/dz/links/${encodeURIComponent(pk)}/multicast-delivery${qs ? `?${qs}` : ''}`)
  if (!res.ok) {
    throw new Error('Failed to fetch link multicast delivery')
  }
  return res.json()
}

// Critical links types
export interface CriticalLink {
  sourcePK: string
  sourceCode: string
  targetPK: string
  targetCode: string
  metric: number
  criticality: 'critical' | 'important' | 'redundant'
}

export interface CriticalLinksResponse {
  links: CriticalLink[]
  error?: string
}

export async function fetchCriticalLinks(): Promise<CriticalLinksResponse> {
  const res = await apiFetch('/api/topology/critical-links')
  if (!res.ok) {
    throw new Error('Failed to fetch critical links')
  }
  return res.json()
}

// Redundancy report types
export interface RedundancyIssue {
  type: 'leaf_device' | 'critical_link' | 'single_exit_metro' | 'no_backup_device'
  severity: 'critical' | 'warning' | 'info'
  entityPK: string
  entityCode: string
  entityType: 'device' | 'link' | 'metro'
  description: string
  impact: string
  targetPK?: string
  targetCode?: string
  metroPK?: string
  metroCode?: string
}

export interface RedundancySummary {
  totalIssues: number
  criticalCount: number
  warningCount: number
  infoCount: number
  leafDevices: number
  criticalLinks: number
  singleExitMetros: number
}

export interface RedundancyReportResponse {
  issues: RedundancyIssue[]
  summary: RedundancySummary
  error?: string
}

export async function fetchRedundancyReport(): Promise<RedundancyReportResponse> {
  const res = await apiFetch('/api/topology/redundancy-report')
  if (!res.ok) {
    throw new Error('Failed to fetch redundancy report')
  }
  return res.json()
}

// Topology comparison types
export interface TopologyDiscrepancy {
  type: 'missing_isis' | 'partial_isis' | 'extra_isis'
  linkPK?: string
  linkCode?: string
  linkStatus?: string  // "activated", "soft-drained", "provisioning"
  deviceAPK: string
  deviceACode: string
  deviceBPK: string
  deviceBCode: string
  isisMetric?: number
  details: string
}

export interface TopologyCompareResponse {
  configuredLinks: number
  isisAdjacencies: number
  matchedLinks: number
  discrepancies: TopologyDiscrepancy[]
  error?: string
}

export async function fetchTopologyCompare(): Promise<TopologyCompareResponse> {
  const res = await apiFetch('/api/topology/compare')
  if (!res.ok) {
    throw new Error('Failed to fetch topology comparison')
  }
  return res.json()
}

// Failure impact types
export interface ImpactDevice {
  pk: string
  code: string
  status: string
  deviceType: string
}

export interface FailureImpactPath {
  fromPK: string
  fromCode: string
  toPK: string
  toCode: string
  beforeHops: number
  beforeMetric: number
  afterHops: number
  afterMetric: number
  hasAlternate: boolean
}

export interface MetroImpact {
  pk: string
  code: string
  name: string
  totalDevices: number
  remainingDevices: number
  isolatedDevices: number
}

export interface FailureImpactResponse {
  devicePK: string
  deviceCode: string
  unreachableDevices: ImpactDevice[]
  unreachableCount: number
  affectedPaths: FailureImpactPath[]
  affectedPathCount: number
  metroImpact: MetroImpact[]
  error?: string
}

export async function fetchFailureImpact(devicePK: string): Promise<FailureImpactResponse> {
  const res = await apiFetch(`/api/topology/impact/${encodeURIComponent(devicePK)}`)
  if (!res.ok) {
    throw new Error('Failed to fetch failure impact')
  }
  return res.json()
}

// What-If simulation types
export interface AffectedPath {
  fromPK: string
  fromCode: string
  toPK: string
  toCode: string
  beforeHops: number
  beforeMetric: number
  afterHops: number
  afterMetric: number
  hasAlternate: boolean
}

export interface SimulateLinkRemovalResponse {
  sourcePK: string
  sourceCode: string
  targetPK: string
  targetCode: string
  disconnectedDevices: ImpactDevice[]
  disconnectedCount: number
  affectedPaths: AffectedPath[]
  affectedPathCount: number
  causesPartition: boolean
  error?: string
}

export async function fetchSimulateLinkRemoval(
  sourcePK: string,
  targetPK: string
): Promise<SimulateLinkRemovalResponse> {
  const res = await apiFetch(
    `/api/topology/simulate-link-removal?sourcePK=${encodeURIComponent(sourcePK)}&targetPK=${encodeURIComponent(targetPK)}`
  )
  if (!res.ok) {
    throw new Error('Failed to simulate link removal')
  }
  return res.json()
}

export interface ImprovedPath {
  fromPK: string
  fromCode: string
  toPK: string
  toCode: string
  beforeHops: number
  beforeMetric: number
  afterHops: number
  afterMetric: number
  hopReduction: number
  metricReduction: number
}

export interface RedundancyGain {
  devicePK: string
  deviceCode: string
  oldDegree: number
  newDegree: number
  wasLeaf: boolean
}

export interface SimulateLinkAdditionResponse {
  sourcePK: string
  sourceCode: string
  targetPK: string
  targetCode: string
  metric: number
  improvedPaths: ImprovedPath[]
  improvedPathCount: number
  redundancyGains: RedundancyGain[]
  redundancyCount: number
  error?: string
}

export async function fetchSimulateLinkAddition(
  sourcePK: string,
  targetPK: string,
  metric: number = 1000
): Promise<SimulateLinkAdditionResponse> {
  const res = await apiFetch(
    `/api/topology/simulate-link-addition?sourcePK=${encodeURIComponent(sourcePK)}&targetPK=${encodeURIComponent(targetPK)}&metric=${metric}`
  )
  if (!res.ok) {
    throw new Error('Failed to simulate link addition')
  }
  return res.json()
}

// Link Health (SLO compliance) for topology overlay
export interface TopologyLinkHealth {
  link_pk: string
  side_a_pk: string
  side_a_code: string
  side_z_pk: string
  side_z_code: string
  avg_rtt_us: number
  p95_rtt_us: number
  committed_rtt_ns: number
  loss_pct: number
  exceeds_commit: boolean
  has_packet_loss: boolean
  is_dark: boolean
  is_down: boolean
  sla_status: 'healthy' | 'warning' | 'critical' | 'unknown'
  sla_ratio: number
}

export interface LinkHealthResponse {
  links: TopologyLinkHealth[]
  total_links: number
  healthy_count: number
  warning_count: number
  critical_count: number
  unknown_count: number
}

export async function fetchLinkHealth(): Promise<LinkHealthResponse> {
  const res = await apiFetch('/api/dz/links-health')
  if (!res.ok) {
    throw new Error('Failed to fetch link health')
  }
  return res.json()
}

// Version check
export interface VersionResponse {
  version: string
  commit: string
  date: string
}

export async function fetchVersion(): Promise<VersionResponse | null> {
  try {
    const res = await apiFetch('/api/version')
    if (!res.ok) return null
    return res.json()
  } catch {
    return null
  }
}

// Session persistence types
export interface SessionMetadata {
  id: string
  type: 'chat' | 'query'
  name: string | null
  content_length: number
  created_at: string
  updated_at: string
}

export interface ServerSession<T> {
  id: string
  type: 'chat' | 'query'
  name: string | null
  content: T
  created_at: string
  updated_at: string
}

export interface SessionListResponse {
  sessions: SessionMetadata[]
  total: number
  has_more: boolean
}

export interface SessionListWithContentResponse<T> {
  sessions: ServerSession<T>[]
  total: number
  has_more: boolean
}

// Get anonymous_id query param for unauthenticated requests
function getAnonymousIdParam(): string {
  if (getAuthToken()) {
    return '' // Authenticated users don't need anonymous_id
  }
  return `anonymous_id=${encodeURIComponent(getAnonymousId())}`
}

// Session API functions
export async function listSessions(
  type: 'chat' | 'query',
  limit = 50,
  offset = 0
): Promise<SessionListResponse> {
  const anonParam = getAnonymousIdParam()
  const params = [`type=${type}`, `limit=${limit}`, `offset=${offset}`]
  if (anonParam) params.push(anonParam)

  const res = await fetchWithRetry(`/api/sessions?${params.join('&')}`)
  if (!res.ok) {
    throw new Error('Failed to list sessions')
  }
  return res.json()
}

// List sessions with full content (avoids N+1 queries when loading all sessions)
export async function listSessionsWithContent<T>(
  type: 'chat' | 'query',
  limit = 50,
  offset = 0
): Promise<SessionListWithContentResponse<T>> {
  const anonParam = getAnonymousIdParam()
  const params = [`type=${type}`, `limit=${limit}`, `offset=${offset}`, 'include_content=true']
  if (anonParam) params.push(anonParam)

  const res = await fetchWithRetry(`/api/sessions?${params.join('&')}`)
  if (!res.ok) {
    throw new Error('Failed to list sessions')
  }
  return res.json()
}

export async function getSession<T>(id: string): Promise<ServerSession<T>> {
  const anonParam = getAnonymousIdParam()
  const url = anonParam ? `/api/sessions/${id}?${anonParam}` : `/api/sessions/${id}`

  const res = await fetchWithRetry(url)
  if (!res.ok) {
    if (res.status === 404) {
      throw new Error('Session not found')
    }
    throw new Error('Failed to get session')
  }
  return res.json()
}

export async function batchGetSessions<T>(
  ids: string[]
): Promise<{ sessions: ServerSession<T>[] }> {
  if (ids.length === 0) {
    return { sessions: [] }
  }
  const body: { ids: string[]; anonymous_id?: string } = { ids }
  if (!getAuthToken()) {
    body.anonymous_id = getAnonymousId()
  }

  const res = await fetchWithRetry('/api/sessions/batch', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    throw new Error('Failed to batch fetch sessions')
  }
  return res.json()
}

export async function createSession<T>(
  id: string,
  type: 'chat' | 'query',
  content: T,
  name?: string
): Promise<ServerSession<T>> {
  const body: { id: string; type: string; name: string | null; content: T; anonymous_id?: string } = {
    id,
    type,
    name: name ?? null,
    content,
  }
  if (!getAuthToken()) {
    body.anonymous_id = getAnonymousId()
  }

  const res = await fetchWithRetry('/api/sessions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    if (res.status === 409) {
      throw new Error('Session already exists')
    }
    throw new Error('Failed to create session')
  }
  return res.json()
}

export async function updateSession<T>(
  id: string,
  content: T,
  name?: string
): Promise<ServerSession<T>> {
  const body: { content: T; name: string | null; anonymous_id?: string } = {
    content,
    name: name ?? null,
  }
  if (!getAuthToken()) {
    body.anonymous_id = getAnonymousId()
  }

  const res = await fetchWithRetry(`/api/sessions/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    if (res.status === 404) {
      throw new Error('Session not found')
    }
    throw new Error('Failed to update session')
  }
  return res.json()
}

export async function deleteSession(id: string): Promise<void> {
  const anonParam = getAnonymousIdParam()
  const url = anonParam ? `/api/sessions/${id}?${anonParam}` : `/api/sessions/${id}`

  const res = await fetchWithRetry(url, {
    method: 'DELETE',
  })
  if (!res.ok && res.status !== 404) {
    throw new Error('Failed to delete session')
  }
}

// Upsert helper - creates if not exists, updates otherwise
export async function upsertSession<T>(
  id: string,
  type: 'chat' | 'query',
  content: T,
  name?: string
): Promise<ServerSession<T>> {
  try {
    return await updateSession(id, content, name)
  } catch (err) {
    if (err instanceof Error && err.message === 'Session not found') {
      return await createSession(id, type, content, name)
    }
    throw err
  }
}

// Workflow types for durable workflow persistence
export interface WorkflowStep {
  type: 'thinking' | 'query'
  content?: string
  question?: string
  sql?: string
  status?: string
  columns?: string[]
  rows?: unknown[][]
  count?: number
  error?: string
}

export interface WorkflowRun {
  id: string
  session_id: string
  status: 'running' | 'completed' | 'failed' | 'cancelled'
  user_question: string
  iteration: number
  steps?: WorkflowStep[]
  final_answer?: string
  llm_calls: number
  input_tokens: number
  output_tokens: number
  started_at: string
  updated_at: string
  completed_at?: string
  error?: string
}

// Get a workflow run by ID
export async function getWorkflow(workflowId: string): Promise<WorkflowRun | null> {
  const anonParam = getAnonymousIdParam()
  const url = anonParam ? `/api/workflows/${workflowId}?${anonParam}` : `/api/workflows/${workflowId}`
  const res = await apiFetch(url)
  if (res.status === 404) {
    return null
  }
  if (!res.ok) {
    throw new Error('Failed to get workflow')
  }
  return res.json()
}

// Get the latest workflow for a session (running, completed, or failed)
export async function getLatestWorkflowForSession(sessionId: string): Promise<WorkflowRun | null> {
  const anonParam = getAnonymousIdParam()
  const url = anonParam ? `/api/sessions/${sessionId}/workflow?${anonParam}` : `/api/sessions/${sessionId}/workflow`
  const res = await apiFetch(url)
  if (res.status === 204 || res.status === 404) {
    return null // No workflow
  }
  if (!res.ok) {
    throw new Error('Failed to get workflow')
  }
  return res.json()
}

// Legacy alias for backwards compatibility
export const getRunningWorkflowForSession = getLatestWorkflowForSession

// Workflow reconnection callbacks
export interface WorkflowReconnectCallbacks {
  onThinking: (data: { id: string; content: string }) => void
  // SQL query events
  onSqlDone: (data: { id: string; question: string; sql: string; rows: number; error: string; env?: string }) => void
  // Cypher query events
  onCypherDone?: (data: { id: string; question: string; cypher: string; rows: number; error: string; env?: string }) => void
  // ReadDocs events
  onReadDocsDone?: (data: { id: string; page: string; content: string; error: string }) => void
  onSynthesizing?: () => void
  onDone: (response: ChatResponse) => void
  onError: (error: string) => void
  onStatus?: (data: { status: string; iteration: number }) => void
  onRetry?: () => void
}

// Reconnect to a running or completed workflow stream
// This allows the frontend to catch up on progress if the page was refreshed
export async function reconnectToWorkflow(
  workflowId: string,
  callbacks: WorkflowReconnectCallbacks,
  signal?: AbortSignal
): Promise<void> {
  const anonParam = getAnonymousIdParam()
  const url = anonParam
    ? `/api/workflows/${workflowId}/stream?${anonParam}`
    : `/api/workflows/${workflowId}/stream`
  const res = await apiFetch(url, { signal })

  if (!res.ok) {
    if (res.status === 404) {
      callbacks.onError('Workflow not found')
      return
    }
    callbacks.onError('Failed to connect to workflow')
    return
  }

  if (!res.body) {
    callbacks.onError('No response body')
    return
  }

  const reader = res.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  let currentEvent = ''

  const processLines = (lines: string[]) => {
    for (const line of lines) {
      if (line.startsWith('event: ')) {
        currentEvent = line.slice(7).trim()
      } else if (line.startsWith('data:') && currentEvent) {
        const data = line.startsWith('data: ') ? line.slice(6) : line.slice(5)
        if (!data.trim()) {
          continue
        }
        try {
          const parsed = JSON.parse(data)
          switch (currentEvent) {
            case 'workflow_status':
              callbacks.onStatus?.(parsed)
              break
            case 'thinking':
              callbacks.onThinking(parsed)
              break
            // SQL query events
            case 'sql_done':
              callbacks.onSqlDone(parsed)
              break
            // Cypher query events
            case 'cypher_done':
              callbacks.onCypherDone?.(parsed)
              break
            // ReadDocs events
            case 'read_docs_done':
              callbacks.onReadDocsDone?.(parsed)
              break
            case 'synthesizing':
              callbacks.onSynthesizing?.()
              break
            case 'done':
              callbacks.onDone(parsed)
              break
            case 'error':
              callbacks.onError(parsed.error || 'Unknown error')
              break
            case 'live':
              // Workflow is still running - keep connection open
              console.log('[Workflow] Connected to live workflow')
              break
            case 'retry':
              // Workflow is running but not on this server - client should retry
              console.log('[Workflow] Server says retry')
              callbacks.onRetry?.()
              break
          }
        } catch (e) {
          console.error('[Workflow] Parse error for event', currentEvent, e)
        }
        currentEvent = ''
      }
    }
  }

  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) {
        if (buffer.trim()) {
          const lines = buffer.split('\n')
          processLines(lines)
        }
        break
      }

      buffer += decoder.decode(value, { stream: true })
      const lines = buffer.split('\n')
      buffer = lines.pop() || ''
      processLines(lines)
    }
  } catch (err) {
    if (err instanceof Error && err.name === 'AbortError') {
      return
    }
    callbacks.onError(err instanceof Error ? err.message : 'Stream error')
  }
}

// Paginated response type
export interface PaginatedResponse<T> {
  items: T[]
  total: number
  limit: number
  offset: number
}

export async function fetchAllPaginated<T, R extends PaginatedResponse<T> = PaginatedResponse<T>>(
  fetchPage: (limit: number, offset: number) => Promise<R>,
  pageSize = 100
): Promise<R> {
  const items: T[] = []
  let offset = 0
  let firstResponse: R | null = null

  while (true) {
    const response = await fetchPage(pageSize, offset)
    if (offset === 0) {
      firstResponse = response
    }
    items.push(...response.items)
    offset += response.limit
    if (items.length >= firstResponse!.total || response.items.length === 0) {
      break
    }
  }

  return {
    ...firstResponse!,
    items,
    total: firstResponse!.total,
    limit: pageSize,
    offset: 0,
  }
}

// Entity types - DoubleZero
export interface Device {
  pk: string
  code: string
  status: string
  device_type: string
  contributor_pk: string
  contributor_code: string
  metro_pk: string
  metro_code: string
  location_pk: string
  location_code: string
  public_ip: string
  max_users: number
  current_users: number
  unicast_users: number
  multicast_users: number
  max_unicast_users: number
  max_multicast_subscribers: number
  max_multicast_publishers: number
  unicast_users_count: number
  multicast_subscribers_count: number
  reserved_seats: number
  multicast_publishers_count: number
  in_bps: number
  out_bps: number
  peak_in_bps: number
  peak_out_bps: number
}

export async function fetchDevices(
  limit = 100,
  offset = 0,
  sortBy?: string,
  sortDir?: 'asc' | 'desc',
  filters?: string[]
): Promise<PaginatedResponse<Device>> {
  const params = new URLSearchParams()
  params.set('limit', String(limit))
  params.set('offset', String(offset))
  if (sortBy) params.set('sort_by', sortBy)
  if (sortDir) params.set('sort_dir', sortDir)
  if (filters) filters.forEach(f => params.append('filters', f))
  const res = await fetchWithRetry(`/api/dz/devices?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch devices')
  }
  return res.json()
}

export async function fetchDevicesByMetro(metroPk: string, limit = 500, offset = 0): Promise<PaginatedResponse<Device>> {
  const res = await fetchWithRetry(`/api/dz/devices?metro_pk=${encodeURIComponent(metroPk)}&limit=${limit}&offset=${offset}`)
  if (!res.ok) {
    throw new Error('Failed to fetch devices')
  }
  return res.json()
}

export async function fetchDevicesByContributor(contributorPk: string, limit = 500, offset = 0): Promise<PaginatedResponse<Device>> {
  const res = await fetchWithRetry(`/api/dz/devices?contributor_pk=${encodeURIComponent(contributorPk)}&limit=${limit}&offset=${offset}`)
  if (!res.ok) {
    throw new Error('Failed to fetch devices')
  }
  return res.json()
}

export interface DeviceDetail extends Device {
  metro_name: string
  interfaces: DeviceInterface[]
}

export async function fetchDevice(pk: string): Promise<DeviceDetail> {
  const res = await fetchWithRetry(`/api/dz/devices/${encodeURIComponent(pk)}`)
  if (!res.ok) {
    throw new Error('Failed to fetch device')
  }
  return res.json()
}

export interface DeviceValidatorStats {
  validator_count: number
  stake_sol: number
  stake_share: number
}

export async function fetchDeviceValidatorStats(pk: string): Promise<DeviceValidatorStats> {
  const res = await fetchWithRetry(`/api/dz/devices/${encodeURIComponent(pk)}/validator-stats`)
  if (!res.ok) {
    throw new Error('Failed to fetch device validator stats')
  }
  return res.json()
}

export type DeviceControllerCallStatus = 'calling' | 'stopped' | 'recovered' | 'no_data' | 'not_expected'

export interface DeviceControllerCallsBucket {
  ts: string
  calls: number
  minutes_with_calls: number
  status: DeviceControllerCallStatus
  gap_seconds?: number
}

export interface DeviceControllerCallsResponse {
  device_pk: string
  device_code: string
  device_status: string
  time_range: string
  bucket_seconds: number
  bucket_count: number
  from: string
  to: string
  source_available: boolean
  last_call_at?: string
  current_gap_seconds?: number
  last_status: DeviceControllerCallStatus
  total_calls: number
  minutes_with_calls: number
  alert_threshold_minutes: number
  history_window_hours: number
  prior_history_minimum: number
  buckets: DeviceControllerCallsBucket[]
}

export interface FetchDeviceControllerCallsParams {
  range?: string
  startTime?: number
  endTime?: number
  bucket?: string
}

export async function fetchDeviceControllerCalls(
  pk: string,
  params: FetchDeviceControllerCallsParams = {}
): Promise<DeviceControllerCallsResponse> {
  const qs = new URLSearchParams()
  if (params.range) qs.set('range', params.range)
  if (params.startTime) qs.set('start_time', params.startTime.toString())
  if (params.endTime) qs.set('end_time', params.endTime.toString())
  if (params.bucket) qs.set('bucket', params.bucket)
  const res = await apiFetch(`/api/dz/devices/${encodeURIComponent(pk)}/controller-calls${qs.toString() ? '?' + qs.toString() : ''}`)
  if (!res.ok) throw new Error('Failed to fetch device controller calls')
  return res.json()
}

export type OpticsSeverity = 'ok' | 'warning' | 'critical' | 'unknown'

export interface OpticsThresholds {
  input_warning_lower?: number
  input_warning_upper?: number
  input_critical_lower?: number
  input_critical_upper?: number
  output_warning_lower?: number
  output_warning_upper?: number
  output_critical_lower?: number
  output_critical_upper?: number
  bias_warning_lower?: number
  bias_warning_upper?: number
  bias_critical_lower?: number
  bias_critical_upper?: number
}

export interface OpticsLane {
  interface_name: string
  channel_index: number
  timestamp: string
  input_power: number
  output_power: number
  laser_bias_current: number
  input_severity: OpticsSeverity
  output_severity: OpticsSeverity
  bias_severity: OpticsSeverity
  overall_severity: OpticsSeverity
  thresholds?: OpticsThresholds
}

export interface OpticsSummary {
  ok: number
  warning: number
  critical: number
  unknown: number
  total: number
}

export interface DeviceOpticsResponse {
  device_pk: string
  device_code: string
  lanes: OpticsLane[]
  summary: OpticsSummary
  fetched_at: string
}

export async function fetchDeviceOptics(pk: string): Promise<DeviceOpticsResponse> {
  const res = await fetchWithRetry(`/api/dz/devices/${encodeURIComponent(pk)}/optics`)
  if (!res.ok) {
    throw new Error('Failed to fetch device optics')
  }
  return res.json()
}

export interface DeviceOpticsHistoryPoint {
  ts: string
  channel_index: number
  avg_input_power: number
  min_input_power: number
  max_input_power: number
  avg_output_power: number
  min_output_power: number
  max_output_power: number
  avg_laser_bias_current: number
}

export interface DeviceOpticsHistoryResponse {
  device_pk: string
  interface_name: string
  channel_index?: number
  bucket_seconds: number
  from: string
  to: string
  buckets: DeviceOpticsHistoryPoint[]
}

export interface FetchDeviceOpticsHistoryParams {
  interface: string
  channel?: number
  range?: string
  startTime?: number
  endTime?: number
  bucket?: string
}

export async function fetchDeviceOpticsHistory(
  pk: string,
  params: FetchDeviceOpticsHistoryParams
): Promise<DeviceOpticsHistoryResponse> {
  const search = new URLSearchParams({ interface: params.interface })
  if (params.channel !== undefined) search.set('channel', String(params.channel))
  if (params.range) search.set('range', params.range)
  if (params.startTime !== undefined) search.set('start_time', String(params.startTime))
  if (params.endTime !== undefined) search.set('end_time', String(params.endTime))
  if (params.bucket) search.set('bucket', params.bucket)
  const res = await fetchWithRetry(
    `/api/dz/devices/${encodeURIComponent(pk)}/optics/history?${search.toString()}`
  )
  if (!res.ok) {
    throw new Error('Failed to fetch device optics history')
  }
  return res.json()
}

export interface Link {
  pk: string
  code: string
  status: string
  link_type: string
  bandwidth_bps: number
  side_a_pk: string
  side_a_code: string
  side_a_metro_pk: string
  side_a_metro: string
  side_a_iface_name: string
  side_a_ip: string
  side_z_pk: string
  side_z_code: string
  side_z_metro_pk: string
  side_z_metro: string
  side_z_iface_name: string
  side_z_ip: string
  contributor_pk: string
  contributor_code: string
  side_a_contributor_pk: string
  side_a_contributor_code: string
  side_z_contributor_pk: string
  side_z_contributor_code: string
  in_bps: number
  out_bps: number
  utilization_in: number
  utilization_out: number
  latency_us: number
  jitter_us: number
  latency_a_to_z_us: number
  jitter_a_to_z_us: number
  latency_z_to_a_us: number
  jitter_z_to_a_us: number
  loss_percent: number
  link_topologies?: string[]
  unicast_drained?: boolean
}

export async function fetchLinks(
  limit = 100,
  offset = 0,
  sortBy?: string,
  sortDir?: 'asc' | 'desc',
  filters?: string[]
): Promise<PaginatedResponse<Link>> {
  const params = new URLSearchParams()
  params.set('limit', String(limit))
  params.set('offset', String(offset))
  if (sortBy) params.set('sort_by', sortBy)
  if (sortDir) params.set('sort_dir', sortDir)
  if (filters) filters.forEach(f => params.append('filters', f))
  const res = await fetchWithRetry(`/api/dz/links?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch links')
  }
  return res.json()
}

export async function fetchLinksByContributor(contributorPk: string, limit = 500, offset = 0): Promise<PaginatedResponse<Link>> {
  const res = await fetchWithRetry(`/api/dz/links?contributor_pk=${encodeURIComponent(contributorPk)}&limit=${limit}&offset=${offset}&sort_by=code&sort_dir=asc`)
  if (!res.ok) throw new Error('Failed to fetch links')
  return res.json()
}

export interface LinkDetail extends Link {
  peak_in_bps: number
  peak_out_bps: number
  committed_rtt_ns: number
  isis_delay_override_ns: number
}

export async function fetchLink(pk: string): Promise<LinkDetail> {
  const res = await fetchWithRetry(`/api/dz/links/${encodeURIComponent(pk)}`)
  if (!res.ok) {
    throw new Error('Failed to fetch link')
  }
  return res.json()
}

export interface Metro {
  pk: string
  code: string
  name: string
  country: string
  latitude: number
  longitude: number
  device_count: number
  user_count: number
  facility_count: number
  unicast_users_count: number
  multicast_subscribers_count: number
  multicast_publishers_count: number
  max_users: number
  max_unicast_users: number
  max_multicast_subscribers: number
  max_multicast_publishers: number
  raw_max_unicast_users: number
  raw_max_multicast_subscribers: number
  raw_max_multicast_publishers: number
}

export async function fetchMetros(
  limit = 100,
  offset = 0,
  sortBy?: string,
  sortDir?: 'asc' | 'desc',
  filters?: string[]
): Promise<PaginatedResponse<Metro>> {
  const params = new URLSearchParams()
  params.set('limit', String(limit))
  params.set('offset', String(offset))
  if (sortBy) params.set('sort_by', sortBy)
  if (sortDir) params.set('sort_dir', sortDir)
  if (filters) filters.forEach(f => params.append('filters', f))
  const res = await fetchWithRetry(`/api/dz/metros?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch metros')
  }
  return res.json()
}

export type MetroDetail = Metro

export async function fetchMetro(pk: string): Promise<MetroDetail> {
  const res = await fetchWithRetry(`/api/dz/metros/${encodeURIComponent(pk)}`)
  if (!res.ok) {
    throw new Error('Failed to fetch metro')
  }
  return res.json()
}

export interface MetroStats {
  validator_count: number
  stake_sol: number
  in_bps: number
  out_bps: number
}

export async function fetchMetroStats(pk: string): Promise<MetroStats> {
  const res = await fetchWithRetry(`/api/dz/metros/${encodeURIComponent(pk)}/stats`)
  if (!res.ok) {
    throw new Error('Failed to fetch metro stats')
  }
  return res.json()
}

export interface Location {
  pk: string
  code: string
  name: string
  country: string
  lat: number
  lng: number
  loc_id: number
  metro_pk: string
  metro_code: string
  device_count: number
  user_count: number
  max_users: number
  unicast_users_count: number
  multicast_subscribers_count: number
  multicast_publishers_count: number
  max_unicast_users: number
  max_multicast_subscribers: number
  max_multicast_publishers: number
}

export interface FacilityDetail {
  pk: string
  code: string
  name: string
  country: string
  lat: number
  lng: number
  loc_id: number
  status: string
  metro_pk: string
  metro_code: string
  device_count: number
  user_count: number
  max_users: number
  unicast_users_count: number
  multicast_subscribers_count: number
  multicast_publishers_count: number
  max_unicast_users: number
  max_multicast_subscribers: number
  max_multicast_publishers: number
}

export async function fetchFacility(pk: string): Promise<FacilityDetail> {
  const res = await fetchWithRetry(`/api/dz/facilities/${encodeURIComponent(pk)}`)
  if (!res.ok) throw new Error('Failed to fetch facility')
  return res.json()
}

export async function fetchFacilities(
  limit = 100,
  offset = 0,
  sortBy?: string,
  sortDir?: 'asc' | 'desc',
  filters?: string[]
): Promise<PaginatedResponse<Location>> {
  const params = new URLSearchParams()
  params.set('limit', String(limit))
  params.set('offset', String(offset))
  if (sortBy) params.set('sort_by', sortBy)
  if (sortDir) params.set('sort_dir', sortDir)
  if (filters) filters.forEach(f => params.append('filters', f))
  const res = await fetchWithRetry(`/api/dz/facilities?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch facilities')
  }
  return res.json()
}

export async function fetchFacilitiesByMetro(metroPk: string, limit = 500, offset = 0): Promise<PaginatedResponse<Location>> {
  const params = new URLSearchParams()
  params.set('limit', String(limit))
  params.set('offset', String(offset))
  params.set('sort_by', 'code')
  params.set('sort_dir', 'asc')
  params.append('filters', `metro_pk:${metroPk}`)
  const res = await fetchWithRetry(`/api/dz/facilities?${params}`)
  if (!res.ok) throw new Error('Failed to fetch facilities')
  return res.json()
}

export interface PeeringDBFacility {
  orgName: string
  aka: string
  logoUrl: string
}

export async function fetchPeeringDBFacility(locId: number): Promise<PeeringDBFacility> {
  const res = await fetchWithRetry(`/api/peeringdb/fac/${locId}`)
  if (!res.ok) {
    throw new Error(`Failed to fetch PeeringDB facility ${locId}`)
  }
  return res.json()
}

export interface Contributor {
  pk: string
  code: string
  name: string
  metro_count: number
  facility_count: number
  device_count: number
  side_a_devices: number
  side_z_devices: number
  link_count: number
  user_count: number
  max_users: number
}

export async function fetchContributors(
  limit = 100,
  offset = 0,
  sortBy?: string,
  sortDir?: 'asc' | 'desc',
  filters?: string[]
): Promise<PaginatedResponse<Contributor>> {
  const params = new URLSearchParams()
  params.set('limit', String(limit))
  params.set('offset', String(offset))
  if (sortBy) params.set('sort_by', sortBy)
  if (sortDir) params.set('sort_dir', sortDir)
  if (filters) filters.forEach(f => params.append('filters', f))
  const res = await fetchWithRetry(`/api/dz/contributors?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch contributors')
  }
  return res.json()
}

export interface ContributorDetail extends Contributor {
  user_count: number
  unicast_users_count: number
  multicast_subscribers_count: number
  multicast_publishers_count: number
  max_users: number
  max_unicast_users: number
  max_multicast_subscribers: number
  max_multicast_publishers: number
  raw_max_unicast_users: number
  raw_max_multicast_subscribers: number
  raw_max_multicast_publishers: number
  in_bps: number
  out_bps: number
}

export async function fetchContributor(pk: string): Promise<ContributorDetail> {
  const res = await fetchWithRetry(`/api/dz/contributors/${encodeURIComponent(pk)}`)
  if (!res.ok) {
    throw new Error('Failed to fetch contributor')
  }
  return res.json()
}

export interface User {
  pk: string
  owner_pubkey: string
  status: string
  kind: string
  dz_ip: string
  client_ip: string
  device_pk: string
  device_code: string
  metro_pk: string
  metro_code: string
  metro_name: string
  location_pk: string
  location_code: string
  tenant_pk: string
  tenant_code: string
  include_topologies: string[]
  in_bps: number
  out_bps: number
  is_deleted: boolean
}

export async function fetchUsers(
  limit = 100,
  offset = 0,
  sortBy?: string,
  sortDir?: 'asc' | 'desc',
  filters?: string[],
  includeDeleted = false
): Promise<PaginatedResponse<User>> {
  const params = new URLSearchParams({ limit: String(limit), offset: String(offset) })
  if (sortBy) params.set('sort_by', sortBy)
  if (sortDir) params.set('sort_dir', sortDir)
  if (filters) {
    for (const f of filters) params.append('filters', f)
  }
  if (includeDeleted) params.set('include_deleted', 'true')
  const res = await fetchWithRetry(`/api/dz/users?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch users')
  }
  return res.json()
}

export interface UserDetail extends User {
  client_ip: string
  tunnel_id: number
  metro_pk: string
  facility_loc_id: number
  contributor_pk: string
  contributor_code: string
  is_validator: boolean
  node_pubkey: string
  vote_pubkey: string
  stake_sol: number
  stake_weight_pct: number
  is_deleted: boolean
}

export async function fetchUser(pk: string): Promise<UserDetail> {
  const res = await fetchWithRetry(`/api/dz/users/${encodeURIComponent(pk)}`)
  if (!res.ok) {
    throw new Error('Failed to fetch user')
  }
  return res.json()
}

export interface UserMulticastGroup {
  group_pk: string
  group_code: string
  multicast_ip: string
  mode: 'P' | 'S' | 'P+S'
  status: string
  publisher_count: number
  subscriber_count: number
}

export async function fetchUserMulticastGroups(pk: string): Promise<UserMulticastGroup[]> {
  const res = await fetchWithRetry(`/api/dz/users/${encodeURIComponent(pk)}/multicast-groups`)
  if (!res.ok) {
    throw new Error('Failed to fetch user multicast groups')
  }
  return res.json()
}

// Solana entity types
export interface Validator {
  vote_pubkey: string
  node_pubkey: string
  stake_sol: number
  stake_share: number
  commission: number
  on_dz: boolean
  device_code: string
  metro_code: string
  city: string
  country: string
  in_bps: number
  out_bps: number
  skip_rate: number
  version: string
  software_client: string
}

export interface ValidatorsResponse extends PaginatedResponse<Validator> {
  on_dz_count: number
}

export async function fetchValidators(
  limit = 100,
  offset = 0,
  sortBy?: string,
  sortDir?: 'asc' | 'desc',
  filters?: string[]
): Promise<ValidatorsResponse> {
  const params = new URLSearchParams({ limit: String(limit), offset: String(offset) })
  if (sortBy) params.set('sort_by', sortBy)
  if (sortDir) params.set('sort_dir', sortDir)
  if (filters) {
    for (const f of filters) params.append('filters', f)
  }
  const res = await fetchWithRetry(`/api/solana/validators?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch validators')
  }
  return res.json()
}

export interface ValidatorDetail extends Validator {
  device_pk: string
  metro_pk: string
  gossip_ip: string
  gossip_port: number
  software_version: string
}

export async function fetchValidator(votePubkey: string): Promise<ValidatorDetail> {
  const res = await fetchWithRetry(`/api/solana/validators/${encodeURIComponent(votePubkey)}`)
  if (!res.ok) {
    throw new Error('Failed to fetch validator')
  }
  return res.json()
}

// User traffic types
export interface UserTrafficPoint {
  time: string
  tunnel_id: number
  in_bps: number
  out_bps: number
  in_pps: number
  out_pps: number
}

export async function fetchUserTraffic(pk: string, timeRange?: string, bucket?: string, agg?: string): Promise<UserTrafficPoint[]> {
  const params = new URLSearchParams()
  if (timeRange) params.set('time_range', timeRange)
  if (bucket && bucket !== 'auto') params.set('bucket', bucket)
  if (agg && agg !== 'max') params.set('agg', agg)
  const qs = params.toString()
  const res = await apiFetch(`/api/dz/users/${encodeURIComponent(pk)}/traffic${qs ? `?${qs}` : ''}`)
  if (!res.ok) {
    throw new Error('Failed to fetch user traffic')
  }
  return res.json()
}

export interface GossipNode {
  pubkey: string
  gossip_ip: string
  gossip_port: number
  version: string
  city: string
  country: string
  on_dz: boolean
  device_code: string
  metro_code: string
  stake_sol: number
  is_validator: boolean
}

export interface GossipNodesResponse extends PaginatedResponse<GossipNode> {
  on_dz_count: number
  validator_count: number
}

export async function fetchGossipNodes(
  limit = 100,
  offset = 0,
  sortBy?: string,
  sortDir?: 'asc' | 'desc',
  filters?: string[]
): Promise<GossipNodesResponse> {
  const params = new URLSearchParams({ limit: String(limit), offset: String(offset) })
  if (sortBy) params.set('sort_by', sortBy)
  if (sortDir) params.set('sort_dir', sortDir)
  if (filters) {
    for (const f of filters) params.append('filters', f)
  }
  const res = await fetchWithRetry(`/api/solana/gossip-nodes?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch gossip nodes')
  }
  return res.json()
}

export interface GossipNodeDetail extends GossipNode {
  user_pk: string
  owner_pubkey: string
  device_pk: string
  metro_pk: string
  vote_pubkey: string
}

export async function fetchGossipNode(pubkey: string): Promise<GossipNodeDetail> {
  const res = await fetchWithRetry(`/api/solana/gossip-nodes/${encodeURIComponent(pubkey)}`)
  if (!res.ok) {
    throw new Error('Failed to fetch gossip node')
  }
  return res.json()
}

// Timeline types
export interface TimelineEvent {
  id: string
  event_type: string
  timestamp: string
  category: string
  severity: 'info' | 'warning' | 'critical' | 'success'
  title: string
  description?: string
  entity_type: string
  entity_pk: string
  entity_code: string
  details?: EntityChangeDetails | IncidentEventDetails | ValidatorEventDetails
}

export interface EntityChangeDetails {
  change_type: 'created' | 'updated' | 'deleted'
  changes?: FieldChange[]
  entity?: DeviceEntity | LinkEntity | MetroEntity | ContributorEntity | UserEntity | FeedEntity
}

export interface FieldChange {
  field: string
  old_value?: unknown
  new_value?: unknown
}

export interface DeviceEntity {
  pk: string
  code: string
  status: string
  device_type: string
  public_ip: string
  contributor_pk: string
  metro_pk: string
  max_users: number
  contributor_code?: string
  metro_code?: string
}

export interface LinkEntity {
  pk: string
  code: string
  status: string
  link_type: string
  tunnel_net: string
  contributor_pk: string
  side_a_pk: string
  side_z_pk: string
  side_a_iface_name: string
  side_z_iface_name: string
  committed_rtt_ns: number
  committed_jitter_ns: number
  bandwidth_bps: number
  isis_delay_override_ns: number
  contributor_code?: string
  side_a_code?: string
  side_z_code?: string
  side_a_metro_code?: string
  side_z_metro_code?: string
  side_a_metro_pk?: string
  side_z_metro_pk?: string
}

export interface MetroEntity {
  pk: string
  code: string
  name: string
  longitude: number
  latitude: number
}

export interface ContributorEntity {
  pk: string
  code: string
  name: string
}

export interface UserEntity {
  pk: string
  owner_pubkey: string
  status: string
  kind: string
  client_ip: string
  dz_ip: string
  device_pk: string
  tunnel_id: number
  device_code?: string
  metro_code?: string
}

export interface FeedEntity {
  pk: string
  owner_pubkey: string
  code: string
  name: string
  metro_pk: string
  groups: string
  metro_code?: string
}

export interface IncidentEventDetails {
  entity_pk: string
  entity_code: string
  entity_type: 'link' | 'device'
  incident_type: string
  peak_value: number
  duration_seconds: number
  is_ongoing: boolean
  link_type?: string
  side_a_metro?: string
  side_z_metro?: string
  metro?: string
  contributor_code?: string
  status?: string
}

export interface ValidatorEventDetails {
  owner_pubkey: string
  dz_ip?: string
  vote_pubkey?: string
  node_pubkey?: string
  stake_lamports?: number
  stake_sol?: number
  stake_share_pct?: number
  stake_share_change_pct?: number
  dz_total_stake_share_pct?: number
  user_pk?: string
  device_pk?: string
  device_code?: string
  metro_code?: string
  kind: 'validator' | 'gossip_only'
  action: string
  contribution_change_lamports?: number
  prev_gossip_ip?: string
}

export interface HistogramBucket {
  timestamp: string
  count: number
}

export interface TimelineResponse {
  events: TimelineEvent[]
  total: number
  limit: number
  offset: number
  time_range: {
    start: string
    end: string
  }
  histogram?: HistogramBucket[]
  error?: string
}

export type TimeRange = '1h' | '6h' | '12h' | '24h' | '3d' | '7d'
export type ActionFilter = 'added' | 'removed' | 'changed' | 'alerting' | 'resolved'

export interface TimelineParams {
  range?: TimeRange
  start?: string // ISO 8601 timestamp for custom range
  end?: string   // ISO 8601 timestamp for custom range
  category?: string
  entity_type?: string
  severity?: string
  action?: string // Comma-separated action filters
  dz_filter?: 'on_dz' | 'off_dz' // Filter Solana events by DZ connection
  min_stake_pct?: number // Minimum stake share percentage to include
  search?: string // Comma-separated search terms to filter by entity codes, device codes, etc.
  limit?: number
  offset?: number
  include_internal?: boolean
}

export interface TimelineBoundsResponse {
  earliest_data: string // ISO 8601 timestamp
  latest_data: string   // ISO 8601 timestamp
}

export async function fetchTimelineBounds(): Promise<TimelineBoundsResponse> {
  const res = await fetchWithRetry('/api/timeline/bounds')
  if (!res.ok) {
    throw new Error('Failed to fetch timeline bounds')
  }
  return res.json()
}

export async function fetchTimeline(params: TimelineParams = {}): Promise<TimelineResponse> {
  const searchParams = new URLSearchParams()
  if (params.range) searchParams.set('range', params.range)
  if (params.start) searchParams.set('start', params.start)
  if (params.end) searchParams.set('end', params.end)
  if (params.category) searchParams.set('category', params.category)
  if (params.entity_type) searchParams.set('entity_type', params.entity_type)
  if (params.severity) searchParams.set('severity', params.severity)
  if (params.action) searchParams.set('action', params.action)
  if (params.dz_filter) searchParams.set('dz_filter', params.dz_filter)
  if (params.min_stake_pct) searchParams.set('min_stake_pct', params.min_stake_pct.toString())
  if (params.search) searchParams.set('search', params.search)
  if (params.limit) searchParams.set('limit', params.limit.toString())
  if (params.offset) searchParams.set('offset', params.offset.toString())
  if (params.include_internal) searchParams.set('include_internal', 'true')

  const url = `/api/timeline${searchParams.toString() ? '?' + searchParams.toString() : ''}`
  const res = await fetchWithRetry(url)
  if (!res.ok) {
    throw new Error('Failed to fetch timeline')
  }
  return res.json()
}

// Metro connectivity matrix types
export interface MetroInfo {
  pk: string
  code: string
  name: string
}

export interface MetroConnectivity {
  fromMetroPK: string
  fromMetroCode: string
  fromMetroName: string
  toMetroPK: string
  toMetroCode: string
  toMetroName: string
  pathCount: number
  minHops: number
  minMetric: number
  bottleneckBwGbps?: number  // min bandwidth along best path
}

export interface MetroConnectivityResponse {
  metros: MetroInfo[]
  connectivity: MetroConnectivity[]
  error?: string
}

export async function fetchMetroConnectivity(): Promise<MetroConnectivityResponse> {
  const res = await apiFetch('/api/topology/metro-connectivity')
  if (!res.ok) {
    throw new Error('Failed to fetch metro connectivity')
  }
  return res.json()
}

// DZ vs Internet latency comparison types
export interface LatencyComparison {
  origin_metro_pk: string
  origin_metro_code: string
  origin_metro_name: string
  target_metro_pk: string
  target_metro_code: string
  target_metro_name: string
  dz_avg_rtt_ms: number
  dz_p95_rtt_ms: number
  dz_avg_jitter_ms: number | null
  dz_loss_pct: number
  dz_sample_count: number
  internet_avg_rtt_ms: number
  internet_p95_rtt_ms: number
  internet_avg_jitter_ms: number | null
  internet_sample_count: number
  rtt_improvement_pct: number | null
  jitter_improvement_pct: number | null
}

export interface LatencyComparisonResponse {
  comparisons: LatencyComparison[]
  summary: {
    total_pairs: number
    avg_improvement_pct: number
    max_improvement_pct: number
    pairs_with_data: number
  }
}

export async function fetchLatencyComparison(): Promise<LatencyComparisonResponse> {
  const res = await apiFetch('/api/topology/latency-comparison')
  if (!res.ok) {
    throw new Error('Failed to fetch latency comparison')
  }
  return res.json()
}

// Latency history time series types
export interface LatencyHistoryPoint {
  timestamp: string
  dz_avg_rtt_ms: number | null
  dz_avg_jitter_ms: number | null
  dz_sample_count: number
  inet_avg_rtt_ms: number | null
  inet_avg_jitter_ms: number | null
  inet_sample_count: number
}

export interface LatencyHistoryResponse {
  origin_metro_code: string
  target_metro_code: string
  points: LatencyHistoryPoint[]
}

export async function fetchLatencyHistory(
  originCode: string,
  targetCode: string,
  timeRange?: string
): Promise<LatencyHistoryResponse> {
  const params = timeRange ? `?range=${timeRange}` : ''
  const res = await apiFetch(`/api/topology/latency-history/${originCode}/${targetCode}${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch latency history')
  }
  return res.json()
}

// Metro path latency types (path-based DZ vs Internet comparison)
export type PathOptimizeMode = 'hops' | 'latency' | 'bandwidth'

export interface MetroPathLatency {
  fromMetroPK: string
  fromMetroCode: string
  toMetroPK: string
  toMetroCode: string
  pathLatencyMs: number
  /** Observed sum of per-hop RTT. Falls back to the contracted figure when partiallyCommitted. */
  measuredLatencyMs: number
  /** 0 when partiallyCommitted — absent, not zero. */
  measuredP95Ms: number
  /** 0 when partiallyCommitted — absent, not zero. */
  measuredJitterMs: number
  partiallyCommitted: boolean
  pathMetros: string[]
  hopCount: number
  bottleneckBwGbps: number
  internetLatencyMs: number
  improvementPct: number | null
}

export interface MetroPathLatencyResponse {
  optimize: PathOptimizeMode
  paths: MetroPathLatency[]
  summary: {
    totalPairs: number
    pairsWithInternet: number
    avgImprovementPct: number
    maxImprovementPct: number
  }
  error?: string
}

export async function fetchMetroPathLatency(optimize: PathOptimizeMode = 'latency'): Promise<MetroPathLatencyResponse> {
  const res = await apiFetch(`/api/topology/metro-path-latency?optimize=${optimize}`)
  if (!res.ok) {
    throw new Error('Failed to fetch metro path latency')
  }
  return res.json()
}

export interface RouteSeriesPoint {
  ts: string
  dzMs: number
  internetMs: number
}

export interface RouteSeries {
  fromMetroCode: string
  toMetroCode: string
  points: RouteSeriesPoint[]
}

export interface RouteSeriesResponse {
  series: RouteSeries[]
  error?: string
}

export async function fetchRouteSeries(pairs: string[]): Promise<RouteSeriesResponse> {
  const res = await apiFetch(`/api/topology/route-series?pairs=${encodeURIComponent(pairs.join(','))}`)
  if (!res.ok) {
    throw new Error('Failed to fetch route series')
  }
  return res.json()
}

// Metro path detail types (single path breakdown)
export interface MetroPathDetailHop {
  devicePK: string
  deviceCode: string
  metroPK: string
  metroCode: string
  linkMetric: number
  linkBwGbps: number
  linkLatency: number
}

export interface MetroPathDetailResponse {
  fromMetroCode: string
  toMetroCode: string
  optimize: PathOptimizeMode
  totalLatencyMs: number
  totalHops: number
  bottleneckBwGbps: number
  internetLatencyMs: number
  improvementPct: number | null
  hops: MetroPathDetailHop[]
  error?: string
}

export async function fetchMetroPathDetail(
  from: string,
  to: string,
  optimize: PathOptimizeMode = 'latency'
): Promise<MetroPathDetailResponse> {
  const res = await apiFetch(`/api/topology/metro-path-detail?from=${from}&to=${to}&optimize=${optimize}`)
  if (!res.ok) {
    throw new Error('Failed to fetch metro path detail')
  }
  return res.json()
}

// Metro paths types (for connectivity matrix detail)
export interface MetroPathsHop {
  devicePK: string
  deviceCode: string
  metroPK: string
  metroCode: string
}

export interface MetroPath {
  hops: MetroPathsHop[]
  totalHops: number
  totalMetric: number
  latencyMs: number
}

export interface MetroPathsResponse {
  fromMetroCode: string
  toMetroCode: string
  paths: MetroPath[]
  error?: string
}

export async function fetchMetroPaths(
  fromPK: string,
  toPK: string,
  k: number = 5
): Promise<MetroPathsResponse> {
  const res = await apiFetch(`/api/topology/metro-paths?from=${fromPK}&to=${toPK}&k=${k}`)
  if (!res.ok) {
    throw new Error('Failed to fetch metro paths')
  }
  return res.json()
}

// Maintenance planner types
export interface MaintenanceAffectedPath {
  source: string
  target: string
  sourceMetro: string
  targetMetro: string
  hopsBefore: number
  hopsAfter: number
  metricBefore: number
  metricAfter: number
  status: 'rerouted' | 'degraded' | 'disconnected'
}

export interface MaintenanceItem {
  type: 'device' | 'link'
  pk: string
  code: string
  impact: number
  disconnected: number
  causesPartition: boolean
  disconnectedDevices?: string[]
  affectedPaths?: MaintenanceAffectedPath[]
}

export interface AffectedLink {
  sourceDevice: string
  targetDevice: string
  status: 'offline' | 'rerouted'
}

export interface AffectedMetroPair {
  sourceMetro: string
  targetMetro: string
  affectedLinks: AffectedLink[]
  status: 'reduced' | 'degraded' | 'disconnected'
}

export interface MaintenanceImpactRequest {
  devices: string[]
  links: string[]
}

export interface MaintenanceImpactResponse {
  items: MaintenanceItem[]
  totalImpact: number
  totalDisconnected: number
  recommendedOrder: string[]
  affectedPaths?: MaintenanceAffectedPath[]
  affectedMetros?: AffectedMetroPair[]
  disconnectedList?: string[]
  error?: string
}

export async function fetchMaintenanceImpact(
  devices: string[],
  links: string[]
): Promise<MaintenanceImpactResponse> {
  const res = await apiFetch('/api/topology/maintenance-impact', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ devices, links }),
  })
  if (!res.ok) {
    throw new Error('Failed to analyze maintenance impact')
  }
  return res.json()
}

// What-if removal types (unified API for devices and links)
export interface WhatIfAffectedPath {
  source: string
  target: string
  sourceMetro?: string
  targetMetro?: string
  hopsBefore: number
  metricBefore: number
  hopsAfter: number
  metricAfter: number
  status: 'rerouted' | 'degraded' | 'disconnected'
}

export interface WhatIfRemovalItem {
  type: 'device' | 'link'
  pk: string
  code: string
  affectedPaths: WhatIfAffectedPath[]
  affectedPathCount: number
  disconnectedDevices: string[]
  disconnectedCount: number
  causesPartition: boolean
}

export interface WhatIfRemovalResponse {
  items: WhatIfRemovalItem[]
  totalAffectedPaths: number
  totalDisconnected: number
  affectedPaths?: WhatIfAffectedPath[]
  disconnectedList?: string[]
  error?: string
}

export async function fetchWhatIfRemoval(
  devices: string[],
  links: string[]
): Promise<WhatIfRemovalResponse> {
  const res = await apiFetch('/api/topology/whatif-removal', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ devices, links }),
  })
  if (!res.ok) {
    throw new Error('Failed to analyze what-if removal impact')
  }
  return res.json()
}

// Stake analytics types
export interface StakeOverview {
  dz_stake_sol: number
  total_stake_sol: number
  stake_share_pct: number
  validator_count: number
  dz_stake_sol_24h_ago: number
  stake_share_pct_24h_ago: number
  dz_stake_change_24h: number
  share_change_24h: number
  dz_stake_sol_7d_ago: number
  stake_share_pct_7d_ago: number
  dz_stake_change_7d: number
  share_change_7d: number
  fetched_at: string
  error?: string
}

export interface StakeHistoryPoint {
  timestamp: string
  dz_stake_sol: number
  total_stake_sol: number
  stake_share_pct: number
}

export interface StakeHistoryResponse {
  points: StakeHistoryPoint[]
  fetched_at: string
  error?: string
}

export async function fetchStakeOverview(): Promise<StakeOverview> {
  const res = await fetchWithRetry('/api/stake/overview')
  if (!res.ok) {
    throw new Error('Failed to fetch stake overview')
  }
  return res.json()
}

export async function fetchStakeHistory(
  range: '24h' | '7d' | '30d' = '7d',
  interval: '5m' | '15m' | '1h' | '6h' | '1d' = '1h'
): Promise<StakeHistoryResponse> {
  const params = new URLSearchParams({ range, interval })
  const res = await fetchWithRetry(`/api/stake/history?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch stake history')
  }
  return res.json()
}

// Stake changes (attribution)
export interface StakeChange {
  category: 'joined' | 'left' | 'stake_increase' | 'stake_decrease'
  vote_pubkey: string
  node_pubkey: string
  stake_sol: number
  stake_change_sol: number
  timestamp: string
  city?: string
  country?: string
}

export interface ChangeSummary {
  joined_count: number
  joined_stake_sol: number
  left_count: number
  left_stake_sol: number
  stake_increase_sol: number
  stake_decrease_sol: number
  net_change_sol: number
}

export interface StakeChangesResponse {
  changes: StakeChange[]
  summary: ChangeSummary
  range: string
  fetched_at: string
  error?: string
}

export async function fetchStakeChanges(
  range: '24h' | '7d' | '30d' = '24h'
): Promise<StakeChangesResponse> {
  const params = new URLSearchParams({ range })
  const res = await fetchWithRetry(`/api/stake/changes?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch stake changes')
  }
  return res.json()
}

// Stake validators
export interface StakeValidator {
  vote_pubkey: string
  node_pubkey: string
  stake_sol: number
  stake_share_pct: number
  commission: number
  version: string
  city: string
  country: string
  on_dz: boolean
  device_code?: string
  metro_code?: string
}

export interface StakeValidatorsResponse {
  validators: StakeValidator[]
  total_count: number
  on_dz_count: number
  total_stake_sol: number
  dz_stake_sol: number
  fetched_at: string
  error?: string
}

export async function fetchStakeValidators(
  filter: 'all' | 'on_dz' | 'off_dz' = 'on_dz',
  limit: number = 100
): Promise<StakeValidatorsResponse> {
  const params = new URLSearchParams({ filter, limit: limit.toString() })
  const res = await fetchWithRetry(`/api/stake/validators?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch stake validators')
  }
  return res.json()
}

// Traffic analytics types and functions
export interface TrafficPoint {
  time: string
  device_pk: string
  device: string
  intf: string
  in_bps: number
  out_bps: number
  in_discards: number
  out_discards: number
  in_errors: number
  out_errors: number
  in_fcs_errors: number
  carrier_transitions: number
}

export interface SeriesInfo {
  key: string
  device: string
  intf: string
  direction: string
  mean: number
  link_pk?: string
  cyoa_type?: string
}

export interface TrafficDataResponse {
  points: TrafficPoint[]
  series: SeriesInfo[]
  discards_series: DiscardSeriesInfo[]
  effective_bucket: string
  truncated: boolean
}

export async function fetchTrafficData(
  timeRange: string = '6h',
  tunnelOnly: boolean | null = null,
  bucket: string = 'auto',
  agg: string = 'max',
  filters?: Record<string, string>,
  metric?: string
): Promise<TrafficDataResponse> {
  const hasCustomRange = filters?.start_time && filters?.end_time
  const params = new URLSearchParams({ bucket, agg })
  if (tunnelOnly !== null) {
    params.set('tunnel_only', String(tunnelOnly))
  }
  if (metric && metric !== 'throughput') {
    params.set('metric', metric)
  }
  if (!hasCustomRange) {
    params.set('time_range', timeRange)
  }
  if (filters) {
    for (const [k, v] of Object.entries(filters)) {
      if (v && k !== 'time_range' && k !== 'threshold') params.set(k, v)
    }
  }
  const res = await fetchWithRetry(`/api/traffic/data?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch traffic data')
  }
  return res.json()
}

// Link latency analytics types and functions
export interface LinkLatencySummary {
  link_pk: string
  link_code: string
  link_type: string
  link_status: string
  provisioning: boolean
  isis_down: boolean
  contributor_code: string
  side_a_code: string
  side_z_code: string
  committed_rtt_ms: number
  committed_jitter_ms: number
  rtt_a_to_z_ms: number
  rtt_z_to_a_ms: number
  jitter_a_to_z_ms: number
  jitter_z_to_a_ms: number
  loss_a_pct: number
  loss_z_pct: number
  samples: number
}

export interface LinkLatencySummaryResponse {
  links: LinkLatencySummary[]
}

export async function fetchLinkLatencySummary(
  timeRange: string = '24h',
  agg: string = 'avg',
  filters?: Record<string, string>,
  showExcluded = false,
): Promise<LinkLatencySummaryResponse> {
  const hasCustomRange = filters?.start_time && filters?.end_time
  const params = new URLSearchParams({ agg })
  if (!hasCustomRange) {
    params.set('time_range', timeRange)
  }
  if (showExcluded) {
    params.set('show_excluded', 'true')
  }
  if (filters) {
    for (const [k, v] of Object.entries(filters)) {
      if (v && k !== 'time_range' && k !== 'threshold') params.set(k, v)
    }
  }
  const res = await fetchWithRetry(`/api/performance/link-latency?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch link latency summary')
  }
  return res.json()
}

// Multi-link latency history types and functions
export interface MultiLinkLatencyPoint {
  time: string
  link_pk: string
  link_code: string
  rtt_a_to_z_ms: number
  rtt_z_to_a_ms: number
  jitter_a_to_z_ms: number
  jitter_z_to_a_ms: number
  loss_pct: number
}

export interface MultiLinkLatencyResponse {
  points: MultiLinkLatencyPoint[]
}

export async function fetchMultiLinkLatencyHistory(
  opts: {
    mode: 'aggregate' | 'per_link'
    pks?: string[]
    timeRange?: string
    agg?: string
    filters?: Record<string, string>
  },
): Promise<MultiLinkLatencyResponse> {
  const params = new URLSearchParams({ mode: opts.mode, agg: opts.agg || 'avg' })
  if (opts.pks?.length) {
    params.set('pks', opts.pks.join(','))
  }
  const hasCustomRange = opts.filters?.start_time && opts.filters?.end_time
  if (!hasCustomRange && opts.timeRange) {
    params.set('range', opts.timeRange)
  }
  if (opts.filters) {
    for (const [k, v] of Object.entries(opts.filters)) {
      if (v && k !== 'time_range' && k !== 'threshold') params.set(k, v)
    }
  }
  const res = await fetchWithRetry(`/api/performance/link-latency/history?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch multi-link latency history')
  }
  return res.json()
}

// Discards analytics types and functions
export interface DiscardsPoint {
  time: string
  device_pk: string
  device: string
  intf: string
  in_discards: number
  out_discards: number
}

export interface DiscardSeriesInfo {
  key: string
  device: string
  intf: string
  total: number
}

export interface DiscardsDataResponse {
  points: DiscardsPoint[]
  series: DiscardSeriesInfo[]
}

export async function fetchDiscardsData(
  timeRange: string = '12h',
  bucket: string = 'auto',
  filters?: Record<string, string>
): Promise<DiscardsDataResponse> {
  const hasCustomRange = filters?.start_time && filters?.end_time
  const params = new URLSearchParams({
    bucket: bucket
  })
  if (!hasCustomRange) {
    params.set('time_range', timeRange)
  }
  if (filters) {
    for (const [k, v] of Object.entries(filters)) {
      if (v && k !== 'time_range' && k !== 'threshold') params.set(k, v)
    }
  }
  const res = await fetchWithRetry(`/api/traffic/discards?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch discards data')
  }
  return res.json()
}

// Traffic Dashboard types and functions

export interface DashboardStressResponse {
  timestamps: string[]
  p50_in: number[]
  p95_in: number[]
  max_in: number[]
  p50_out: number[]
  p95_out: number[]
  max_out: number[]
  stressed_count: number[]
  total_count: number[]
  effective_bucket: string
  groups?: DashboardStressGroup[]
}

export interface DashboardStressGroup {
  key: string
  label: string
  p50_in: number[]
  p95_in: number[]
  max_in: number[]
  p50_out: number[]
  p95_out: number[]
  max_out: number[]
  stressed_count: number[]
}

export interface DashboardStressParams {
  time_range?: string
  bucket?: string
  threshold?: number
  group_by?: string
  metric?: 'utilization' | 'throughput' | 'packets'
  metro?: string
  device?: string
  link_type?: string
  contributor?: string
}

export async function fetchDashboardStress(
  params: DashboardStressParams = {}
): Promise<DashboardStressResponse> {
  const searchParams = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== '') {
      searchParams.set(key, String(value))
    }
  }
  const res = await fetchWithRetry(`/api/traffic/dashboard/stress?${searchParams}`)
  if (!res.ok) throw new Error('Failed to fetch dashboard stress data')
  return res.json()
}

export interface DashboardTopEntity {
  device_pk: string
  device_code: string
  intf: string
  metro_code: string
  link_type: string
  contributor_code: string
  bandwidth_bps: number
  max_util: number
  avg_util: number
  p95_util: number
  max_in_bps: number
  max_out_bps: number
}

export interface DashboardTopResponse {
  entities: DashboardTopEntity[]
}

export interface DashboardTopParams {
  time_range?: string
  entity?: 'device' | 'interface'
  metric?: 'max_util' | 'p95_util' | 'avg_util' | 'max_throughput' | 'max_in_bps' | 'max_out_bps' | 'bandwidth_bps' | 'headroom'
  dir?: 'asc' | 'desc'
  limit?: number
  metro?: string
  device?: string
  link_type?: string
  contributor?: string
}

export async function fetchDashboardTop(
  params: DashboardTopParams = {}
): Promise<DashboardTopResponse> {
  const searchParams = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== '') {
      searchParams.set(key, String(value))
    }
  }
  const res = await fetchWithRetry(`/api/traffic/dashboard/top?${searchParams}`)
  if (!res.ok) throw new Error('Failed to fetch dashboard top data')
  return res.json()
}

export interface DashboardDrilldownPoint {
  time: string
  intf: string
  in_bps: number
  out_bps: number
  in_discards: number
  out_discards: number
  in_pps: number
  out_pps: number
}

export interface DashboardDrilldownSeries {
  intf: string
  bandwidth_bps: number
  link_type: string
}

export interface DashboardDrilldownResponse {
  points: DashboardDrilldownPoint[]
  series: DashboardDrilldownSeries[]
  effective_bucket: string
}

export interface DashboardDrilldownParams {
  time_range?: string
  start_time?: string
  end_time?: string
  bucket?: string
  device_pk: string
  intf?: string
  intf_type?: string
}

export async function fetchDashboardDrilldown(
  params: DashboardDrilldownParams
): Promise<DashboardDrilldownResponse> {
  const searchParams = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== '') {
      searchParams.set(key, String(value))
    }
  }
  const res = await fetchWithRetry(`/api/traffic/dashboard/drilldown?${searchParams}`)
  if (!res.ok) throw new Error('Failed to fetch dashboard drilldown data')
  return res.json()
}

export interface DashboardBurstinessEntity {
  device_pk: string
  device_code: string
  intf: string
  metro_code: string
  contributor_code: string
  bandwidth_bps: number
  p50_bps: number
  p95_bps: number
  spike_count: number
  total_buckets: number
  max_spike_bps: number
  max_spike_ratio: number
  last_spike_time: string
  peak_direction: 'rx' | 'tx'
}

export interface DashboardBurstinessResponse {
  entities: DashboardBurstinessEntity[]
  total: number
}

export interface DashboardBurstinessParams {
  time_range?: string
  sort?: 'spike_count' | 'max_spike_ratio' | 'p50_bps' | 'max_spike_bps'
  dir?: 'asc' | 'desc'
  limit?: number
  offset?: number
  threshold?: number
  min_bps?: number
  min_peak_bps?: number
  intf_type?: string
  metro?: string
  device?: string
  link_type?: string
  contributor?: string
}

export async function fetchDashboardBurstiness(
  params: DashboardBurstinessParams = {}
): Promise<DashboardBurstinessResponse> {
  const searchParams = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== '') {
      searchParams.set(key, String(value))
    }
  }
  const res = await fetchWithRetry(`/api/traffic/dashboard/burstiness?${searchParams}`)
  if (!res.ok) throw new Error('Failed to fetch dashboard burstiness data')
  return res.json()
}

// Dashboard health types and functions

export interface DashboardHealthEntity {
  device_pk: string
  device_code: string
  intf: string
  metro_code: string
  contributor_code: string
  total_errors: number
  total_discards: number
  total_fcs_errors: number
  total_carrier_transitions: number
  total_events: number
}

export interface DashboardHealthResponse {
  entities: DashboardHealthEntity[]
}

export interface DashboardHealthParams {
  time_range?: string
  sort?: 'total_events' | 'total_errors' | 'total_discards' | 'total_fcs_errors' | 'total_carrier_transitions'
  dir?: 'asc' | 'desc'
  limit?: number
  metro?: string
  device?: string
  link_type?: string
  contributor?: string
  intf?: string
  intf_type?: string
  start_time?: string
  end_time?: string
  threshold?: string
}

export async function fetchDashboardHealth(
  params: DashboardHealthParams = {}
): Promise<DashboardHealthResponse> {
  const searchParams = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== '') {
      searchParams.set(key, String(value))
    }
  }
  const res = await fetchWithRetry(`/api/traffic/dashboard/health?${searchParams}`)
  if (!res.ok) throw new Error('Failed to fetch dashboard health data')
  return res.json()
}

// Search types and functions
export type SearchEntityType = 'device' | 'link' | 'metro' | 'contributor' | 'user' | 'validator' | 'gossip' | 'multicast'

export interface SearchSuggestion {
  type: SearchEntityType
  id: string
  label: string
  sublabel: string
  url: string
}

export interface AutocompleteResponse {
  suggestions: SearchSuggestion[]
}

export interface SearchResultGroup {
  items: SearchSuggestion[]
  total: number
}

export interface SearchResponse {
  query: string
  results: Partial<Record<SearchEntityType, SearchResultGroup>>
}

export async function fetchAutocomplete(query: string, limit = 10): Promise<AutocompleteResponse> {
  const params = new URLSearchParams({ q: query, limit: limit.toString() })
  const res = await fetchWithRetry(`/api/search/autocomplete?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch autocomplete')
  }
  return res.json()
}

export async function fetchSearch(
  query: string,
  types?: SearchEntityType[],
  limit = 20
): Promise<SearchResponse> {
  const params = new URLSearchParams({ q: query, limit: limit.toString() })
  if (types && types.length > 0) {
    params.set('types', types.join(','))
  }
  const res = await fetchWithRetry(`/api/search?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch search')
  }
  return res.json()
}

// Incidents types and functions
export type IncidentTimeRange = '3h' | '6h' | '12h' | '24h' | '3d' | '7d' | '30d'
export type IncidentType = 'packet_loss' | 'errors' | 'fcs' | 'discards' | 'carrier' | 'no_data' | 'isis_down'

export interface LinkIncident {
  id: string
  link_pk: string
  link_code: string
  link_type: string
  side_a_metro: string
  side_z_metro: string
  contributor_code: string
  incident_type: IncidentType
  threshold_pct?: number
  peak_loss_pct?: number
  threshold_count?: number
  peak_count?: number
  started_at: string
  ended_at?: string
  duration_seconds?: number
  is_ongoing: boolean
  confirmed: boolean
  is_drained: boolean
  severity: 'degraded' | 'incident'
  affected_interfaces?: string[]
}

export interface DrainedLinkInfo {
  link_pk: string
  link_code: string
  link_type: string
  side_a_metro: string
  side_z_metro: string
  contributor_code: string
  drain_status: string
  drained_since: string
  active_incidents: LinkIncident[]
  recent_incidents: LinkIncident[]
  last_incident_end?: string
  clear_for_seconds?: number
  readiness: 'red' | 'yellow' | 'green' | 'gray'
}

export interface LinkIncidentsSummary {
  total: number
  ongoing: number
  by_type: Record<string, number>
}

export interface DrainedSummary {
  total: number
  with_incidents: number
  ready: number
  not_ready: number
}

export interface LinkIncidentsResponse {
  active: LinkIncident[]
  drained: DrainedLinkInfo[]
  active_summary: LinkIncidentsSummary
  drained_summary: DrainedSummary
}

export interface FetchLinkIncidentsParams {
  range?: IncidentTimeRange
  threshold?: number
  errors_threshold?: number
  fcs_threshold?: number
  discards_threshold?: number
  carrier_threshold?: number
  min_duration?: number
  coalesce_gap?: number
  type?: 'all' | IncidentType
  filter?: string
}

export async function fetchLinkIncidents(params: FetchLinkIncidentsParams = {}): Promise<LinkIncidentsResponse> {
  const searchParams = new URLSearchParams()
  if (params.range) searchParams.set('range', params.range)
  if (params.threshold) searchParams.set('threshold', params.threshold.toString())
  if (params.errors_threshold) searchParams.set('errors_threshold', params.errors_threshold.toString())
  if (params.fcs_threshold) searchParams.set('fcs_threshold', params.fcs_threshold.toString())
  if (params.discards_threshold) searchParams.set('discards_threshold', params.discards_threshold.toString())
  if (params.carrier_threshold) searchParams.set('carrier_threshold', params.carrier_threshold.toString())
  if (params.min_duration) searchParams.set('min_duration', params.min_duration.toString())
  if (params.coalesce_gap != null) searchParams.set('coalesce_gap', params.coalesce_gap.toString())
  if (params.type) searchParams.set('type', params.type)
  if (params.filter) searchParams.set('filter', params.filter)

  const queryString = searchParams.toString()
  const url = `/api/incidents/links${queryString ? `?${queryString}` : ''}`

  const res = await fetchWithRetry(url)
  if (!res.ok) {
    throw new Error('Failed to fetch link incidents')
  }
  return res.json()
}

// Device incident types
export type DeviceIncidentType = 'errors' | 'fcs' | 'discards' | 'carrier' | 'no_data' | 'isis_overload' | 'isis_unreachable'

export interface DeviceIncident {
  id: string
  device_pk: string
  device_code: string
  device_type: string
  metro: string
  contributor_code: string
  incident_type: DeviceIncidentType
  threshold_count?: number
  peak_count?: number
  started_at: string
  ended_at?: string
  duration_seconds?: number
  is_ongoing: boolean
  confirmed: boolean
  is_drained: boolean
  severity: 'degraded' | 'incident'
  affected_interfaces?: string[]
}

export interface DrainedDeviceInfo {
  device_pk: string
  device_code: string
  device_type: string
  metro: string
  contributor_code: string
  drain_status: string
  drained_since: string
  active_incidents: DeviceIncident[]
  recent_incidents: DeviceIncident[]
  last_incident_end?: string
  clear_for_seconds?: number
  readiness: 'red' | 'yellow' | 'green' | 'gray'
}

export interface DeviceIncidentsSummary {
  total: number
  ongoing: number
  by_type: Record<string, number>
}

export interface DeviceIncidentsResponse {
  active: DeviceIncident[]
  drained: DrainedDeviceInfo[]
  active_summary: DeviceIncidentsSummary
  drained_summary: DrainedSummary
}

export interface FetchDeviceIncidentsParams {
  range?: IncidentTimeRange
  errors_threshold?: number
  fcs_threshold?: number
  discards_threshold?: number
  carrier_threshold?: number
  min_duration?: number
  coalesce_gap?: number
  type?: 'all' | DeviceIncidentType
  filter?: string
  link_interfaces?: boolean
}

export async function fetchDeviceIncidents(params: FetchDeviceIncidentsParams = {}): Promise<DeviceIncidentsResponse> {
  const searchParams = new URLSearchParams()
  if (params.range) searchParams.set('range', params.range)
  if (params.errors_threshold) searchParams.set('errors_threshold', params.errors_threshold.toString())
  if (params.fcs_threshold) searchParams.set('fcs_threshold', params.fcs_threshold.toString())
  if (params.discards_threshold) searchParams.set('discards_threshold', params.discards_threshold.toString())
  if (params.carrier_threshold) searchParams.set('carrier_threshold', params.carrier_threshold.toString())
  if (params.min_duration) searchParams.set('min_duration', params.min_duration.toString())
  if (params.coalesce_gap != null) searchParams.set('coalesce_gap', params.coalesce_gap.toString())
  if (params.type) searchParams.set('type', params.type)
  if (params.filter) searchParams.set('filter', params.filter)
  if (params.link_interfaces) searchParams.set('link_interfaces', 'true')

  const queryString = searchParams.toString()
  const url = `/api/incidents/devices${queryString ? `?${queryString}` : ''}`

  const res = await fetchWithRetry(url)
  if (!res.ok) {
    throw new Error('Failed to fetch device incidents')
  }
  return res.json()
}

// Auth types and functions
export type AccountType = 'domain' | 'wallet'

export interface Account {
  id: string
  account_type: AccountType
  wallet_address?: string
  email?: string
  email_domain?: string
  display_name?: string
  sol_balance?: number
  sol_balance_updated_at?: string
  is_active: boolean
  is_internal_user: boolean
  created_at: string
  updated_at: string
  last_login_at?: string
}

export interface QuotaInfo {
  remaining: number | null  // null = unlimited
  limit: number | null      // null = unlimited
  resets_at: string         // ISO timestamp
}

export interface AuthMeResponse {
  account: Account | null
  quota: QuotaInfo | null
}

export interface WalletNonceResponse {
  nonce: string
}

export interface WalletAuthResponse {
  token: string
  account: Account
}

export interface GoogleAuthResponse {
  token: string
  account: Account
}

// Get current user and quota
export class AuthError extends Error {
  status: number
  constructor(message: string, status: number) {
    super(message)
    this.name = 'AuthError'
    this.status = status
  }
}

export async function fetchAuthMe(): Promise<AuthMeResponse> {
  const res = await fetchWithRetry('/api/auth/me')
  if (!res.ok) {
    throw new AuthError('Failed to get auth status', res.status)
  }
  return res.json()
}

// Logout
export async function logout(): Promise<void> {
  try {
    await fetchWithRetry('/api/auth/logout', { method: 'POST' })
  } finally {
    clearAuthToken()
  }
}

// Get nonce for wallet signing
export async function getWalletNonce(): Promise<string> {
  const res = await apiFetch('/api/auth/nonce')
  if (!res.ok) {
    throw new Error('Failed to get nonce')
  }
  const data: WalletNonceResponse = await res.json()
  return data.nonce
}

// Authenticate with wallet signature
export async function authenticateWallet(
  publicKey: string,
  signature: string,
  message: string
): Promise<WalletAuthResponse> {
  // Include anonymous_id for session migration
  const anonymousId = localStorage.getItem(ANONYMOUS_ID_KEY)

  const res = await apiFetch('/api/auth/wallet', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      public_key: publicKey,
      signature,
      message,
      anonymous_id: anonymousId,
    }),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || 'Wallet authentication failed')
  }
  const data: WalletAuthResponse = await res.json()
  setAuthToken(data.token)
  return data
}

// Authenticate with Google ID token
export async function authenticateGoogle(idToken: string): Promise<GoogleAuthResponse> {
  // Include anonymous_id for session migration
  const anonymousId = localStorage.getItem(ANONYMOUS_ID_KEY)

  const res = await apiFetch('/api/auth/google', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      id_token: idToken,
      anonymous_id: anonymousId,
    }),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || 'Google authentication failed')
  }
  const data: GoogleAuthResponse = await res.json()
  setAuthToken(data.token)
  return data
}

// Get current quota
export async function fetchQuota(): Promise<QuotaInfo> {
  const res = await fetchWithRetry('/api/usage/quota')
  if (!res.ok) {
    throw new Error('Failed to get quota')
  }
  return res.json()
}

// Build SIWS message for signing
export function buildSIWSMessage(nonce: string): string {
  return `Sign this message to authenticate with DoubleZero Data.\n\nNonce: ${nonce}`
}

// Slack installations
export interface SlackInstallation {
  id: string
  team_id: string
  team_name?: string
  bot_user_id: string
  is_active: boolean
  installed_at: string
  updated_at: string
}

export async function getSlackInstallations(): Promise<SlackInstallation[]> {
  const res = await fetchWithRetry('/api/slack/installations')
  if (!res.ok) {
    throw new Error('Failed to get Slack installations')
  }
  return res.json()
}

export async function confirmSlackInstallation(pendingId: string): Promise<{ status: string; team_id: string; team_name: string }> {
  const res = await fetchWithRetry(`/api/slack/installations/confirm/${encodeURIComponent(pendingId)}`, {
    method: 'POST',
  })
  if (!res.ok) {
    throw new Error('Failed to confirm Slack installation')
  }
  return res.json()
}

export async function removeSlackInstallation(teamId: string): Promise<void> {
  const res = await fetchWithRetry(`/api/slack/installations/${encodeURIComponent(teamId)}`, {
    method: 'DELETE',
  })
  if (!res.ok) {
    throw new Error('Failed to remove Slack installation')
  }
}

// Field values for autocomplete
export interface FieldValuesResponse {
  values: string[]
}

export async function fetchFieldValues(entity: string, field: string, filters?: Record<string, string>): Promise<string[]> {
  const params = new URLSearchParams({ entity, field })
  if (filters) {
    for (const [key, value] of Object.entries(filters)) {
      if (value) params.set(key, value)
    }
  }
  const res = await fetchWithRetry(`/api/dz/field-values?${params}`)
  if (!res.ok) {
    return []
  }
  const data: FieldValuesResponse = await res.json()
  return data.values
}

export interface Tenant {
  pk: string
  owner_pubkey: string
  code: string
  payment_status: string
  vrf_id: number
  metro_routing: boolean
  route_liveness: boolean
  billing_rate: number
  include_topologies: string[]
}

export async function fetchTenants(
  limit = 100,
  offset = 0,
  sortBy?: string,
  sortDir?: 'asc' | 'desc',
  filters?: string[]
): Promise<PaginatedResponse<Tenant>> {
  const params = new URLSearchParams()
  params.set('limit', String(limit))
  params.set('offset', String(offset))
  if (sortBy) params.set('sort_by', sortBy)
  if (sortDir) params.set('sort_dir', sortDir)
  if (filters) filters.forEach(f => params.append('filters', f))
  const res = await fetchWithRetry(`/api/dz/tenants?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch tenants')
  }
  return res.json()
}

export async function fetchTenant(pk: string): Promise<Tenant> {
  const res = await fetchWithRetry(`/api/dz/tenants/${encodeURIComponent(pk)}`)
  if (!res.ok) {
    throw new Error('Failed to fetch tenant')
  }
  return res.json()
}

// Shreds Subscription Program

export interface ShredsOverview {
  phase: string
  current_subscription_epoch: number
  total_metros: number
  total_enabled_devices: number
  total_client_seats: number
  settled_devices_count: number
  settled_client_seats_count: number
  next_seat_funding_index: number
  current_solana_epoch: number
  client_seat_count: number
  payment_escrow_count: number
  metro_history_count: number
  device_history_count: number
  validator_client_reward_count: number
}

export async function fetchShredsOverview(): Promise<ShredsOverview> {
  const res = await fetchWithRetry('/api/dz/shreds/overview')
  if (!res.ok) {
    throw new Error('Failed to fetch shreds overview')
  }
  return res.json()
}

export interface ShredClientSeat {
  pk: string
  device_key: string
  device_code: string
  metro_pk: string
  metro_code: string
  client_ip: string
  tenure_epochs: number
  funded_epoch: number
  active_epoch: number
  has_price_override: number
  override_usdc_price_dollars: number
  escrow_count: number
  // Largest single escrow balance (micro-USDC). Activation/renewal is per-escrow,
  // so this — not the sum — decides whether the seat covers the per-epoch price.
  spendable_usdc_balance: number
  // Sum across all escrows (micro-USDC); informational, cannot be spent as one charge.
  all_escrows_usdc_balance: number
  price_per_epoch_dollars: number
  funding_authority_key: string
  user_pk: string
  user_owner_pubkey: string
  user_status: string
  last_activity: string
}

export interface FetchShredClientSeatsParams {
  limit?: number
  offset?: number
  sortBy?: string
  sortDir?: 'asc' | 'desc'
  status?: string
  filters?: string[]
}

export async function fetchShredClientSeats(
  params: FetchShredClientSeatsParams = {},
): Promise<PaginatedResponse<ShredClientSeat>> {
  const q = new URLSearchParams()
  if (params.limit) q.set('limit', String(params.limit))
  if (params.offset) q.set('offset', String(params.offset))
  if (params.sortBy) q.set('sort_by', params.sortBy)
  if (params.sortDir) q.set('sort_dir', params.sortDir)
  if (params.status) q.set('status', params.status)
  if (params.filters) {
    for (const f of params.filters) {
      q.append('filters', f)
    }
  }
  const res = await fetchWithRetry(`/api/dz/shreds/client-seats?${q}`)
  if (!res.ok) {
    throw new Error('Failed to fetch shred client seats')
  }
  return res.json()
}

export interface ShredFunder {
  funding_authority_key: string
  total_seats: number
  active_seats: number
  inactive_seats: number
  closed_seats: number
  total_escrows: number
  unique_devices: number
}

export async function fetchShredFunders(): Promise<ShredFunder[]> {
  const res = await fetchWithRetry('/api/dz/shreds/funders')
  if (!res.ok) {
    throw new Error('Failed to fetch shred funders')
  }
  return res.json()
}

// Escrow Events
export interface ShredEscrowEvent {
  event_ts: string
  escrow_pk: string
  client_seat_pk: string
  tx_signature: string
  slot: number
  event_type: string
  amount_usdc: number | null
  balance_after_usdc: number | null
  epoch: number | null
  status: string
  signer: string
  client_ip: string
  solscan_url: string
}

export interface FetchShredEscrowEventsParams {
  limit?: number
  offset?: number
  sortBy?: string
  sortDir?: 'asc' | 'desc'
  range?: string
  startTime?: number
  endTime?: number
  filters?: string[]
  includeInternal?: boolean
}

export async function fetchShredEscrowEvents(
  params: FetchShredEscrowEventsParams = {},
): Promise<PaginatedResponse<ShredEscrowEvent>> {
  const q = new URLSearchParams()
  if (params.limit) q.set('limit', String(params.limit))
  if (params.offset) q.set('offset', String(params.offset))
  if (params.sortBy) q.set('sort_by', params.sortBy)
  if (params.sortDir) q.set('sort_dir', params.sortDir)
  if (params.range) q.set('range', params.range)
  if (params.startTime) q.set('start_time', String(params.startTime))
  if (params.endTime) q.set('end_time', String(params.endTime))
  if (params.filters) {
    for (const f of params.filters) {
      q.append('filters', f)
    }
  }
  if (params.includeInternal) q.set('include_internal', 'true')
  const res = await fetchWithRetry(`/api/dz/shreds/escrow-events?${q}`)
  if (!res.ok) {
    throw new Error('Failed to fetch shred escrow events')
  }
  return res.json()
}

export interface ShredEpochRevenue {
  epoch: number
  total_usdc: number
  total_dollars: number
  payment_count: number
}

export async function fetchShredEpochRevenue(limit = 20): Promise<ShredEpochRevenue[]> {
  const res = await fetchWithRetry(`/api/dz/shreds/epoch-revenue?limit=${limit}`)
  if (!res.ok) throw new Error('Failed to fetch shred epoch revenue')
  return res.json()
}

export interface ShredSubscriberHistory {
  epoch: number
  active_seats: number
}

export async function fetchShredSubscriberHistory(limit = 50): Promise<ShredSubscriberHistory[]> {
  const res = await fetchWithRetry(`/api/dz/shreds/subscriber-history?limit=${limit}`)
  if (!res.ok) throw new Error('Failed to fetch shred subscriber history')
  return res.json()
}

export interface SwapRate {
  sol_price_usd: number
  twoz_price_usd: number
  swap_rate: number
  fetched_at: number
}

export async function fetchSwapRate(): Promise<SwapRate> {
  const res = await fetchWithRetry('/api/dz/swap-rate')
  if (!res.ok) throw new Error('Failed to fetch swap rate')
  return res.json()
}

// Publisher Check
export interface PublisherCheckItem {
  publisher_ip: string
  client_ip: string
  node_pubkey: string
  vote_pubkey: string
  dz_user_pubkey: string
  dz_device_code: string
  dz_metro_code: string
  activated_stake: number
  multicast_connected: boolean
  publishing_leader_shreds: boolean
  publishing_retransmitted: boolean
  leader_slots: number
  total_slots: number
  total_unique_shreds: number
  slots_needing_repair: number
  validator_client: string
  validator_version: string
  validator_name: string
  validator_version_ok: boolean
  is_backup: boolean
}

export interface PublisherCheckResponse {
  epoch: number
  max_slot: number
  total_network_stake: number
  total_publishers: number
  total_publisher_stake: number
  publishers: PublisherCheckItem[]
}

// Ledger telemetry for a Solana-compatible chain (DZ or Solana)
export interface LedgerResponse {
  epoch: number
  slot_index: number
  slots_in_epoch: number
  epoch_pct: number
  epoch_eta_sec: number
  absolute_slot: number
  block_height: number
  transaction_count: number
  skip_rate: number
  tps: number
  total_supply: number
  circulating_supply: number
  inflation_total: number
  inflation_validator: number
  inflation_foundation: number
  active_validators: number
  delinquent_validators: number
  total_stake_sol: number
  node_version: string
  error?: string
}

export async function fetchDZLedger(): Promise<LedgerResponse> {
  const res = await apiFetch('/api/dz/ledger')
  if (!res.ok) throw new Error('Failed to fetch DZ ledger')
  return res.json()
}

export async function fetchSolanaLedger(): Promise<LedgerResponse> {
  const res = await apiFetch('/api/solana/ledger')
  if (!res.ok) throw new Error('Failed to fetch Solana ledger')
  return res.json()
}

export interface ValidatorPerfGroup {
  validator_count: number
  avg_vote_lag: number
  avg_skip_rate: number
  delinquent_count: number
  total_stake_sol: number
}

export interface ValidatorPerfResponse {
  on_dz: ValidatorPerfGroup
  off_dz: ValidatorPerfGroup
  error?: string
}

export async function fetchValidatorPerformance(): Promise<ValidatorPerfResponse> {
  const res = await apiFetch('/api/solana/validator-performance')
  if (!res.ok) throw new Error('Failed to fetch validator performance')
  return res.json()
}

export async function fetchPublisherCheck(q?: string, epochs?: number, slots?: number): Promise<PublisherCheckResponse> {
  const params = new URLSearchParams()
  if (q) params.set('q', q)
  if (slots) {
    params.set('slots', String(slots))
  } else if (epochs && epochs !== 2) {
    params.set('epochs', String(epochs))
  }
  const qs = params.toString()
  const res = await apiFetch(`/api/dz/publisher-check${qs ? `?${qs}` : ''}`)
  if (!res.ok) {
    throw new Error('Failed to fetch publisher check')
  }
  return res.json()
}

// Edge Scoreboard

export interface EdgeScoreboardLeadTime {
  loser_feed: string
  p50_ms: number
  p95_ms: number
  slot_count: number
}

export interface EdgeScoreboardFeedStats {
  shreds_won: number
  total_shreds: number
  win_rate_pct: number
  lead_times: EdgeScoreboardLeadTime[]
}

export interface EdgeScoreboardNode {
  host: string
  location: string
  metro_name: string
  latitude: number
  longitude: number
  feeds: Record<string, EdgeScoreboardFeedStats>
  stake_sol: number
  validators: number
  total_slots: number
  slots_observed: number
  dz_leader_slots: number
  last_updated: string
  name?: string
  gossip_pubkey?: string
  gossip_ip?: string
  asn?: number
  asn_org?: string
  city?: string
  country?: string
}

export interface EdgeScoreboardResponse {
  window: string
  leaders_only: boolean
  generated_at: string
  current_epoch: number
  current_slot: number
  total_slots: number
  global_total_slots: number
  dz_slots: number
  total_dz_leader_slots: number
  completeness_pct: number
  publisher_count: number
  publishing_count: number
  publishing_stake_pct: number
  nodes: EdgeScoreboardNode[]
  recent_slots: EdgeScoreboardSlotRace[]
  slot_buckets?: EdgeScoreboardSlotBucket[]
  slot_bucket_size?: number
  slot_leaders?: Record<string, EdgeScoreboardLeader>
  data_lag_ms?: number
}

export interface EdgeScoreboardLeader {
  name?: string
  pubkey: string
  ip?: string
  asn_org?: string
  city?: string
  country?: string
}

export interface EdgeScoreboardSlotRace {
  host: string
  slot: number
  feed: string
  shreds_won: number
  win_pct: number
}

export interface EdgeScoreboardSlotBucket {
  host: string
  slot_bucket: number
  feed: string
  feed_won: number    // sum of shreds_won for this feed in bucket (integer)
  bucket_total: number // sum of shreds_won across all feeds in bucket (integer)
}

export async function fetchEdgeScoreboard(
  window: string = '1h',
  leadersOnly: boolean = true,
  opts?: { sinceSlot?: number; limit?: number; beforeSlot?: number }
): Promise<EdgeScoreboardResponse> {
  const params = new URLSearchParams()
  params.set('window', window)
  params.set('leaders_only', leadersOnly ? 'true' : 'false')
  if (opts?.sinceSlot) params.set('since_slot', String(opts.sinceSlot))
  if (opts?.beforeSlot) params.set('before_slot', String(opts.beforeSlot))
  if (opts?.limit) params.set('limit', String(opts.limit))
  const res = await apiFetch(`/api/dz/edge/scoreboard?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch edge scoreboard')
  }
  return res.json()
}

// --- Unified Metrics Types ---

export interface EntityStatusChange {
  previous_status: string
  new_status: string
  changed_ts: string
}

export interface LinkMetricsStatus {
  health: string
  drain_status: string
  provisioning: boolean
  isis_down: boolean
  collecting: boolean
  reasons?: string[]
}

export interface LinkMetricsLatency {
  a_avg_rtt_us: number
  a_min_rtt_us: number
  a_p50_rtt_us: number
  a_p90_rtt_us: number
  a_p95_rtt_us: number
  a_p99_rtt_us: number
  a_max_rtt_us: number
  a_loss_pct: number
  a_samples: number
  z_avg_rtt_us: number
  z_min_rtt_us: number
  z_p50_rtt_us: number
  z_p90_rtt_us: number
  z_p95_rtt_us: number
  z_p99_rtt_us: number
  z_max_rtt_us: number
  z_loss_pct: number
  z_samples: number
  a_avg_jitter_us: number
  a_min_jitter_us: number
  a_p50_jitter_us: number
  a_p90_jitter_us: number
  a_p95_jitter_us: number
  a_p99_jitter_us: number
  a_max_jitter_us: number
  z_avg_jitter_us: number
  z_min_jitter_us: number
  z_p50_jitter_us: number
  z_p90_jitter_us: number
  z_p95_jitter_us: number
  z_p99_jitter_us: number
  z_max_jitter_us: number
}

export interface LinkMetricsTraffic {
  side_a_in_bps: number
  side_a_p50_in_bps: number
  side_a_p90_in_bps: number
  side_a_p95_in_bps: number
  side_a_p99_in_bps: number
  side_a_max_in_bps: number
  side_a_out_bps: number
  side_a_p50_out_bps: number
  side_a_p90_out_bps: number
  side_a_p95_out_bps: number
  side_a_p99_out_bps: number
  side_a_max_out_bps: number
  side_z_in_bps: number
  side_z_p50_in_bps: number
  side_z_p90_in_bps: number
  side_z_p95_in_bps: number
  side_z_p99_in_bps: number
  side_z_max_in_bps: number
  side_z_out_bps: number
  side_z_p50_out_bps: number
  side_z_p90_out_bps: number
  side_z_p95_out_bps: number
  side_z_p99_out_bps: number
  side_z_max_out_bps: number
  side_a_in_pps: number
  side_a_p50_in_pps: number
  side_a_p90_in_pps: number
  side_a_p95_in_pps: number
  side_a_p99_in_pps: number
  side_a_max_in_pps: number
  side_a_out_pps: number
  side_a_p50_out_pps: number
  side_a_p90_out_pps: number
  side_a_p95_out_pps: number
  side_a_p99_out_pps: number
  side_a_max_out_pps: number
  side_z_in_pps: number
  side_z_p50_in_pps: number
  side_z_p90_in_pps: number
  side_z_p95_in_pps: number
  side_z_p99_in_pps: number
  side_z_max_in_pps: number
  side_z_out_pps: number
  side_z_p50_out_pps: number
  side_z_p90_out_pps: number
  side_z_p95_out_pps: number
  side_z_p99_out_pps: number
  side_z_max_out_pps: number
  side_a_in_errors: number
  side_a_out_errors: number
  side_a_in_fcs_errors: number
  side_a_in_discards: number
  side_a_out_discards: number
  side_a_carrier_transitions: number
  side_z_in_errors: number
  side_z_out_errors: number
  side_z_in_fcs_errors: number
  side_z_in_discards: number
  side_z_out_discards: number
  side_z_carrier_transitions: number
  utilization_in_pct: number
  utilization_out_pct: number
}

export interface LinkMetricsBucket {
  ts: string
  status?: LinkMetricsStatus
  latency?: LinkMetricsLatency
  traffic?: LinkMetricsTraffic
}

export interface LinkMetricsResponse {
  link_pk: string
  link_code: string
  link_type: string
  contributor_code: string
  contributor_pk: string
  side_z_contributor_code: string
  side_a_metro: string
  side_z_metro: string
  side_a_device: string
  side_z_device: string
  side_a_iface_name: string
  side_z_iface_name: string
  committed_rtt_us: number
  committed_jitter_us: number
  bandwidth_bps: number
  current_drain_status: string
  link_topologies: string[]
  unicast_drained: boolean
  time_range: string
  bucket_seconds: number
  bucket_count: number
  buckets: LinkMetricsBucket[]
  status_changes?: EntityStatusChange[]
}

export type LinkMetricsInclude = 'all' | 'status' | 'latency' | 'traffic' | 'status_changes'

export interface FetchLinkMetricsParams {
  range?: string
  startTime?: number
  endTime?: number
  bucket?: string
  include?: LinkMetricsInclude[]
  hasIssues?: boolean
}

export async function fetchLinkMetrics(pk: string, params: FetchLinkMetricsParams = {}): Promise<LinkMetricsResponse> {
  const qs = new URLSearchParams()
  if (params.range) qs.set('range', params.range)
  if (params.startTime) qs.set('start_time', params.startTime.toString())
  if (params.endTime) qs.set('end_time', params.endTime.toString())
  if (params.bucket) qs.set('bucket', params.bucket)
  if (params.include) qs.set('include', params.include.join(','))
  const res = await apiFetch(`/api/link-metrics/${encodeURIComponent(pk)}${qs.toString() ? '?' + qs.toString() : ''}`)
  if (!res.ok) throw new Error('Failed to fetch link metrics')
  return res.json()
}

export interface BulkLinkMetricsResponse {
  time_range: string
  bucket_seconds: number
  bucket_count: number
  links: Record<string, LinkMetricsResponse>
}

export async function fetchBulkLinkMetrics(params: FetchLinkMetricsParams = {}): Promise<BulkLinkMetricsResponse> {
  const qs = new URLSearchParams()
  if (params.range) qs.set('range', params.range)
  if (params.startTime) qs.set('start_time', params.startTime.toString())
  if (params.endTime) qs.set('end_time', params.endTime.toString())
  if (params.bucket) qs.set('bucket', params.bucket)
  if (params.include) qs.set('include', params.include.join(','))
  if (params.hasIssues) qs.set('has_issues', 'true')
  const res = await apiFetch(`/api/link-metrics${qs.toString() ? '?' + qs.toString() : ''}`)
  if (!res.ok) throw new Error('Failed to fetch bulk link metrics')
  return res.json()
}

export interface DeviceMetricsStatus {
  health: string
  drain_status: string
  collecting: boolean
  isis_overload: boolean
  isis_unreachable: boolean
  no_probes: boolean
}

export interface DeviceMetricsTraffic {
  in_bps: number
  p50_in_bps: number
  p90_in_bps: number
  p95_in_bps: number
  p99_in_bps: number
  max_in_bps: number
  out_bps: number
  p50_out_bps: number
  p90_out_bps: number
  p95_out_bps: number
  p99_out_bps: number
  max_out_bps: number
  in_pps: number
  p50_in_pps: number
  p90_in_pps: number
  p95_in_pps: number
  p99_in_pps: number
  max_in_pps: number
  out_pps: number
  p50_out_pps: number
  p90_out_pps: number
  p95_out_pps: number
  p99_out_pps: number
  max_out_pps: number
  in_errors: number
  out_errors: number
  in_fcs_errors: number
  in_discards: number
  out_discards: number
  carrier_transitions: number
}

export interface DeviceInterfaceTraffic {
  intf: string
  link_pk?: string
  link_code?: string
  link_side?: string
  user_pk?: string
  cyoa_type?: string
  in_bps: number
  p50_in_bps: number
  p90_in_bps: number
  p95_in_bps: number
  p99_in_bps: number
  max_in_bps: number
  out_bps: number
  p50_out_bps: number
  p90_out_bps: number
  p95_out_bps: number
  p99_out_bps: number
  max_out_bps: number
  in_errors: number
  out_errors: number
  in_fcs_errors: number
  in_discards: number
  out_discards: number
  carrier_transitions: number
}

export interface DeviceMetricsBucket {
  ts: string
  status?: DeviceMetricsStatus
  latency?: LinkMetricsLatency
  traffic?: DeviceMetricsTraffic
  interfaces?: DeviceInterfaceTraffic[]
}

export interface DeviceMetricsResponse {
  device_pk: string
  device_code: string
  device_type: string
  contributor_code: string
  contributor_pk: string
  metro: string
  max_users: number
  time_range: string
  bucket_seconds: number
  bucket_count: number
  buckets: DeviceMetricsBucket[]
  status_changes?: EntityStatusChange[]
}

export type DeviceMetricsInclude = 'all' | 'status' | 'traffic' | 'status_changes'

export interface FetchDeviceMetricsParams {
  range?: string
  startTime?: number
  endTime?: number
  bucket?: string
  include?: DeviceMetricsInclude[]
  hasIssues?: boolean
}

export async function fetchDeviceMetrics(pk: string, params: FetchDeviceMetricsParams = {}): Promise<DeviceMetricsResponse> {
  const qs = new URLSearchParams()
  if (params.range) qs.set('range', params.range)
  if (params.startTime) qs.set('start_time', params.startTime.toString())
  if (params.endTime) qs.set('end_time', params.endTime.toString())
  if (params.bucket) qs.set('bucket', params.bucket)
  if (params.include) qs.set('include', params.include.join(','))
  const res = await apiFetch(`/api/device-metrics/${encodeURIComponent(pk)}${qs.toString() ? '?' + qs.toString() : ''}`)
  if (!res.ok) throw new Error('Failed to fetch device metrics')
  return res.json()
}

export interface BulkDeviceMetricsResponse {
  time_range: string
  bucket_seconds: number
  bucket_count: number
  devices: Record<string, DeviceMetricsResponse>
}

export async function fetchBulkDeviceMetrics(params: FetchDeviceMetricsParams = {}): Promise<BulkDeviceMetricsResponse> {
  const qs = new URLSearchParams()
  if (params.range) qs.set('range', params.range)
  if (params.startTime) qs.set('start_time', params.startTime.toString())
  if (params.endTime) qs.set('end_time', params.endTime.toString())
  if (params.bucket) qs.set('bucket', params.bucket)
  if (params.include) qs.set('include', params.include.join(','))
  if (params.hasIssues) qs.set('has_issues', 'true')
  const res = await apiFetch(`/api/device-metrics${qs.toString() ? '?' + qs.toString() : ''}`)
  if (!res.ok) throw new Error('Failed to fetch bulk device metrics')
  return res.json()
}

// Shred Devices (with pricing and seat info)
export interface ShredDevice {
  device_key: string
  device_code: string
  metro_exchange_key: string
  metro_code: string
  is_enabled: number
  base_price_dollars: number
  premium_dollars: number
  total_price_dollars: number
  granted_seats: number
  capacity: number
  available_seats: number
  /** Metro-level: 1 when every device in the metro serves the retransmit group only. */
  retransmit_only_enabled: number
}

export async function fetchShredDevices(params: {
  limit: number
  offset: number
  sortBy?: string
  sortDir?: string
  filters?: string[]
}): Promise<PaginatedResponse<ShredDevice>> {
  const qs = new URLSearchParams()
  qs.set('limit', params.limit.toString())
  qs.set('offset', params.offset.toString())
  if (params.sortBy) qs.set('sort_by', params.sortBy)
  if (params.sortDir) qs.set('sort_dir', params.sortDir)
  if (params.filters) {
    for (const f of params.filters) qs.append('filters', f)
  }
  const res = await fetchWithRetry(`/api/dz/shreds/devices?${qs.toString()}`)
  if (!res.ok) {
    throw new Error('Failed to fetch shred devices')
  }
  return res.json()
}

// Geolocation types
export interface GeolocProbe {
  pk: string
  owner: string
  exchange_pk: string
  public_ip: string
  location_offset_port: number
  metrics_publisher_pk: string
  reference_count: number
  code: string
  parent_devices: string
  target_update_count: number
}

export async function fetchGeolocProbes(
  limit = 100,
  offset = 0,
  sortBy?: string,
  sortDir?: 'asc' | 'desc',
  filters?: string[]
): Promise<PaginatedResponse<GeolocProbe>> {
  const params = new URLSearchParams()
  params.set('limit', String(limit))
  params.set('offset', String(offset))
  if (sortBy) params.set('sort_by', sortBy)
  if (sortDir) params.set('sort_dir', sortDir)
  if (filters) filters.forEach(f => params.append('filters', f))
  const res = await apiFetch(`/api/dz/geoloc/probes?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch geolocation probes')
  }
  return res.json()
}

export interface GeolocUser {
  pk: string
  owner: string
  code: string
  token_account: string
  payment_status: string
  status: string
  target_count: number
  billing_rate: number
  last_deduction_dz_epoch: number
}

export async function fetchGeolocUsers(
  limit = 100,
  offset = 0,
  sortBy?: string,
  sortDir?: 'asc' | 'desc',
  filters?: string[]
): Promise<PaginatedResponse<GeolocUser>> {
  const params = new URLSearchParams()
  params.set('limit', String(limit))
  params.set('offset', String(offset))
  if (sortBy) params.set('sort_by', sortBy)
  if (sortDir) params.set('sort_dir', sortDir)
  if (filters) filters.forEach(f => params.append('filters', f))
  const res = await apiFetch(`/api/dz/geoloc/users?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch geolocation users')
  }
  return res.json()
}

export interface GeolocExplorerDevice {
  sender_pubkey: string
  probe_code: string
  lat: number
  lng: number
  min_ref_measured_rtt_ns: number
}

export interface GeolocExplorerProbe {
  pk: string
  code: string
  lat: number
  lng: number
}

export interface GeolocExplorerTarget {
  sender_pubkey: string
  target_ip: string
  lat: number
  lng: number
  min_measured_rtt_ns: number
}

export interface GeolocExplorerResponse {
  devices: GeolocExplorerDevice[]
  probes: GeolocExplorerProbe[]
  targets: GeolocExplorerTarget[]
}

export async function fetchGeolocExplorer(hours?: number): Promise<GeolocExplorerResponse> {
  const params = new URLSearchParams()
  if (hours) params.set('hours', String(hours))
  const query = params.toString()
  const res = await apiFetch(`/api/dz/geoloc/explorer${query ? `?${query}` : ''}`)
  if (!res.ok) {
    throw new Error('Failed to fetch geolocation explorer data')
  }
  return res.json()
}

// Geo Concentration (DZDP)

export interface GeoConcentrationHeroStats {
  validators_measured: number
  stake_top_two_metros_pct: number
  anchor_points: number
  stake_max_asn_pct: number
}

export interface GeoConcentrationMetro {
  metro_code: string
  validators: number
  stake_sol: number
  stake_pct: number
}

export interface GeoConcentrationCountry {
  country_code: string
  country_name: string
  validators: number
  stake_sol: number
  stake_pct: number
}

export interface GeoConcentrationASN {
  asn: number
  asn_org: string
  validators: number
  stake_sol: number
  stake_pct: number
}

export interface GeoConcentrationResponse {
  hero_stats: GeoConcentrationHeroStats
  metros: GeoConcentrationMetro[]
  countries: GeoConcentrationCountry[]
  asns: GeoConcentrationASN[]
}

export async function fetchGeoConcentration(): Promise<GeoConcentrationResponse> {
  const res = await apiFetch('/api/dz/geoloc/concentration')
  if (!res.ok) {
    throw new Error('Failed to fetch geo concentration data')
  }
  return res.json()
}

// Geo Validators (DZDP)

export interface GeoValidatorItem {
  vote_pubkey: string
  node_pubkey: string
  name: string
  stake_sol: number
  stake_pct: number
  commission: number
  metro_code: string
  country_code: string
  asn: number
  asn_org: string
  datacenter: string
  is_dz: boolean
  tier: string
  dzdp_lat: number
  dzdp_lng: number
}

export interface GeoTierDistribution {
  tier: string
  validators: number
  stake_pct: number
}

export interface GeoMetroBreakdown {
  metro_code: string
  validators: number
  stake_sol: number
  stake_pct: number
}

export interface GeoValidatorsResponse {
  total_validators: number
  total_stake_sol: number
  validators: GeoValidatorItem[]
  tier_distribution: GeoTierDistribution[]
  metro_breakdown: GeoMetroBreakdown[]
}

export async function fetchGeoValidators(
  metro?: string,
  dzFilter?: 'on' | 'off'
): Promise<GeoValidatorsResponse> {
  const params = new URLSearchParams()
  if (metro) params.set('metro', metro)
  if (dzFilter) params.set('dz_filter', dzFilter)
  const query = params.toString()
  const res = await apiFetch(`/api/dz/geoloc/validators${query ? `?${query}` : ''}`)
  if (!res.ok) {
    throw new Error('Failed to fetch geo validators data')
  }
  return res.json()
}

// Access Passes

export interface AccessPass {
  pk: string
  owner_pubkey: string
  type_tag: string
  status: string
  client_ip: string
  connection_count: number
  associated_pubkey: string
  first_pub_code: string
  first_sub_code: string
}

export interface MulticastGroupRef {
  pk: string
  code: string
  multicast_ip: string
  status: string
}

export interface AccessPassShredsSeat {
  pk: string
  device_key: string
  device_code: string
  metro_pk: string
  metro_code: string
  tenure_epochs: number
  funded_epoch: number
  active_epoch: number
  escrow_count: number
  spendable_usdc_balance: number
  all_escrows_usdc_balance: number
  price_per_epoch_dollars: number
  funding_authority_key: string
}

export interface AccessPassDetail extends AccessPass {
  user_payer: string
  others_type_name: string
  others_key: string
  last_access_epoch: number
  flags: number
  mgroup_pub_allowlist: MulticastGroupRef[]
  mgroup_sub_allowlist: MulticastGroupRef[]
  validator_vote_pubkey?: string
  validator_node_pubkey?: string
  shreds_seat?: AccessPassShredsSeat
}

export async function fetchAccessPasses(
  limit = 100,
  offset = 0,
  sortBy?: string,
  sortDir?: 'asc' | 'desc',
  filters?: string[]
): Promise<PaginatedResponse<AccessPass>> {
  const params = new URLSearchParams({ limit: String(limit), offset: String(offset) })
  if (sortBy) params.set('sort_by', sortBy)
  if (sortDir) params.set('sort_dir', sortDir)
  if (filters) {
    for (const f of filters) params.append('filters', f)
  }
  const res = await apiFetch(`/api/dz/access-passes?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch access passes')
  }
  return res.json()
}

export async function fetchAccessPass(pk: string): Promise<AccessPassDetail> {
  const res = await apiFetch(`/api/dz/access-passes/${encodeURIComponent(pk)}`)
  if (!res.ok) {
    throw new Error('Failed to fetch access pass')
  }
  return res.json()
}

export interface AccessPassConnection {
  pk: string
  owner_pubkey: string
  status: string
  kind: string
  dz_ip: string
  client_ip: string
  device_code: string
  metro_code: string
  tenant_code: string
}

export async function fetchAccessPassConnections(pk: string): Promise<AccessPassConnection[]> {
  const res = await apiFetch(`/api/dz/access-passes/${encodeURIComponent(pk)}/connections`)
  if (!res.ok) {
    throw new Error('Failed to fetch access pass connections')
  }
  return res.json()
}

// Shreds — Edge Rewards (per-epoch validator earnings)

export interface ShredsRewardsRow {
  node_id: string
  vote_pubkey: string
  validator_name: string
  activated_stake: number
  dz_user_ip: string
  // 2Z-only headline totals (cross-token sums are not meaningful).
  total_earned_2z: number
  immediately_claimable_2z: number
  // Per recent-epoch reward, in whole units of the token earned that epoch.
  epoch_earnings: Record<string, number>
  // Per recent-epoch reward-token symbol, parallel to epoch_earnings.
  epoch_tokens: Record<string, string>
}

export interface ShredsRewardsResponse {
  current_solana_epoch: number
  latest_finalized_epoch: number
  epoch_columns: number[]
  validators: ShredsRewardsRow[]
  // Total distinct validators matching the filter, before limit/offset.
  total: number
}

export interface ShredsRewardsParams {
  search?: string
  sort?: 'validator_name' | 'activated_stake' | 'total_earned_2z' | 'immediately_claimable_2z'
  order?: 'asc' | 'desc'
  limit?: number
  offset?: number
}

export async function fetchShredsRewards(
  params: ShredsRewardsParams = {},
): Promise<ShredsRewardsResponse> {
  const q = new URLSearchParams()
  if (params.search) q.set('search', params.search)
  if (params.sort) q.set('sort', params.sort)
  if (params.order) q.set('order', params.order)
  if (params.limit != null) q.set('limit', String(params.limit))
  if (params.offset != null) q.set('offset', String(params.offset))
  const qs = q.toString()
  const res = await apiFetch(`/api/dz/shreds/rewards${qs ? `?${qs}` : ''}`)
  if (!res.ok) throw new Error('Failed to fetch shreds rewards')
  return res.json()
}

export type ShredsRewardsClaimState =
  | 'claimable'
  | 'distributed'
  | 'pending'
  | 'unknown'

export interface ShredsRewardsEpochDetail {
  solana_epoch: number
  subscription_epoch: number
  leader_slots: number
  client_id: number
  // Reward in whole units of token_symbol (the token the validator chose for
  // the epoch: 2Z, USDC, or wSOL).
  earned: number
  token_symbol: string
  // Retained for back-compat; `state` carries the full lifecycle.
  is_claimable?: boolean | null
  state: ShredsRewardsClaimState
}

export interface ShredsRewardsDetail {
  node_id: string
  vote_pubkey: string
  validator_name: string
  activated_stake: number
  dz_user_ip: string
  epochs: ShredsRewardsEpochDetail[]
}

export async function fetchShredsRewardsDetail(nodeId: string): Promise<ShredsRewardsDetail> {
  const res = await apiFetch(`/api/dz/shreds/rewards/${encodeURIComponent(nodeId)}`)
  if (!res.ok) throw new Error('Failed to fetch shreds rewards detail')
  return res.json()
}

export interface HyperliquidCompetitor {
  feed: string
  label: string
  dz_win_pct: number
  lead_p50_ms: number
  lead_p95_ms: number
  races: number
}

export interface HyperliquidNode {
  measurement_node_id: string
  location_code: string
  dz_win_share_pct: number
  total_races: number
  competitors: HyperliquidCompetitor[]
}

export interface HyperliquidRace {
  event_ts: string
  symbol: string
  location_code: string
  winner_feed: string
  winner_label: string
  is_dz: boolean
  runner_up_feed: string
  runner_up_label: string
  lead_ms: number
}

export interface HyperliquidScoreboardResponse {
  window: string
  symbol?: string
  generated_at: string
  feed_type: string
  dz_win_share_pct: number
  total_races: number
  competitors: HyperliquidCompetitor[]
  nodes: HyperliquidNode[]
  recent_races: HyperliquidRace[]
  prices?: Record<string, number>
  composite_latency?: HyperliquidCompositeLatency
}

export interface HyperliquidCompositeLatency {
  window: string
  p50_ms: number
  p90_ms: number
  p99_ms: number
  generated_at: string
}

export async function fetchHyperliquidScoreboard(
  window: string = '24h',
  symbol?: string,
): Promise<HyperliquidScoreboardResponse> {
  const params = new URLSearchParams()
  params.set('window', window)
  if (symbol && symbol !== 'all') params.set('symbol', symbol)
  const res = await apiFetch(`/api/dz/hyperliquid/scoreboard?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch hyperliquid scoreboard')
  }
  return res.json()
}

// Serviceability permission audit trail (internal only).
export interface PermissionAuditEvent {
  eventTs: string
  txSignature: string
  slot: number
  instructionIndex: number
  signer: string
  permissionPk: string
  targetPubkey: string
  eventType: string
  permissionsAdded: string
  permissionsRemoved: string
  success: boolean
}

export interface PermissionAuditResponse {
  events: PermissionAuditEvent[]
}

export async function fetchPermissionAudit(limit = 200): Promise<PermissionAuditResponse> {
  const res = await apiFetch(`/api/dz/permission-audit?limit=${limit}`)
  if (!res.ok) {
    throw new Error('Failed to fetch permission audit')
  }
  return res.json()
}
