// Example questions for chat/landing, categorized by feature dependency

const BASE_QUESTIONS = [
  'How is the network doing?',
  'Compare DZ to the public internet',
  'Which links have the highest utilization?',
  'Show me link latency by metro pair',
  'Are there any links with packet loss?',
  "What's the average RTT for DZ links?",
]

const SOLANA_QUESTIONS = [
  'How many Solana validators are on DZ?',
  'What is the total stake connected to DZ?',
  'Which metros have the most validators?',
  'Which validators connected recently?',
  'Which validators have the highest stake?',
  'Compare validator performance on vs off DZ',
]

const NEO4J_QUESTIONS = [
  'If the Hong Kong device goes down, what metros lose connectivity?',
  'What metros can I reach from Singapore?',
  'Show the paths between NYC and LON',
  'Show me all devices and links connected to the Singapore device',
]

// Fisher-Yates shuffle, take first n
function selectRandom<T>(arr: T[], n: number): T[] {
  const shuffled = [...arr]
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1))
    ;[shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]]
  }
  return shuffled.slice(0, n)
}

export function getExampleQuestions(
  features: Record<string, boolean>,
  count: number,
): string[] {
  const pool = [
    ...BASE_QUESTIONS,
    ...(features.solana ? SOLANA_QUESTIONS : []),
    ...(features.neo4j ? NEO4J_QUESTIONS : []),
  ]
  return selectRandom(pool, count)
}
