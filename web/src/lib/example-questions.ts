// Example questions for chat/landing, categorized by feature dependency

const BASE_QUESTIONS = [
  'How is the network doing?',
  'Compare DZ to the public internet',
  'Which links have the highest utilization?',
  'Are there any links with packet loss?',
  'Are any links degraded right now?',
  "What's changed on the network this week?",
  'Which contributors have the most capacity?',
]

const SOLANA_QUESTIONS = [
  'How many Solana validators are on DZ?',
  'Which metros have the most validators?',
  'Which validators connected recently?',
  'Compare validator performance on vs off DZ',
]

const NEO4J_QUESTIONS = [
  'If the Hong Kong device goes down, what metros lose connectivity?',
  'What metros can I reach from Singapore?',
  'Show the paths between NYC and LON',
  'Show me the fastest paths between metros',
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
