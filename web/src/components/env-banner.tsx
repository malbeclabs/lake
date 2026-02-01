import { useEnv } from '@/contexts/EnvContext'

const envColors: Record<string, string> = {
  devnet: 'bg-amber-500/90 text-black',
  testnet: 'bg-purple-500/90 text-white',
}

export function EnvBanner() {
  const { env, setEnv } = useEnv()

  if (env === 'mainnet-beta') return null

  const colorClass = envColors[env] || 'bg-blue-500/90 text-white'

  return (
    <div className={`${colorClass} px-3 py-1 text-xs font-medium flex items-center justify-center gap-2`}>
      <span>Viewing DoubleZero {env}</span>
      <button
        onClick={() => setEnv('mainnet-beta')}
        className="underline hover:no-underline"
      >
        Switch to mainnet-beta
      </button>
    </div>
  )
}
