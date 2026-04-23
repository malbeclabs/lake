import { createContext, useContext, useState, useCallback, type ReactNode } from 'react'
import { getEnv, setEnv as setEnvStorage, type AppConfig } from '@/lib/api'

interface EnvContextType {
  env: string
  setEnv: (env: string) => void
  availableEnvs: string[]
  features: Record<string, boolean>
}

const EnvContext = createContext<EnvContextType>({
  env: 'mainnet-beta',
  setEnv: () => {},
  availableEnvs: ['mainnet-beta'],
  features: {},
})

// eslint-disable-next-line react-refresh/only-export-components
export function useEnv() {
  return useContext(EnvContext)
}

export function EnvProvider({ config, children }: { config: AppConfig; children: ReactNode }) {
  // Consume ?env= synchronously on first render so it survives downstream
  // redirects (e.g. `/` → `/status`) that strip the query string.
  const [env, setEnvState] = useState(() => {
    const params = new URLSearchParams(window.location.search)
    const envParam = params.get('env')
    if (envParam) {
      setEnvStorage(envParam)
      params.delete('env')
      const newUrl = params.toString()
        ? `${window.location.pathname}?${params.toString()}`
        : window.location.pathname
      window.history.replaceState({}, '', newUrl)
      return envParam
    }
    return getEnv()
  })

  const setEnv = useCallback((newEnv: string) => {
    setEnvStorage(newEnv)
    setEnvState(newEnv)
    // Reload the page to re-fetch all data with new env
    window.location.reload()
  }, [])

  return (
    <EnvContext.Provider
      value={{
        env,
        setEnv,
        availableEnvs: config.availableEnvs || ['mainnet-beta'],
        features: config.features || {},
      }}
    >
      {children}
    </EnvContext.Provider>
  )
}
