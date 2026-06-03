import { useMemo } from 'react'
import type { ReactNode } from 'react'
import { ConnectionProvider, WalletProvider } from '@solana/wallet-adapter-react'
import { WalletModalProvider } from '@solana/wallet-adapter-react-ui'
import {
  PhantomWalletAdapter,
  SolflareWalletAdapter,
  CoinbaseWalletAdapter,
  LedgerWalletAdapter,
  TrustWalletAdapter,
  NightlyWalletAdapter,
  BitgetWalletAdapter,
} from '@solana/wallet-adapter-wallets'

// Import wallet adapter styles
import '@solana/wallet-adapter-react-ui/styles.css'

interface WalletProviderWrapperProps {
  children: ReactNode
}

const SOLANA_RPC_ENDPOINT =
  import.meta.env.VITE_SOLANA_RPC_URL ?? 'https://api.mainnet-beta.solana.com'

export function WalletProviderWrapper({ children }: WalletProviderWrapperProps) {
  // Initialize wallet adapters
  const wallets = useMemo(
    () => [
      new PhantomWalletAdapter(),
      new SolflareWalletAdapter(),
      new CoinbaseWalletAdapter(),
      new LedgerWalletAdapter(),
      new TrustWalletAdapter(),
      new NightlyWalletAdapter(),
      new BitgetWalletAdapter(),
    ],
    []
  )

  return (
    <ConnectionProvider endpoint={SOLANA_RPC_ENDPOINT}>
      <WalletProvider wallets={wallets} autoConnect={false}>
        <WalletModalProvider>
          {children}
        </WalletModalProvider>
      </WalletProvider>
    </ConnectionProvider>
  )
}
