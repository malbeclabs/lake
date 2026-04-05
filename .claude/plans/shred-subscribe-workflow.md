# Shred Subscribe/Fund/Unsubscribe Workflow — Hidden but Implemented

This workflow is fully implemented but hidden from the UI pending further iteration.

## What exists (code is in the repo, just not wired up)

### Subscribe Page
- **File:** `web/src/components/shreds-subscribe-page.tsx`
- **Route:** Was at `/dz/shreds/subscribe` (removed from App.tsx, commented out)
- Step-by-step flow: select device → enter client IP + USDC amount → connect wallet → sign transaction
- Auto-selects device from `?device=` query param (used by devices page row click)
- Shows pricing, prepaid epoch calculation, epoch remaining warning
- Transaction progress UI (signing → sending → confirming → confirmed with Solscan link)

### Fund Modal (top up existing subscription)
- **File:** `web/src/components/shred-fund-modal.tsx`
- Was triggered by "Fund" button on subscribers page action column
- Shows seat info, amount input, epoch calculation, wallet connection
- Builds `FundPaymentEscrowUsdc` instruction only

### Unsubscribe/Withdraw Modal
- **File:** `web/src/components/shred-withdraw-modal.tsx`
- Was triggered by "Unsubscribe" or "Withdraw" button on subscribers page
- Shows seat info, balance to refund, confirmation
- Builds `RequestInstantSeatWithdrawal` + `ClosePaymentEscrow` instructions

### Solana Transaction Infrastructure
- **`web/src/lib/shred-program.ts`** — Program ID, PDA derivation, discriminators, account parsing
- **`web/src/lib/shred-transactions.ts`** — Transaction builders (subscribe, fund, unsubscribe)
- **`web/src/hooks/use-shred-accounts.ts`** — On-chain seat/escrow state + USDC balance hooks
- **`web/src/hooks/use-shred-transaction.ts`** — Sign/send/confirm lifecycle hook

## What was hidden

1. **Subscribe route** — commented out in `App.tsx` (line ~712)
2. **Subscribe nav link** — was removed from sidebar earlier
3. **Subscribe column on devices page** — row click + "Subscribe >" column removed
4. **Actions column on subscribers page** — Fund/Unsubscribe buttons removed
5. **Modal imports** in `shreds-page.tsx` — `ShredFundModal` and `ShredWithdrawModal` imports removed

## To re-enable

1. In `App.tsx`: uncomment the subscribe route, re-import `ShredsSubscribePage`
2. In `shreds-page.tsx` (ShredsSeatsPage):
   - Import `ShredFundModal`, `ShredWithdrawModal`, `useWallet`, `DollarSign`, `LogOut`
   - Add wallet state: `const { publicKey: walletKey } = useWallet()` + `walletAddress`
   - Add modal state: `fundSeat` and `withdrawSeat` useState
   - Add Actions column header (gated on `walletAddress`)
   - Add Actions td per row (gated on `walletAddress === seat.funding_authority_key`)
   - Add modal renders at bottom of component
3. In `shreds-page.tsx` (ShredsDevicesPage):
   - Add row click with `handleRowClick` to `/dz/shreds/subscribe?device=...`
   - Add empty th + "Subscribe >" td column
4. Optionally re-add "Subscribe" to sidebar nav
