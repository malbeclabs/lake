import {
  PublicKey,
  SystemProgram,
  TransactionInstruction,
  ComputeBudgetProgram,
} from '@solana/web3.js'
import {
  SHRED_SUBSCRIPTION_PROGRAM_ID,
  USDC_MINT_MAINNET,
  SPL_TOKEN_PROGRAM_ID,
  IX_INITIALIZE_CLIENT_SEAT,
  IX_INITIALIZE_PAYMENT_ESCROW,
  IX_FUND_PAYMENT_ESCROW_USDC,
  IX_REQUEST_INSTANT_SEAT_ALLOCATION,
  IX_REQUEST_INSTANT_SEAT_WITHDRAWAL,
  IX_CLOSE_PAYMENT_ESCROW,
  findProgramConfigAddress,
  findExecutionControllerAddress,
  findDeviceHistoryAddress,
  findClientSeatAddress,
  findMetroHistoryAddress,
  findTokenPdaAddress,
  findPaymentEscrowAddress,
  findInstantAllocationRequestAddress,
  findWithdrawSeatRequestAddress,
} from './shred-program'

// ---------------------------------------------------------------------------
// ATA derivation (avoids @solana/spl-token which needs Buffer polyfill)
// ---------------------------------------------------------------------------

const ASSOCIATED_TOKEN_PROGRAM_ID = new PublicKey('ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL')

function getAssociatedTokenAddressSync(mint: PublicKey, owner: PublicKey): PublicKey {
  const [ata] = PublicKey.findProgramAddressSync(
    [owner.toBytes(), SPL_TOKEN_PROGRAM_ID.toBytes(), mint.toBytes()],
    ASSOCIATED_TOKEN_PROGRAM_ID,
  )
  return ata
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function u32LeBytes(n: number): Uint8Array {
  const buf = new Uint8Array(4)
  const view = new DataView(buf.buffer)
  view.setUint32(0, n, true)
  return buf
}

function u64LeBytes(n: bigint): Uint8Array {
  const buf = new Uint8Array(8)
  const view = new DataView(buf.buffer)
  view.setBigUint64(0, n, true)
  return buf
}

function concat(...bufs: Uint8Array[]): Buffer {
  const totalLen = bufs.reduce((sum, b) => sum + b.length, 0)
  const result = new Uint8Array(totalLen)
  let offset = 0
  for (const b of bufs) {
    result.set(b, offset)
    offset += b.length
  }
  // Cast to Buffer for @solana/web3.js v1 TransactionInstruction.data typing.
  // At runtime Uint8Array works fine — this is a type-only concern.
  return result as Buffer
}

function toBuffer(data: Uint8Array): Buffer {
  return data as Buffer
}

// ---------------------------------------------------------------------------
// Derived account set — shared across builders
// ---------------------------------------------------------------------------

export interface ShredAccounts {
  programConfig: PublicKey
  executionController: PublicKey
  deviceHistory: PublicKey
  metroHistory: PublicKey
  clientSeat: PublicKey
  paymentEscrow: PublicKey
  deviceHistoryUsdcToken: PublicKey
  sourceUsdcToken: PublicKey
  instantAllocationRequest: PublicKey
  withdrawSeatRequest: PublicKey
}

export function deriveShredAccounts(params: {
  device: PublicKey
  metroExchange: PublicKey
  clientIpBits: number
  wallet: PublicKey
}): ShredAccounts {
  const [programConfig] = findProgramConfigAddress()
  const [executionController] = findExecutionControllerAddress()
  const [deviceHistory] = findDeviceHistoryAddress(params.device)
  const [metroHistory] = findMetroHistoryAddress(params.metroExchange)
  const [clientSeat] = findClientSeatAddress(params.device, params.clientIpBits)
  const [paymentEscrow] = findPaymentEscrowAddress(clientSeat, params.wallet)
  const [deviceHistoryUsdcToken] = findTokenPdaAddress(deviceHistory, USDC_MINT_MAINNET)
  const sourceUsdcToken = getAssociatedTokenAddressSync(USDC_MINT_MAINNET, params.wallet)
  const [instantAllocationRequest] = findInstantAllocationRequestAddress(
    params.device,
    params.clientIpBits,
  )
  const [withdrawSeatRequest] = findWithdrawSeatRequestAddress(clientSeat)

  return {
    programConfig,
    executionController,
    deviceHistory,
    metroHistory,
    clientSeat,
    paymentEscrow,
    deviceHistoryUsdcToken,
    sourceUsdcToken,
    instantAllocationRequest,
    withdrawSeatRequest,
  }
}

// ---------------------------------------------------------------------------
// Subscribe (pay) — up to 4 instructions
// ---------------------------------------------------------------------------

export interface SubscribeParams {
  accounts: ShredAccounts
  wallet: PublicKey
  clientIpBits: number
  amountMicro: bigint
  seatExists: boolean
  escrowExists: boolean
  seatActive: boolean // tenure_epochs > 0
}

export function buildSubscribeInstructions(params: SubscribeParams): TransactionInstruction[] {
  const { accounts: a, wallet } = params
  const ixs: TransactionInstruction[] = []
  let computeUnits = 0

  // 1. InitializeClientSeat (if seat doesn't exist)
  if (!params.seatExists) {
    ixs.push(
      new TransactionInstruction({
        programId: SHRED_SUBSCRIPTION_PROGRAM_ID,
        keys: [
          { pubkey: a.programConfig, isSigner: false, isWritable: false },
          { pubkey: a.executionController, isSigner: false, isWritable: true },
          { pubkey: a.deviceHistory, isSigner: false, isWritable: false },
          { pubkey: wallet, isSigner: true, isWritable: true },
          { pubkey: a.clientSeat, isSigner: false, isWritable: true },
          { pubkey: SystemProgram.programId, isSigner: false, isWritable: false },
        ],
        data: concat(IX_INITIALIZE_CLIENT_SEAT, u32LeBytes(params.clientIpBits)),
      }),
    )
    computeUnits += 50_000
  }

  // 2. InitializePaymentEscrow (if escrow doesn't exist)
  if (!params.escrowExists) {
    ixs.push(
      new TransactionInstruction({
        programId: SHRED_SUBSCRIPTION_PROGRAM_ID,
        keys: [
          { pubkey: a.programConfig, isSigner: false, isWritable: false },
          { pubkey: a.clientSeat, isSigner: false, isWritable: true },
          { pubkey: wallet, isSigner: true, isWritable: true },
          { pubkey: a.paymentEscrow, isSigner: false, isWritable: true },
          { pubkey: SystemProgram.programId, isSigner: false, isWritable: false },
        ],
        data: toBuffer(IX_INITIALIZE_PAYMENT_ESCROW),
      }),
    )
    computeUnits += 50_000
  }

  // 3. FundPaymentEscrowUsdc (always)
  ixs.push(
    new TransactionInstruction({
      programId: SHRED_SUBSCRIPTION_PROGRAM_ID,
      keys: [
        { pubkey: a.programConfig, isSigner: false, isWritable: false },
        { pubkey: a.executionController, isSigner: false, isWritable: true },
        { pubkey: a.metroHistory, isSigner: false, isWritable: false },
        { pubkey: a.deviceHistory, isSigner: false, isWritable: false },
        { pubkey: a.clientSeat, isSigner: false, isWritable: true },
        { pubkey: a.paymentEscrow, isSigner: false, isWritable: true },
        { pubkey: a.deviceHistoryUsdcToken, isSigner: false, isWritable: true },
        { pubkey: a.sourceUsdcToken, isSigner: false, isWritable: true },
        { pubkey: wallet, isSigner: true, isWritable: false },
        { pubkey: SPL_TOKEN_PROGRAM_ID, isSigner: false, isWritable: false },
      ],
      data: concat(IX_FUND_PAYMENT_ESCROW_USDC, u64LeBytes(params.amountMicro)),
    }),
  )
  computeUnits += 50_000

  // 4. RequestInstantSeatAllocation (if seat not active)
  if (!params.seatActive) {
    ixs.push(
      new TransactionInstruction({
        programId: SHRED_SUBSCRIPTION_PROGRAM_ID,
        keys: [
          { pubkey: a.programConfig, isSigner: false, isWritable: false },
          { pubkey: a.executionController, isSigner: false, isWritable: true },
          { pubkey: a.metroHistory, isSigner: false, isWritable: false },
          { pubkey: a.deviceHistory, isSigner: false, isWritable: true },
          { pubkey: a.clientSeat, isSigner: false, isWritable: true },
          { pubkey: a.paymentEscrow, isSigner: false, isWritable: true },
          { pubkey: wallet, isSigner: true, isWritable: true },
          { pubkey: a.instantAllocationRequest, isSigner: false, isWritable: true },
          { pubkey: SystemProgram.programId, isSigner: false, isWritable: false },
        ],
        data: toBuffer(IX_REQUEST_INSTANT_SEAT_ALLOCATION),
      }),
    )
    computeUnits += 50_000
  }

  // Prepend compute budget
  ixs.unshift(ComputeBudgetProgram.setComputeUnitLimit({ units: computeUnits }))

  return ixs
}

// ---------------------------------------------------------------------------
// Fund (top up existing seat) — 1 instruction
// ---------------------------------------------------------------------------

export interface FundParams {
  accounts: ShredAccounts
  wallet: PublicKey
  amountMicro: bigint
}

export function buildFundInstructions(params: FundParams): TransactionInstruction[] {
  const { accounts: a, wallet } = params

  const fund = new TransactionInstruction({
    programId: SHRED_SUBSCRIPTION_PROGRAM_ID,
    keys: [
      { pubkey: a.programConfig, isSigner: false, isWritable: false },
      { pubkey: a.executionController, isSigner: false, isWritable: true },
      { pubkey: a.metroHistory, isSigner: false, isWritable: false },
      { pubkey: a.deviceHistory, isSigner: false, isWritable: false },
      { pubkey: a.clientSeat, isSigner: false, isWritable: true },
      { pubkey: a.paymentEscrow, isSigner: false, isWritable: true },
      { pubkey: a.deviceHistoryUsdcToken, isSigner: false, isWritable: true },
      { pubkey: a.sourceUsdcToken, isSigner: false, isWritable: true },
      { pubkey: wallet, isSigner: true, isWritable: false },
      { pubkey: SPL_TOKEN_PROGRAM_ID, isSigner: false, isWritable: false },
    ],
    data: concat(IX_FUND_PAYMENT_ESCROW_USDC, u64LeBytes(params.amountMicro)),
  })

  return [
    ComputeBudgetProgram.setComputeUnitLimit({ units: 50_000 }),
    fund,
  ]
}

// ---------------------------------------------------------------------------
// Unsubscribe (withdraw) — up to 2 instructions
// ---------------------------------------------------------------------------

export interface UnsubscribeParams {
  accounts: ShredAccounts
  wallet: PublicKey
  escrowExists: boolean
}

export function buildUnsubscribeInstructions(params: UnsubscribeParams): TransactionInstruction[] {
  const { accounts: a, wallet } = params
  const ixs: TransactionInstruction[] = []
  let computeUnits = 30_000

  // 1. RequestInstantSeatWithdrawal (always)
  ixs.push(
    new TransactionInstruction({
      programId: SHRED_SUBSCRIPTION_PROGRAM_ID,
      keys: [
        { pubkey: a.programConfig, isSigner: false, isWritable: false },
        { pubkey: a.executionController, isSigner: false, isWritable: true },
        { pubkey: a.clientSeat, isSigner: false, isWritable: true },
        { pubkey: a.deviceHistory, isSigner: false, isWritable: true },
        { pubkey: wallet, isSigner: true, isWritable: true },
        { pubkey: a.withdrawSeatRequest, isSigner: false, isWritable: true },
        { pubkey: SystemProgram.programId, isSigner: false, isWritable: false },
      ],
      data: toBuffer(IX_REQUEST_INSTANT_SEAT_WITHDRAWAL),
    }),
  )
  computeUnits += 50_000

  // 2. ClosePaymentEscrow (if escrow exists)
  if (params.escrowExists) {
    // Refund to user's USDC ATA
    ixs.push(
      new TransactionInstruction({
        programId: SHRED_SUBSCRIPTION_PROGRAM_ID,
        keys: [
          { pubkey: a.programConfig, isSigner: false, isWritable: false },
          { pubkey: a.executionController, isSigner: false, isWritable: false },
          { pubkey: a.paymentEscrow, isSigner: false, isWritable: true },
          { pubkey: wallet, isSigner: true, isWritable: true },
          { pubkey: a.clientSeat, isSigner: false, isWritable: true },
          { pubkey: a.deviceHistory, isSigner: false, isWritable: false },
          { pubkey: a.deviceHistoryUsdcToken, isSigner: false, isWritable: true },
          { pubkey: a.sourceUsdcToken, isSigner: false, isWritable: true }, // refund to same ATA
          { pubkey: SPL_TOKEN_PROGRAM_ID, isSigner: false, isWritable: false },
        ],
        data: toBuffer(IX_CLOSE_PAYMENT_ESCROW),
      }),
    )
  }

  // Prepend compute budget
  ixs.unshift(ComputeBudgetProgram.setComputeUnitLimit({ units: computeUnits }))

  return ixs
}
