import { PublicKey } from '@solana/web3.js'

// ---------------------------------------------------------------------------
// Program & token constants
// ---------------------------------------------------------------------------

export const SHRED_SUBSCRIPTION_PROGRAM_ID = new PublicKey(
  'dzshrr3yL57SB13sJPYHYo3TV8Bo1i1FxkyrZr3bKNE',
)

export const USDC_MINT_MAINNET = new PublicKey(
  'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
)

export const SPL_TOKEN_PROGRAM_ID = new PublicKey(
  'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
)

// ---------------------------------------------------------------------------
// Instruction discriminators (first 8 bytes of SHA-256)
// ---------------------------------------------------------------------------

export const IX_INITIALIZE_CLIENT_SEAT = new Uint8Array([
  0xc9, 0xef, 0xe5, 0x06, 0x68, 0x21, 0xee, 0xeb,
])
export const IX_INITIALIZE_PAYMENT_ESCROW = new Uint8Array([
  0x96, 0x31, 0x14, 0x98, 0x17, 0x10, 0x5c, 0x34,
])
export const IX_CLOSE_PAYMENT_ESCROW = new Uint8Array([
  0x48, 0x11, 0x67, 0x7c, 0x9c, 0x88, 0x4a, 0x9b,
])
export const IX_FUND_PAYMENT_ESCROW_USDC = new Uint8Array([
  0x6f, 0x06, 0x60, 0x02, 0x79, 0x5c, 0x44, 0x93,
])
export const IX_REQUEST_INSTANT_SEAT_ALLOCATION = new Uint8Array([
  0x7b, 0x31, 0x56, 0xfd, 0x60, 0x4a, 0x83, 0x05,
])
export const IX_REQUEST_INSTANT_SEAT_WITHDRAWAL = new Uint8Array([
  0x3f, 0x27, 0x99, 0x82, 0xa2, 0xf4, 0xe2, 0xbb,
])

// ---------------------------------------------------------------------------
// PDA derivation
// ---------------------------------------------------------------------------

const enc = new TextEncoder()

function u32ToLeBytes(n: number): Uint8Array {
  const buf = new Uint8Array(4)
  buf[0] = n & 0xff
  buf[1] = (n >>> 8) & 0xff
  buf[2] = (n >>> 16) & 0xff
  buf[3] = (n >>> 24) & 0xff
  return buf
}

export function findProgramConfigAddress(): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [enc.encode('program_config')],
    SHRED_SUBSCRIPTION_PROGRAM_ID,
  )
}

export function findExecutionControllerAddress(): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [enc.encode('execution_controller')],
    SHRED_SUBSCRIPTION_PROGRAM_ID,
  )
}

export function findDeviceHistoryAddress(device: PublicKey): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [enc.encode('device_history'), device.toBytes()],
    SHRED_SUBSCRIPTION_PROGRAM_ID,
  )
}

export function findClientSeatAddress(
  device: PublicKey,
  clientIpBits: number,
): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [enc.encode('client_seat'), device.toBytes(), u32ToLeBytes(clientIpBits)],
    SHRED_SUBSCRIPTION_PROGRAM_ID,
  )
}

export function findMetroHistoryAddress(exchange: PublicKey): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [enc.encode('metro_history'), exchange.toBytes()],
    SHRED_SUBSCRIPTION_PROGRAM_ID,
  )
}

export function findTokenPdaAddress(
  owner: PublicKey,
  mint: PublicKey,
): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [enc.encode('token'), owner.toBytes(), mint.toBytes()],
    SHRED_SUBSCRIPTION_PROGRAM_ID,
  )
}

export function findPaymentEscrowAddress(
  clientSeat: PublicKey,
  withdrawAuthority: PublicKey,
): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [enc.encode('payment_escrow'), clientSeat.toBytes(), withdrawAuthority.toBytes()],
    SHRED_SUBSCRIPTION_PROGRAM_ID,
  )
}

export function findInstantAllocationRequestAddress(
  device: PublicKey,
  clientIpBits: number,
): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [
      enc.encode('instant_seat_allocation_request'),
      device.toBytes(),
      u32ToLeBytes(clientIpBits),
    ],
    SHRED_SUBSCRIPTION_PROGRAM_ID,
  )
}

export function findWithdrawSeatRequestAddress(
  clientSeat: PublicKey,
): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [enc.encode('withdraw_seat_request'), clientSeat.toBytes()],
    SHRED_SUBSCRIPTION_PROGRAM_ID,
  )
}

// ---------------------------------------------------------------------------
// IPv4 helpers
// ---------------------------------------------------------------------------

/** Convert "1.2.3.4" to a u32 in network byte order (big-endian packed into u32). */
export function ipv4ToU32(ip: string): number {
  const parts = ip.split('.')
  if (parts.length !== 4) throw new Error(`Invalid IPv4 address: ${ip}`)
  return parts.reduce((acc, octet) => {
    const n = parseInt(octet, 10)
    if (isNaN(n) || n < 0 || n > 255) throw new Error(`Invalid IPv4 octet: ${octet}`)
    return (acc << 8) | n
  }, 0) >>> 0 // ensure unsigned
}

/** Validate an IPv4 address string. */
export function isValidIpv4(ip: string): boolean {
  const parts = ip.split('.')
  if (parts.length !== 4) return false
  return parts.every(p => {
    const n = parseInt(p, 10)
    return !isNaN(n) && n >= 0 && n <= 255 && String(n) === p
  })
}

// ---------------------------------------------------------------------------
// Account parsing
// ---------------------------------------------------------------------------

const DISCRIMINATOR_LEN = 8

export interface ParsedClientSeat {
  deviceKey: PublicKey
  clientIpBits: number
  tenureEpochs: number
  fundedEpoch: bigint
  activeEpoch: bigint
  escrowCount: number
  fundingAuthorityKey: PublicKey
}

/** Parse a ClientSeat on-chain account. Returns null if data is too short. */
export function parseClientSeat(data: Uint8Array): ParsedClientSeat | null {
  if (data.length < DISCRIMINATOR_LEN + 140) return null
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)

  return {
    deviceKey: new PublicKey(data.slice(8, 40)),
    clientIpBits: view.getUint32(40, true),
    tenureEpochs: view.getUint16(46, true),
    fundedEpoch: view.getBigUint64(56, true),
    activeEpoch: view.getBigUint64(64, true),
    escrowCount: view.getUint32(144, true),
    fundingAuthorityKey: new PublicKey(data.slice(112, 144)),
  }
}

export interface ParsedPaymentEscrow {
  clientSeatKey: PublicKey
  withdrawAuthorityKey: PublicKey
  usdcBalance: bigint
}

/** Parse a PaymentEscrow on-chain account. Returns null if data is too short. */
export function parsePaymentEscrow(data: Uint8Array): ParsedPaymentEscrow | null {
  if (data.length < DISCRIMINATOR_LEN + 72) return null
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)

  return {
    clientSeatKey: new PublicKey(data.slice(8, 40)),
    withdrawAuthorityKey: new PublicKey(data.slice(40, 72)),
    usdcBalance: view.getBigUint64(72, true),
  }
}

/** Parse the metro exchange key from a DeviceHistory account (offset 56..88). */
export function parseDeviceHistoryExchangeKey(data: Uint8Array): PublicKey | null {
  if (data.length < DISCRIMINATOR_LEN + 80) return null
  return new PublicKey(data.slice(56, 88))
}
