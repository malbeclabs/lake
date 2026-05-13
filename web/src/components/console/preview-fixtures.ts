import type { ShredClientSeat, ShredEscrowEvent } from '@/lib/api'

/** Anchor epoch used to evaluate fixture seats — matches the design mockup. */
export const PREVIEW_CURRENT_EPOCH = 437

const NOW = () => new Date()
const HOURS_AGO = (h: number) => new Date(NOW().getTime() - h * 3_600_000).toISOString()
const DAYS_AGO = (d: number) => new Date(NOW().getTime() - d * 86_400_000).toISOString()

const PREVIEW_FUNDER = 'preview-funder'

/**
 * Sample seats shown only when an internal user enters preview mode
 * (?preview=true) on the Subscriptions Console. RFC 5737 TEST-NET-2 IPs.
 * Covers all four `SeatStatus` states: active / low / pending / expired.
 */
export const PREVIEW_SEATS: ShredClientSeat[] = [
  {
    pk: 'preview-seat-sto',
    device_key: 'preview-device-sto',
    device_code: 'dz-preview-sto',
    metro_pk: 'preview-metro-sto',
    metro_code: 'sto',
    client_ip: '198.51.100.10',
    tenure_epochs: 12,
    funded_epoch: PREVIEW_CURRENT_EPOCH,
    active_epoch: PREVIEW_CURRENT_EPOCH,
    has_price_override: 0,
    override_usdc_price_dollars: 0,
    escrow_count: 5,
    total_usdc_balance: 1_800_000_000, // $1,800
    price_per_epoch_dollars: 150,
    funding_authority_key: PREVIEW_FUNDER,
    user_pk: 'preview-user-sto',
    user_owner_pubkey: 'preview-owner',
    user_status: 'active',
    last_activity: HOURS_AGO(2),
  },
  {
    pk: 'preview-seat-fra1',
    device_key: 'preview-device-fra1',
    device_code: 'dz-preview-fra1',
    metro_pk: 'preview-metro-fra',
    metro_code: 'fra',
    client_ip: '198.51.100.11',
    tenure_epochs: 8,
    funded_epoch: PREVIEW_CURRENT_EPOCH,
    active_epoch: PREVIEW_CURRENT_EPOCH,
    has_price_override: 0,
    override_usdc_price_dollars: 0,
    escrow_count: 3,
    total_usdc_balance: 2_400_000_000, // $2,400
    price_per_epoch_dollars: 300,
    funding_authority_key: PREVIEW_FUNDER,
    user_pk: 'preview-user-fra1',
    user_owner_pubkey: 'preview-owner',
    user_status: 'active',
    last_activity: HOURS_AGO(6),
  },
  {
    pk: 'preview-seat-ams',
    device_key: 'preview-device-ams',
    device_code: 'dz-preview-ams',
    metro_pk: 'preview-metro-ams',
    metro_code: 'ams',
    client_ip: '198.51.100.12',
    tenure_epochs: 1,
    funded_epoch: PREVIEW_CURRENT_EPOCH,
    active_epoch: PREVIEW_CURRENT_EPOCH,
    has_price_override: 0,
    override_usdc_price_dollars: 0,
    escrow_count: 1,
    total_usdc_balance: 50_000_000, // $50 — half an epoch
    price_per_epoch_dollars: 100,
    funding_authority_key: PREVIEW_FUNDER,
    user_pk: 'preview-user-ams',
    user_owner_pubkey: 'preview-owner',
    user_status: 'expiring',
    last_activity: HOURS_AGO(1),
  },
  {
    pk: 'preview-seat-fra2',
    device_key: 'preview-device-fra2',
    device_code: 'dz-preview-fra2',
    metro_pk: 'preview-metro-fra',
    metro_code: 'fra',
    client_ip: '198.51.100.13',
    tenure_epochs: 2,
    funded_epoch: PREVIEW_CURRENT_EPOCH,
    active_epoch: PREVIEW_CURRENT_EPOCH,
    has_price_override: 0,
    override_usdc_price_dollars: 0,
    escrow_count: 2,
    total_usdc_balance: 200_000_000, // $200 — 1 epoch
    price_per_epoch_dollars: 200,
    funding_authority_key: PREVIEW_FUNDER,
    user_pk: 'preview-user-fra2',
    user_owner_pubkey: 'preview-owner',
    user_status: 'expiring',
    last_activity: HOURS_AGO(4),
  },
  {
    pk: 'preview-seat-osl',
    device_key: 'preview-device-osl',
    device_code: 'dz-preview-osl',
    metro_pk: 'preview-metro-osl',
    metro_code: 'osl',
    client_ip: '198.51.100.14',
    tenure_epochs: 60,
    funded_epoch: PREVIEW_CURRENT_EPOCH,
    active_epoch: PREVIEW_CURRENT_EPOCH,
    has_price_override: 0,
    override_usdc_price_dollars: 0,
    escrow_count: 4,
    total_usdc_balance: 7_200_000_000, // $7,200 — 60 epochs
    price_per_epoch_dollars: 120,
    funding_authority_key: PREVIEW_FUNDER,
    user_pk: 'preview-user-osl',
    user_owner_pubkey: 'preview-owner',
    user_status: 'active',
    last_activity: DAYS_AGO(1),
  },
  {
    pk: 'preview-seat-bom',
    device_key: 'preview-device-bom',
    device_code: 'dz-preview-bom',
    metro_pk: 'preview-metro-bom',
    metro_code: 'bom',
    client_ip: '198.51.100.15',
    tenure_epochs: 0,
    funded_epoch: PREVIEW_CURRENT_EPOCH + 1,
    active_epoch: PREVIEW_CURRENT_EPOCH + 1, // pending: activates next epoch
    has_price_override: 0,
    override_usdc_price_dollars: 0,
    escrow_count: 2,
    total_usdc_balance: 240_000_000, // $240
    price_per_epoch_dollars: 60,
    funding_authority_key: PREVIEW_FUNDER,
    user_pk: 'preview-user-bom',
    user_owner_pubkey: 'preview-owner',
    user_status: 'pending',
    last_activity: HOURS_AGO(0.2),
  },
  {
    pk: 'preview-seat-chi',
    device_key: 'preview-device-chi',
    device_code: 'dz-preview-chi',
    metro_pk: 'preview-metro-chi',
    metro_code: 'chi',
    client_ip: '198.51.100.16',
    tenure_epochs: 28,
    funded_epoch: PREVIEW_CURRENT_EPOCH,
    active_epoch: PREVIEW_CURRENT_EPOCH,
    has_price_override: 0,
    override_usdc_price_dollars: 0,
    escrow_count: 8,
    total_usdc_balance: 6_720_000_000, // $6,720 — 28 epochs
    price_per_epoch_dollars: 240,
    funding_authority_key: PREVIEW_FUNDER,
    user_pk: 'preview-user-chi',
    user_owner_pubkey: 'preview-owner',
    user_status: 'active',
    last_activity: HOURS_AGO(3),
  },
  {
    pk: 'preview-seat-prg',
    device_key: 'preview-device-prg',
    device_code: 'dz-preview-prg',
    metro_pk: 'preview-metro-prg',
    metro_code: 'prg',
    client_ip: '198.51.100.17',
    tenure_epochs: 4,
    funded_epoch: PREVIEW_CURRENT_EPOCH - 4,
    active_epoch: PREVIEW_CURRENT_EPOCH - 4, // expired: well behind current
    has_price_override: 0,
    override_usdc_price_dollars: 0,
    escrow_count: 1,
    total_usdc_balance: 0,
    price_per_epoch_dollars: 30,
    funding_authority_key: PREVIEW_FUNDER,
    user_pk: 'preview-user-prg',
    user_owner_pubkey: 'preview-owner',
    user_status: 'inactive',
    last_activity: DAYS_AGO(4),
  },
]

/**
 * Fake escrow events keyed by seat pk, used by the drawer's Activity tab when
 * preview mode is on. Negative balances/amounts mirror the real schema where
 * `amount_usdc` is in micro-USDC.
 */
export const PREVIEW_EVENTS_BY_SEAT: Record<string, ShredEscrowEvent[]> = {
  'preview-seat-ams': [
    mkEvent({ ts: HOURS_AGO(0.5), type: 'fund', amount: 50_000_000, balance: 50_000_000, epoch: 437, seat: 'preview-seat-ams' }),
    mkEvent({ ts: DAYS_AGO(1), type: 'batch_settle', amount: -100_000_000, balance: 0, epoch: 436, seat: 'preview-seat-ams' }),
    mkEvent({ ts: DAYS_AGO(2), type: 'batch_settle', amount: -100_000_000, balance: 100_000_000, epoch: 435, seat: 'preview-seat-ams' }),
    mkEvent({ ts: DAYS_AGO(3), type: 'batch_settle', amount: -100_000_000, balance: 200_000_000, epoch: 434, seat: 'preview-seat-ams' }),
    mkEvent({ ts: DAYS_AGO(4), type: 'initialize_escrow', amount: null, balance: 300_000_000, epoch: 433, seat: 'preview-seat-ams' }),
    mkEvent({ ts: DAYS_AGO(4), type: 'initialize_seat', amount: 400_000_000, balance: 400_000_000, epoch: 433, seat: 'preview-seat-ams' }),
  ],
  'preview-seat-sto': [
    mkEvent({ ts: HOURS_AGO(20), type: 'batch_settle', amount: -150_000_000, balance: 1_800_000_000, epoch: 437, seat: 'preview-seat-sto' }),
    mkEvent({ ts: DAYS_AGO(2), type: 'batch_settle', amount: -150_000_000, balance: 1_950_000_000, epoch: 436, seat: 'preview-seat-sto' }),
    mkEvent({ ts: DAYS_AGO(5), type: 'fund', amount: 1_500_000_000, balance: 2_100_000_000, epoch: 435, seat: 'preview-seat-sto' }),
    mkEvent({ ts: DAYS_AGO(7), type: 'initialize_seat', amount: 750_000_000, balance: 750_000_000, epoch: 433, seat: 'preview-seat-sto' }),
  ],
  'preview-seat-fra1': [
    mkEvent({ ts: HOURS_AGO(8), type: 'batch_settle', amount: -300_000_000, balance: 2_400_000_000, epoch: 437, seat: 'preview-seat-fra1' }),
    mkEvent({ ts: DAYS_AGO(2), type: 'batch_settle', amount: -300_000_000, balance: 2_700_000_000, epoch: 436, seat: 'preview-seat-fra1' }),
    mkEvent({ ts: DAYS_AGO(6), type: 'fund', amount: 2_400_000_000, balance: 3_000_000_000, epoch: 435, seat: 'preview-seat-fra1' }),
    mkEvent({ ts: DAYS_AGO(10), type: 'initialize_seat', amount: 900_000_000, balance: 900_000_000, epoch: 432, seat: 'preview-seat-fra1' }),
  ],
  'preview-seat-fra2': [
    mkEvent({ ts: HOURS_AGO(5), type: 'batch_settle', amount: -200_000_000, balance: 200_000_000, epoch: 437, seat: 'preview-seat-fra2' }),
    mkEvent({ ts: DAYS_AGO(2), type: 'batch_settle', amount: -200_000_000, balance: 400_000_000, epoch: 436, seat: 'preview-seat-fra2' }),
    mkEvent({ ts: DAYS_AGO(5), type: 'initialize_seat', amount: 600_000_000, balance: 600_000_000, epoch: 434, seat: 'preview-seat-fra2' }),
  ],
  'preview-seat-osl': [
    mkEvent({ ts: HOURS_AGO(2), type: 'batch_settle', amount: -120_000_000, balance: 7_200_000_000, epoch: 437, seat: 'preview-seat-osl' }),
    mkEvent({ ts: DAYS_AGO(8), type: 'fund', amount: 6_000_000_000, balance: 7_320_000_000, epoch: 433, seat: 'preview-seat-osl' }),
    mkEvent({ ts: DAYS_AGO(15), type: 'initialize_seat', amount: 1_320_000_000, balance: 1_320_000_000, epoch: 426, seat: 'preview-seat-osl' }),
  ],
  'preview-seat-bom': [
    mkEvent({ ts: HOURS_AGO(0.3), type: 'fund', amount: 240_000_000, balance: 240_000_000, epoch: 437, seat: 'preview-seat-bom' }),
    mkEvent({ ts: HOURS_AGO(0.3), type: 'initialize_seat', amount: null, balance: 0, epoch: 437, seat: 'preview-seat-bom' }),
  ],
  'preview-seat-chi': [
    mkEvent({ ts: HOURS_AGO(4), type: 'batch_settle', amount: -240_000_000, balance: 6_720_000_000, epoch: 437, seat: 'preview-seat-chi' }),
    mkEvent({ ts: DAYS_AGO(3), type: 'fund', amount: 5_000_000_000, balance: 6_960_000_000, epoch: 435, seat: 'preview-seat-chi' }),
    mkEvent({ ts: DAYS_AGO(30), type: 'initialize_seat', amount: 1_960_000_000, balance: 1_960_000_000, epoch: 410, seat: 'preview-seat-chi' }),
  ],
  'preview-seat-prg': [
    mkEvent({ ts: DAYS_AGO(3), type: 'close', amount: null, balance: 0, epoch: 434, seat: 'preview-seat-prg' }),
    mkEvent({ ts: DAYS_AGO(3), type: 'withdraw_seat', amount: 0, balance: 0, epoch: 434, seat: 'preview-seat-prg' }),
    mkEvent({ ts: DAYS_AGO(4), type: 'batch_settle', amount: -30_000_000, balance: 0, epoch: 433, seat: 'preview-seat-prg' }),
    mkEvent({ ts: DAYS_AGO(5), type: 'batch_settle', amount: -30_000_000, balance: 30_000_000, epoch: 432, seat: 'preview-seat-prg' }),
    mkEvent({ ts: DAYS_AGO(35), type: 'initialize_seat', amount: 120_000_000, balance: 120_000_000, epoch: 414, seat: 'preview-seat-prg' }),
  ],
}

function mkEvent({
  ts, type, amount, balance, epoch, seat,
}: {
  ts: string
  type: string
  amount: number | null
  balance: number | null
  epoch: number | null
  seat: string
}): ShredEscrowEvent {
  return {
    event_ts: ts,
    escrow_pk: `preview-escrow-${seat}`,
    client_seat_pk: seat,
    tx_signature: 'PreviewTx_NotReal_' + Math.floor(Math.random() * 1_000_000),
    slot: 0,
    event_type: type,
    amount_usdc: amount,
    balance_after_usdc: balance,
    epoch,
    status: 'confirmed',
    signer: 'PreviewSigner',
    client_ip: '198.51.100.1',
    solscan_url: '',
  }
}
