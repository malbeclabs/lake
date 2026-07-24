package v1

import (
	"context"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/malbeclabs/lake/api/handlers"
)

// EdgeShredsClientSeat is the public, stable shape of a single shred client
// seat. This is the same row source as /edge/shreds/subscribers but exposes a
// few extra fields (funded_epoch, escrow_count, has_price_override,
// override_usdc_price_dollars) that wallet UIs need to display the active
// state and remaining-balance estimate. client_ip is intentionally omitted:
// v1 is unauthed and the internal handler redacts it for non-internal callers.
type EdgeShredsClientSeat struct {
	SeatPK                   string `json:"seat_pk" doc:"Client seat pubkey"`
	DeviceKey                string `json:"device_key" doc:"DoubleZero edge device pubkey"`
	DeviceCode               string `json:"device_code" doc:"DoubleZero edge device code"`
	MetroPK                  string `json:"metro_pk" doc:"DoubleZero metro pubkey"`
	MetroCode                string `json:"metro_code" doc:"DoubleZero metro code"`
	TenureEpochs             uint16 `json:"tenure_epochs" doc:"Number of epochs this seat has been active"`
	FundedEpoch              uint64 `json:"funded_epoch" doc:"Epoch the seat was most recently funded for"`
	ActiveEpoch              uint64 `json:"active_epoch" doc:"Epoch the seat became active"`
	HasPriceOverride         uint8  `json:"has_price_override" doc:"1 if the seat has an explicit per-seat price override, 0 otherwise"`
	OverrideUSDCPriceDollars uint16 `json:"override_usdc_price_dollars" doc:"Per-seat price override (whole USDC dollars); 0 unless has_price_override = 1"`
	EscrowCount              uint32 `json:"escrow_count" doc:"Number of payment escrows currently attached to the seat"`
	SpendableUSDCBalance     string `json:"spendable_usdc_balance" doc:"Largest single escrow balance, as a decimal USDC string (6 fractional digits). The oracle evaluates activation/renewal per-escrow, so this — not the sum — determines whether the seat can cover the per-epoch price." example:"25.650000"`
	AllEscrowsUSDCBalance    string `json:"all_escrows_usdc_balance" doc:"Sum of USDC balances across all escrows attached to this seat, as a decimal USDC string (6 fractional digits). Informational: cannot be spent as a single charge." example:"31.480000"`
	// TotalUSDCBalance is a deprecated alias of all_escrows_usdc_balance (the
	// across-escrow sum), kept for backward compatibility. Use spendable_usdc_balance
	// to reason about activation and all_escrows_usdc_balance for total held funds.
	TotalUSDCBalance     string `json:"total_usdc_balance" doc:"DEPRECATED: alias of all_escrows_usdc_balance (the across-escrow sum). Use spendable_usdc_balance for activation and all_escrows_usdc_balance for total held funds." example:"31.480000"`
	PricePerEpochDollars int64  `json:"price_per_epoch_dollars" doc:"Effective per-epoch price (override or metro + device premium)"`
	FundingAuthorityKey  string `json:"funding_authority_key" doc:"Funder pubkey (on-chain authority that funded this seat)"`
	UserPK               string `json:"user_pk" doc:"Linked DoubleZero user pubkey, if any"`
	UserOwnerPubkey      string `json:"user_owner_pubkey" doc:"Solana wallet that owns the linked DZ user"`
	UserStatus           string `json:"user_status" doc:"Linked DZ user status (e.g. activated)"`
	LastActivity         string `json:"last_activity" doc:"RFC3339 timestamp of the last escrow event for this seat, if any" example:"2026-04-23T12:34:56Z"`
}

// EdgeShredsClientSeatsResponse is the paginated response body.
type EdgeShredsClientSeatsResponse struct {
	Items  []EdgeShredsClientSeat `json:"items"`
	Total  int                    `json:"total" doc:"Total matching client seats (ignores limit/offset)"`
	Limit  int                    `json:"limit"`
	Offset int                    `json:"offset"`
}

// EdgeShredsClientSeatsInput is the request for the client-seats endpoint.
type EdgeShredsClientSeatsInput struct {
	Funder string `query:"funder" doc:"Filter by funder pubkey (funding_authority_key, exact match)" example:""`
	Limit  int    `query:"limit" minimum:"1" maximum:"1000" default:"100" doc:"Maximum items to return"`
	Offset int    `query:"offset" minimum:"0" default:"0" doc:"Offset into the result set"`
}

// EdgeShredsClientSeatsOutput wraps the response body for huma.
type EdgeShredsClientSeatsOutput struct {
	Body EdgeShredsClientSeatsResponse
}

func registerEdgeShredsClientSeats(humaAPI huma.API, api *handlers.API) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "list-edge-shreds-client-seats",
		Method:      "GET",
		Path:        "/edge/shreds/client-seats",
		Summary:     "List shred client seats",
		Description: "Returns a paginated list of shred client seats. Same row source as /edge/shreds/subscribers, but exposes funded_epoch, escrow_count, and per-seat price-override fields useful for wallet UIs displaying balance and active state. Optionally filter by funder pubkey. client_ip is not exposed.",
		Tags:        []string{"Edge/Shreds"},
	}, func(ctx context.Context, input *EdgeShredsClientSeatsInput) (*EdgeShredsClientSeatsOutput, error) {
		rows, total, err := api.FetchShredSubscribers(ctx, input.Funder, input.Limit, input.Offset)
		if err != nil {
			return nil, huma.Error500InternalServerError("failed to fetch shred client seats", err)
		}

		items := make([]EdgeShredsClientSeat, len(rows))
		for i, r := range rows {
			items[i] = EdgeShredsClientSeat{
				SeatPK:                   r.PK,
				DeviceKey:                r.DeviceKey,
				DeviceCode:               r.DeviceCode,
				MetroPK:                  r.MetroPK,
				MetroCode:                r.MetroCode,
				TenureEpochs:             r.TenureEpochs,
				FundedEpoch:              r.FundedEpoch,
				ActiveEpoch:              r.ActiveEpoch,
				HasPriceOverride:         r.HasPriceOverride,
				OverrideUSDCPriceDollars: r.OverrideUSDCPriceDollars,
				EscrowCount:              r.EscrowCount,
				SpendableUSDCBalance:     formatUSDC(r.SpendableUSDCBalance),
				AllEscrowsUSDCBalance:    formatUSDC(r.AllEscrowsUSDCBalance),
				TotalUSDCBalance:         formatUSDC(r.AllEscrowsUSDCBalance),
				PricePerEpochDollars:     r.PricePerEpochDollars,
				FundingAuthorityKey:      r.FundingAuthorityKey,
				UserPK:                   r.UserPK,
				UserOwnerPubkey:          r.UserOwnerPubkey,
				UserStatus:               r.UserStatus,
			}
			if r.LastActivity != nil && !r.LastActivity.IsZero() {
				items[i].LastActivity = r.LastActivity.UTC().Format(time.RFC3339)
			}
		}

		return &EdgeShredsClientSeatsOutput{Body: EdgeShredsClientSeatsResponse{
			Items:  items,
			Total:  int(total),
			Limit:  input.Limit,
			Offset: input.Offset,
		}}, nil
	})
}
