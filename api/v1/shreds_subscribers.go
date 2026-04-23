package v1

import (
	"context"
	"fmt"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/malbeclabs/lake/api/handlers"
)

// usdcMicroDecimals is the on-chain USDC decimal scale on Solana.
const usdcMicroDecimals = 1_000_000

// formatUSDC converts a micro-USDC integer (6 decimals) to a fixed-width
// decimal string like "50.000000". Strings are used instead of floats to
// avoid precision loss for monetary values.
func formatUSDC(microUSDC uint64) string {
	whole := microUSDC / usdcMicroDecimals
	frac := microUSDC % usdcMicroDecimals
	return fmt.Sprintf("%d.%06d", whole, frac)
}

// EdgeShredsSubscriber is a stable public shape for a single shreds subscriber
// (a client seat in the shred subscription program). client_ip is
// intentionally omitted: v1 is unauthed and the internal handler redacts it
// for non-internal callers.
type EdgeShredsSubscriber struct {
	SeatPK               string `json:"seat_pk" doc:"Client seat pubkey"`
	DeviceKey            string `json:"device_key" doc:"DoubleZero edge device pubkey"`
	DeviceCode           string `json:"device_code" doc:"DoubleZero edge device code"`
	MetroPK              string `json:"metro_pk" doc:"DoubleZero metro pubkey"`
	MetroCode            string `json:"metro_code" doc:"DoubleZero metro code"`
	TenureEpochs         uint16 `json:"tenure_epochs" doc:"Number of epochs this seat has been active"`
	ActiveEpoch          uint64 `json:"active_epoch" doc:"Epoch the seat became active"`
	TotalUSDCBalance     string `json:"total_usdc_balance" doc:"Sum of USDC balances across all escrows, as a decimal USDC string (6 fractional digits)" example:"50.000000"`
	PricePerEpochDollars int64  `json:"price_per_epoch_dollars" doc:"Effective per-epoch price (override or metro + device premium)"`
	FundingAuthorityKey  string `json:"funding_authority_key" doc:"Funder pubkey (on-chain authority that funded this seat)"`
	UserPK               string `json:"user_pk" doc:"Linked DoubleZero user pubkey, if any"`
	UserOwnerPubkey      string `json:"user_owner_pubkey" doc:"Solana wallet that owns the linked DZ user"`
	UserStatus           string `json:"user_status" doc:"Linked DZ user status (e.g. activated)"`
	LastActivity         string `json:"last_activity" doc:"RFC3339 timestamp of the last escrow event for this seat, if any" example:"2026-04-23T12:34:56Z"`
}

// EdgeShredsSubscribersResponse is the paginated response body.
type EdgeShredsSubscribersResponse struct {
	Items  []EdgeShredsSubscriber `json:"items"`
	Total  int                    `json:"total" doc:"Total matching subscribers (ignores limit/offset)"`
	Limit  int                    `json:"limit"`
	Offset int                    `json:"offset"`
}

// EdgeShredsSubscribersInput is the request for the subscribers endpoint.
type EdgeShredsSubscribersInput struct {
	Funder string `query:"funder" doc:"Filter by funder pubkey (funding_authority_key, exact match)" example:""`
	Limit  int    `query:"limit" minimum:"1" maximum:"1000" default:"100" doc:"Maximum items to return"`
	Offset int    `query:"offset" minimum:"0" default:"0" doc:"Offset into the result set"`
}

// EdgeShredsSubscribersOutput wraps the response body for huma.
type EdgeShredsSubscribersOutput struct {
	Body EdgeShredsSubscribersResponse
}

func registerEdgeShredsSubscribers(humaAPI huma.API, api *handlers.API) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "list-edge-shreds-subscribers",
		Method:      "GET",
		Path:        "/edge/shreds/subscribers",
		Summary:     "List shreds subscribers",
		Description: "Returns a paginated list of shreds subscribers (client seats in the DoubleZero shred subscription program). Optionally filter by funder pubkey.",
		Tags:        []string{"Edge/Shreds"},
	}, func(ctx context.Context, input *EdgeShredsSubscribersInput) (*EdgeShredsSubscribersOutput, error) {
		rows, total, err := api.FetchShredSubscribers(ctx, input.Funder, input.Limit, input.Offset)
		if err != nil {
			return nil, huma.Error500InternalServerError("failed to fetch shreds subscribers", err)
		}

		items := make([]EdgeShredsSubscriber, len(rows))
		for i, r := range rows {
			items[i] = EdgeShredsSubscriber{
				SeatPK:               r.PK,
				DeviceKey:            r.DeviceKey,
				DeviceCode:           r.DeviceCode,
				MetroPK:              r.MetroPK,
				MetroCode:            r.MetroCode,
				TenureEpochs:         r.TenureEpochs,
				ActiveEpoch:          r.ActiveEpoch,
				TotalUSDCBalance:     formatUSDC(r.TotalUSDCBalance),
				PricePerEpochDollars: r.PricePerEpochDollars,
				FundingAuthorityKey:  r.FundingAuthorityKey,
				UserPK:               r.UserPK,
				UserOwnerPubkey:      r.UserOwnerPubkey,
				UserStatus:           r.UserStatus,
			}
			if r.LastActivity != nil && !r.LastActivity.IsZero() {
				items[i].LastActivity = r.LastActivity.UTC().Format(time.RFC3339)
			}
		}

		return &EdgeShredsSubscribersOutput{Body: EdgeShredsSubscribersResponse{
			Items:  items,
			Total:  int(total),
			Limit:  input.Limit,
			Offset: input.Offset,
		}}, nil
	})
}
