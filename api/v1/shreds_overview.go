package v1

import (
	"context"

	"github.com/danielgtaylor/huma/v2"

	"github.com/malbeclabs/lake/api/handlers"
)

// EdgeShredsOverview is the public, stable shape of the shred subscription
// program's overall state. Fields are a 1:1 projection of the internal
// ShredsOverview struct; we keep them stable across releases.
type EdgeShredsOverview struct {
	Phase                      string `json:"phase" doc:"Current execution phase (e.g. 'open for requests', 'closed for requests', 'updating prices', 'settlement', 'settled')"`
	CurrentSubscriptionEpoch   uint64 `json:"current_subscription_epoch" doc:"DZ shred subscription epoch the program is currently in"`
	CurrentSolanaEpoch         uint64 `json:"current_solana_epoch" doc:"Latest observed Solana epoch (max from vote accounts); useful for determining seat active/inactive state"`
	TotalMetros                uint16 `json:"total_metros" doc:"Initialized metro count tracked by the execution controller"`
	TotalEnabledDevices        uint16 `json:"total_enabled_devices" doc:"Devices that are currently enabled for shreds"`
	TotalClientSeats           uint32 `json:"total_client_seats" doc:"Total client seats known to the execution controller"`
	SettledDevicesCount        uint16 `json:"settled_devices_count" doc:"Devices that have completed settlement in the current epoch"`
	SettledClientSeatsCount    uint16 `json:"settled_client_seats_count" doc:"Client seats that have completed settlement in the current epoch"`
	NextSeatFundingIndex       uint64 `json:"next_seat_funding_index" doc:"Monotonic funding index the program will assign to the next funded seat"`
	ClientSeatCount            uint64 `json:"client_seat_count" doc:"Live row count of client seats currently tracked"`
	PaymentEscrowCount         uint64 `json:"payment_escrow_count" doc:"Live row count of payment escrows currently tracked"`
	MetroHistoryCount          uint64 `json:"metro_history_count" doc:"Live row count of metro history entries currently tracked"`
	DeviceHistoryCount         uint64 `json:"device_history_count" doc:"Live row count of device history entries currently tracked"`
	ValidatorClientRewardCount uint64 `json:"validator_client_reward_count" doc:"Live row count of validator client reward entries currently tracked"`
}

// EdgeShredsOverviewInput is empty (no inputs), but huma requires a non-nil
// pointer type so we declare a struct here for symmetry with the rest of v1.
type EdgeShredsOverviewInput struct{}

// EdgeShredsOverviewOutput wraps the response body for huma.
type EdgeShredsOverviewOutput struct {
	Body EdgeShredsOverview
}

func registerEdgeShredsOverview(humaAPI huma.API, api *handlers.API) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "get-edge-shreds-overview",
		Method:      "GET",
		Path:        "/edge/shreds/overview",
		Summary:     "Get shred subscription program overview",
		Description: "Returns the current state of the DoubleZero shred subscription program — execution phase, current subscription/Solana epochs, and aggregate row counts. Use this to determine whether the program is open for requests, what epoch consumers are currently funding, and to size dashboards.",
		Tags:        []string{"Edge/Shreds"},
	}, func(ctx context.Context, _ *EdgeShredsOverviewInput) (*EdgeShredsOverviewOutput, error) {
		o := api.FetchShredsOverview(ctx)
		return &EdgeShredsOverviewOutput{Body: EdgeShredsOverview{
			Phase:                      o.Phase,
			CurrentSubscriptionEpoch:   o.CurrentSubscriptionEpoch,
			CurrentSolanaEpoch:         o.CurrentSolanaEpoch,
			TotalMetros:                o.TotalMetros,
			TotalEnabledDevices:        o.TotalEnabledDevices,
			TotalClientSeats:           o.TotalClientSeats,
			SettledDevicesCount:        o.SettledDevicesCount,
			SettledClientSeatsCount:    o.SettledClientSeatsCount,
			NextSeatFundingIndex:       o.NextSeatFundingIndex,
			ClientSeatCount:            o.ClientSeatCount,
			PaymentEscrowCount:         o.PaymentEscrowCount,
			MetroHistoryCount:          o.MetroHistoryCount,
			DeviceHistoryCount:         o.DeviceHistoryCount,
			ValidatorClientRewardCount: o.ValidatorClientRewardCount,
		}}, nil
	})
}
