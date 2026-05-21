package v1

import (
	"context"

	"github.com/danielgtaylor/huma/v2"

	"github.com/malbeclabs/lake/api/handlers"
)

// EdgeShredsDevice is the public, stable shape of a single shred-eligible
// device. Fields mirror the internal ShredDeviceItem 1:1 — they're already
// derived from on-chain state and safe to expose.
type EdgeShredsDevice struct {
	DeviceKey         string `json:"device_key" doc:"DoubleZero edge device pubkey"`
	DeviceCode        string `json:"device_code" doc:"Human-readable device code (e.g. NYC-CORE-01)"`
	MetroExchangeKey  string `json:"metro_exchange_key" doc:"Metro exchange pubkey the device is registered under"`
	MetroCode         string `json:"metro_code" doc:"Metro code (e.g. NYC, LAX)"`
	IsEnabled         uint8  `json:"is_enabled" doc:"1 if the device is currently enabled for shreds, 0 otherwise"`
	BasePriceDollars  uint16 `json:"base_price_dollars" doc:"Metro base USDC price per epoch (whole dollars; 6-decimal scaling applied client-side)"`
	PremiumDollars    int16  `json:"premium_dollars" doc:"Device-specific USDC premium over the metro base (signed)"`
	TotalPriceDollars int64  `json:"total_price_dollars" doc:"Effective per-epoch USDC price for a seat on this device (base + premium, clamped to >= 0 on chain)"`
	GrantedSeats      uint16 `json:"granted_seats" doc:"Seats currently granted on this device"`
	Capacity          uint16 `json:"capacity" doc:"Total seat capacity available on this device"`
	AvailableSeats    int64  `json:"available_seats" doc:"Capacity minus granted seats (signed for safety; negative would indicate overgranted state)"`
}

// EdgeShredsDevicesResponse is the paginated response body.
type EdgeShredsDevicesResponse struct {
	Items  []EdgeShredsDevice `json:"items"`
	Total  int                `json:"total" doc:"Total devices in the env (ignores limit/offset)"`
	Limit  int                `json:"limit"`
	Offset int                `json:"offset"`
}

// EdgeShredsDevicesInput is the request for the devices endpoint. The v1
// surface intentionally exposes only pagination; the internal
// /api/dz/shreds/devices endpoint still has filter/sort knobs for the lake UI.
type EdgeShredsDevicesInput struct {
	Limit  int `query:"limit" minimum:"1" maximum:"1000" default:"500" doc:"Maximum items to return"`
	Offset int `query:"offset" minimum:"0" default:"0" doc:"Offset into the result set"`
}

// EdgeShredsDevicesOutput wraps the response body for huma.
type EdgeShredsDevicesOutput struct {
	Body EdgeShredsDevicesResponse
}

func registerEdgeShredsDevices(humaAPI huma.API, api *handlers.API) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "list-edge-shreds-devices",
		Method:      "GET",
		Path:        "/edge/shreds/devices",
		Summary:     "List shred-eligible edge devices",
		Description: "Returns a paginated list of edge devices known to the shred subscription program, with per-device pricing and seat availability. Sorted by device_code ascending for deterministic ordering.",
		Tags:        []string{"Edge/Shreds"},
	}, func(ctx context.Context, input *EdgeShredsDevicesInput) (*EdgeShredsDevicesOutput, error) {
		rows, total, err := api.FetchEdgeShredsDevices(ctx, input.Limit, input.Offset)
		if err != nil {
			return nil, huma.Error500InternalServerError("failed to fetch shreds devices", err)
		}

		items := make([]EdgeShredsDevice, len(rows))
		for i, r := range rows {
			items[i] = EdgeShredsDevice{
				DeviceKey:         r.DeviceKey,
				DeviceCode:        r.DeviceCode,
				MetroExchangeKey:  r.MetroExchangeKey,
				MetroCode:         r.MetroCode,
				IsEnabled:         r.IsEnabled,
				BasePriceDollars:  r.BasePriceDollars,
				PremiumDollars:    r.PremiumDollars,
				TotalPriceDollars: r.TotalPriceDollars,
				GrantedSeats:      r.GrantedSeats,
				Capacity:          r.Capacity,
				AvailableSeats:    r.AvailableSeats,
			}
		}

		return &EdgeShredsDevicesOutput{Body: EdgeShredsDevicesResponse{
			Items:  items,
			Total:  int(total),
			Limit:  input.Limit,
			Offset: input.Offset,
		}}, nil
	})
}
