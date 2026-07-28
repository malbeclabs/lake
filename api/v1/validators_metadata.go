package v1

import (
	"context"

	"github.com/danielgtaylor/huma/v2"

	"github.com/malbeclabs/lake/api/handlers"
)

// ValidatorMetadata is a stable public shape for a single validator's
// client/version/stake metadata.
type ValidatorMetadata struct {
	IP              string `json:"ip" doc:"Validator gossip IP"`
	ActiveStake     int64  `json:"active_stake" doc:"Active stake in lamports"`
	VoteAccount     string `json:"vote_account" doc:"Solana vote account pubkey"`
	SoftwareClient  string `json:"software_client" doc:"Validator client name (e.g. Agave, Jito, Firedancer)"`
	SoftwareVersion string `json:"software_version" doc:"Validator client version"`
}

// ValidatorsMetadataOutput wraps the response body for huma.
type ValidatorsMetadataOutput struct {
	Body []ValidatorMetadata
}

func registerValidatorsMetadata(humaAPI huma.API, api *handlers.API) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "list-solana-validators-metadata",
		Method:      "GET",
		Path:        "/solana/validators-metadata",
		Summary:     "List Solana validator metadata",
		Description: "Returns client name/version/stake metadata for every active Solana validator, ordered by active stake descending.",
		Tags:        []string{"Solana"},
	}, func(ctx context.Context, _ *struct{}) (*ValidatorsMetadataOutput, error) {
		rows, err := api.FetchValidatorsMetadataCachedOrLive(ctx)
		if err != nil {
			return nil, huma.Error500InternalServerError("failed to fetch validators metadata", err)
		}

		items := make([]ValidatorMetadata, len(rows))
		for i, r := range rows {
			items[i] = ValidatorMetadata{
				IP:              r.IP,
				ActiveStake:     r.ActiveStake,
				VoteAccount:     r.VoteAccount,
				SoftwareClient:  r.SoftwareClient,
				SoftwareVersion: r.SoftwareVersion,
			}
		}

		return &ValidatorsMetadataOutput{Body: items}, nil
	})
}
