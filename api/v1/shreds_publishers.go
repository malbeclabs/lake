package v1

import (
	"context"

	"github.com/danielgtaylor/huma/v2"

	"github.com/malbeclabs/lake/api/handlers"
)

// EdgeShredsPublisher is a stable public shape for a single shreds publisher.
// It is intentionally decoupled from internal/UI types so the v1 contract
// can evolve independently.
type EdgeShredsPublisher struct {
	PublisherIP             string `json:"publisher_ip" doc:"DoubleZero-assigned IP of the publisher"`
	ClientIP                string `json:"client_ip" doc:"Validator gossip IP advertised to Solana"`
	NodePubkey              string `json:"node_pubkey" doc:"Solana node identity pubkey"`
	VotePubkey              string `json:"vote_pubkey" doc:"Solana vote account pubkey"`
	DZUserPubkey            string `json:"dz_user_pubkey" doc:"DoubleZero user account pubkey"`
	DZDeviceCode            string `json:"dz_device_code" doc:"DoubleZero edge device code"`
	DZMetroCode             string `json:"dz_metro_code" doc:"DoubleZero metro code"`
	ActivatedStake          uint64 `json:"activated_stake" doc:"Activated stake in lamports"`
	MulticastConnected      bool   `json:"multicast_connected" doc:"Whether the publisher is connected to the shred multicast group"`
	PublishingLeaderShreds  bool   `json:"publishing_leader_shreds" doc:"Whether the publisher has produced leader shreds in the window"`
	PublishingRetransmitted bool   `json:"publishing_retransmitted" doc:"Whether the publisher meets the retransmit volume thresholds"`
	LeaderSlots             uint64 `json:"leader_slots" doc:"Distinct slots where this publisher produced leader shreds"`
	TotalSlots              uint64 `json:"total_slots" doc:"Distinct slots observed for this publisher"`
	TotalUniqueShreds       uint64 `json:"total_unique_shreds" doc:"Unique shreds observed across all slots in the window"`
	SlotsNeedingRepair      uint64 `json:"slots_needing_repair" doc:"Number of slots that required repair"`
	ValidatorClient         string `json:"validator_client" doc:"Validator client name (e.g. Agave, Jito, Firedancer)"`
	ValidatorVersion        string `json:"validator_version" doc:"Validator client version"`
	ValidatorName           string `json:"validator_name" doc:"Validator display name from validators.app"`
	ValidatorVersionOk      bool   `json:"validator_version_ok" doc:"Whether the version meets the minimum for its client"`
	IsBackup                bool   `json:"is_backup" doc:"Whether this gossip node lacks an active vote account (hot-spare)"`
}

// EdgeShredsPublishersResponse is the body returned by the shreds publishers endpoint.
type EdgeShredsPublishersResponse struct {
	Epoch               uint64                `json:"epoch" doc:"Current Solana epoch at query time"`
	MaxSlot             uint64                `json:"max_slot" doc:"Highest slot observed in the query window"`
	TotalNetworkStake   int64                 `json:"total_network_stake" doc:"Total active stake across all Solana validators (lamports)"`
	TotalPublishers     uint64                `json:"total_publishers" doc:"Count of activated DZ publishers with a matched vote account"`
	TotalPublisherStake int64                 `json:"total_publisher_stake" doc:"Total activated stake across all DZ publishers (lamports)"`
	Publishers          []EdgeShredsPublisher `json:"publishers"`
}

// EdgeShredsPublishersInput is the request for the shreds publishers endpoint.
type EdgeShredsPublishersInput struct {
	Q      string `query:"q" doc:"Optional filter: DZ user pubkey, publisher IP, or client IP"`
	Epochs int    `query:"epochs" minimum:"1" maximum:"10" default:"2" doc:"Number of recent epochs to include (ignored if slots > 0)"`
	Slots  int    `query:"slots" minimum:"0" maximum:"5000" default:"0" doc:"If > 0, restrict the window to this many most-recent slots instead of epochs. Recommended values: 100, 500, 1000, 5000."`
}

// EdgeShredsPublishersOutput wraps the response body for huma.
type EdgeShredsPublishersOutput struct {
	Body EdgeShredsPublishersResponse
}

func registerEdgeShredsPublishers(humaAPI huma.API, api *handlers.API) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "list-edge-shreds-leader-publishers",
		Method:      "GET",
		Path:        "/edge/shreds/publishers/leaders",
		Summary:     "List shreds leader publishers",
		Description: "Returns the status of every DoubleZero validator-publisher in the shred multicast group for a recent window (epochs or slots). Includes activated stake, leader/retransmit activity, and validator client/version info. A broader publishers endpoint (covering non-validator publishers) may be introduced later at /edge/shreds/publishers.",
		Tags:        []string{"Edge/Shreds"},
	}, func(ctx context.Context, input *EdgeShredsPublishersInput) (*EdgeShredsPublishersOutput, error) {
		resp, err := api.FetchPublisherCheckData(ctx, input.Q, input.Epochs, input.Slots)
		if err != nil {
			return nil, huma.Error500InternalServerError("failed to fetch shreds publishers", err)
		}
		return &EdgeShredsPublishersOutput{Body: toEdgeShredsPublishersResponse(resp)}, nil
	})
}

func toEdgeShredsPublishersResponse(r *handlers.PublisherCheckResponse) EdgeShredsPublishersResponse {
	publishers := make([]EdgeShredsPublisher, len(r.Publishers))
	for i, p := range r.Publishers {
		publishers[i] = EdgeShredsPublisher{
			PublisherIP:             p.PublisherIP,
			ClientIP:                p.ClientIP,
			NodePubkey:              p.NodePubkey,
			VotePubkey:              p.VotePubkey,
			DZUserPubkey:            p.DZUserPubkey,
			DZDeviceCode:            p.DZDeviceCode,
			DZMetroCode:             p.DZMetroCode,
			ActivatedStake:          p.ActivatedStake,
			MulticastConnected:      p.MulticastConnected,
			PublishingLeaderShreds:  p.PublishingLeaderShreds,
			PublishingRetransmitted: p.PublishingRetransmitted,
			LeaderSlots:             p.LeaderSlots,
			TotalSlots:              p.TotalSlots,
			TotalUniqueShreds:       p.TotalUniqueShreds,
			SlotsNeedingRepair:      p.SlotsNeedingRepair,
			ValidatorClient:         p.ValidatorClient,
			ValidatorVersion:        p.ValidatorVersion,
			ValidatorName:           p.ValidatorName,
			ValidatorVersionOk:      p.ValidatorVersionOk,
			IsBackup:                p.IsBackup,
		}
	}
	return EdgeShredsPublishersResponse{
		Epoch:               r.Epoch,
		MaxSlot:             r.MaxSlot,
		TotalNetworkStake:   r.TotalNetworkStake,
		TotalPublishers:     r.TotalPublishers,
		TotalPublisherStake: r.TotalPublisherStake,
		Publishers:          publishers,
	}
}
