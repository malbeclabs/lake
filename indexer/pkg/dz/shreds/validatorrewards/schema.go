package validatorrewards

import (
	"fmt"
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// LeafRow is one row of ValidatorRewardsLeaf data per (subscription_epoch, node_id).
type LeafRow struct {
	PK                string // {subscription_epoch}:{node_id}
	SubscriptionEpoch uint64
	AssociatedDZEpoch uint64
	NodeID            string
	LeaderSlots       uint32
	ClientID          uint16
	LeafIndex         uint32
}

type leafSchema struct{}

func (s *leafSchema) Name() string { return "dz_shred_validator_rewards_leaves" }

func (s *leafSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *leafSchema) PayloadColumns() []string {
	return []string{
		"subscription_epoch:BIGINT",
		"associated_dz_epoch:BIGINT",
		"node_id:VARCHAR",
		"leader_slots:INTEGER",
		"client_id:INTEGER",
		"leaf_index:INTEGER",
	}
}

func (s *leafSchema) ToRow(r LeafRow) []any {
	return []any{
		r.PK,
		r.SubscriptionEpoch,
		r.AssociatedDZEpoch,
		r.NodeID,
		r.LeaderSlots,
		r.ClientID,
		r.LeafIndex,
	}
}

func (s *leafSchema) GetPrimaryKey(r LeafRow) string { return r.PK }

var schema = &leafSchema{}

func NewLeafDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, schema)
}

// LeafPK builds the canonical primary key for a leaf row.
func LeafPK(subscriptionEpoch uint64, nodeID string) string {
	return fmt.Sprintf("epoch-%d:node-%s", subscriptionEpoch, nodeID)
}

// LeafDistributionStatusRow is one row per (subscription_epoch, node_id)
// recording the publisher-accumulation bitmap bit: 1 = accumulated but not
// yet distributed (immediately claimable); 0 = either not yet accumulated
// or already distributed.
type LeafDistributionStatusRow struct {
	PK                string // {subscription_epoch}:{node_id}
	SubscriptionEpoch uint64
	NodeID            string
	IsClaimable       uint8
	JournalMintKey    string // base58 of the journal mint we read (the 2Z mint for v1)
}

type leafDistributionStatusSchema struct{}

func (s *leafDistributionStatusSchema) Name() string {
	return "dz_shred_validator_leaf_distribution_status"
}

func (s *leafDistributionStatusSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *leafDistributionStatusSchema) PayloadColumns() []string {
	return []string{
		"subscription_epoch:BIGINT",
		"node_id:VARCHAR",
		"is_claimable:INTEGER",
		"journal_mint_key:VARCHAR",
	}
}

func (s *leafDistributionStatusSchema) ToRow(r LeafDistributionStatusRow) []any {
	return []any{r.PK, r.SubscriptionEpoch, r.NodeID, r.IsClaimable, r.JournalMintKey}
}

func (s *leafDistributionStatusSchema) GetPrimaryKey(r LeafDistributionStatusRow) string {
	return r.PK
}

var leafDistributionStatusSchemaSingleton = &leafDistributionStatusSchema{}

// NewLeafDistributionStatusDataset constructs a dim-type-2 dataset for the
// (subscription_epoch, node_id) claimability bitmap projection.
func NewLeafDistributionStatusDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, leafDistributionStatusSchemaSingleton)
}

// LeafDistributionStatusPK builds the canonical primary key.
func LeafDistributionStatusPK(subscriptionEpoch uint64, nodeID string) string {
	return fmt.Sprintf("epoch-%d:node-%s", subscriptionEpoch, nodeID)
}
