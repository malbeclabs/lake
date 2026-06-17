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
	// IsVerified is 1 when the row's leaf set was confirmed against the
	// on-chain ShredDistribution.validator_rewards_merkle_root, 0 when it was
	// indexed from S3 before the chain had a non-zero root posted. The
	// indexer re-fetches and verifies unverified rows on subsequent
	// refreshes once the root lands.
	IsVerified uint8
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
		"is_verified:INTEGER",
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
		r.IsVerified,
	}
}

func (s *leafSchema) GetPrimaryKey(r LeafRow) string { return r.PK }

var schema = &leafSchema{}

func NewLeafDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, schema)
}

// LeafPK builds the canonical primary key for a leaf row. The grain is
// (subscription_epoch, node_id, client_id): a validator can publish under
// multiple software clients in a single epoch, each producing its own merkle
// leaf. client_id MUST be part of the key, otherwise the multiple leaves for
// one node collapse to a single row and that node's leader slots (and thus
// earnings) are understated.
func LeafPK(subscriptionEpoch uint64, nodeID string, clientID uint16) string {
	return fmt.Sprintf("epoch-%d:node-%s:client-%d", subscriptionEpoch, nodeID, clientID)
}

// LeafDistributionStatusRow is one row per (subscription_epoch, node_id)
// recording the publisher-accumulation bitmap bit: 1 = accumulated but not
// yet distributed (immediately claimable); 0 = either not yet accumulated
// or already distributed.
type LeafDistributionStatusRow struct {
	PK                string // {subscription_epoch}:{node_id}:{client_id}
	SubscriptionEpoch uint64
	NodeID            string
	ClientID          uint16
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
		"client_id:INTEGER",
		"is_claimable:INTEGER",
		"journal_mint_key:VARCHAR",
	}
}

func (s *leafDistributionStatusSchema) ToRow(r LeafDistributionStatusRow) []any {
	return []any{r.PK, r.SubscriptionEpoch, r.NodeID, r.ClientID, r.IsClaimable, r.JournalMintKey}
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

// LeafDistributionStatusPK builds the canonical primary key. The grain matches
// the leaves table — (subscription_epoch, node_id, client_id) — because the
// publisher-accumulation bitmap is per leaf, and a leaf is a (node, client)
// pair. Keying per node alone would let one client's claimable bit overwrite
// another's for the same validator in an epoch.
func LeafDistributionStatusPK(subscriptionEpoch uint64, nodeID string, clientID uint16) string {
	return fmt.Sprintf("epoch-%d:node-%s:client-%d", subscriptionEpoch, nodeID, clientID)
}

// Distribution2ZPoolRow is one row per (subscription_epoch, reward token)
// recording the reward pool read from that epoch's ShredDistributionJournal for
// the token. Before epoch 968 there was a single 2Z journal per epoch (one row);
// from 968 on, each supported token (2Z, USDC, wSOL) has its own journal owning
// the leaves of validators who picked it (one row per token). This is the pool
// the per-leaf publisher shares are drawn from.
//
// The table name and the 2Z row's PK (epoch-{e}) are retained so the historical
// 2Z pool series — which must survive journal sweeps for all-time earnings — is
// preserved; non-2Z rows are keyed per (epoch, mint). See DistributionPoolPK.
type Distribution2ZPoolRow struct {
	PK                string // epoch-{subscription_epoch}[:mint-{reward_mint}]
	SubscriptionEpoch uint64
	// TokensReceived2Z is the pool size in base units of RewardMint (the column
	// name is legacy; it now holds whichever token the journal pays out).
	TokensReceived2Z uint64
	// TotalLeaderSlots is the journal's authoritative leader-slot count for the
	// epoch (see JournalView.TotalLeaderSlots). Legacy fallback denominator used
	// when AccumulatedSlotsScaled is unavailable (older/swept rows).
	TotalLeaderSlots uint64
	// RewardMint is the base58 mint this pool pays out in (JournalView.RewardMintKey).
	// Empty on legacy rows written before multi-token support; the API treats an
	// empty mint as 2Z.
	RewardMint string
	// AccumulatedSlotsScaled is the journal's per-token reward denominator:
	// accumulated_publisher_slots_scaled + accumulated_client_slots_scaled, which
	// equals (this token's leader slots) × 10000. The API divides
	// tokens × leader_slots × (10000 − client_proportion) by it. Zero on legacy
	// rows; the API falls back to TotalLeaderSlots × 10000 then summed leaves.
	AccumulatedSlotsScaled uint64
	// DistributedAmount is the journal's running total of paid-out reward base
	// units (JournalView.DistributedAmount). For non-2Z tokens this is the pool
	// the page splits (their TokensReceived2Z is 0); the 2Z journal instead uses
	// TokensReceived2Z less the 10% burn. Zero on legacy rows.
	DistributedAmount uint64
}

type distribution2ZPoolSchema struct{}

func (s *distribution2ZPoolSchema) Name() string {
	return "dz_shred_distribution_2z_pool"
}

func (s *distribution2ZPoolSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *distribution2ZPoolSchema) PayloadColumns() []string {
	return []string{
		"subscription_epoch:BIGINT",
		"tokens_received_2z:BIGINT",
		"total_leader_slots:BIGINT",
		"reward_mint:VARCHAR",
		"accumulated_slots_scaled:BIGINT",
		"distributed_amount:BIGINT",
	}
}

func (s *distribution2ZPoolSchema) ToRow(r Distribution2ZPoolRow) []any {
	return []any{r.PK, r.SubscriptionEpoch, r.TokensReceived2Z, r.TotalLeaderSlots, r.RewardMint, r.AccumulatedSlotsScaled, r.DistributedAmount}
}

func (s *distribution2ZPoolSchema) GetPrimaryKey(r Distribution2ZPoolRow) string {
	return r.PK
}

var distribution2ZPoolSchemaSingleton = &distribution2ZPoolSchema{}

// NewDistribution2ZPoolDataset constructs a dim-type-2 dataset for the
// per-subscription_epoch 2Z reward pool.
func NewDistribution2ZPoolDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, distribution2ZPoolSchemaSingleton)
}

// Distribution2ZPoolPK builds the canonical primary key for the 2Z pool row.
// The 2Z journal keeps the bare epoch-{e} key so the historical 2Z series
// (written before multi-token support and before the reward_mint column) stays
// one continuous entity per epoch rather than forking into a duplicate row.
func Distribution2ZPoolPK(subscriptionEpoch uint64) string {
	return fmt.Sprintf("epoch-%d", subscriptionEpoch)
}

// DistributionPoolPK builds the primary key for a per-(epoch, reward mint) pool
// row. The 2Z journal uses the legacy bare epoch-{e} key (Distribution2ZPoolPK)
// to preserve its historical series; every other token journal is keyed
// epoch-{e}:mint-{mint} so the per-token pools never collide with it.
func DistributionPoolPK(subscriptionEpoch uint64, rewardMint string) string {
	if rewardMint == DoubleZeroMintKey {
		return Distribution2ZPoolPK(subscriptionEpoch)
	}
	return fmt.Sprintf("epoch-%d:mint-%s", subscriptionEpoch, rewardMint)
}
