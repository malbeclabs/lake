package validatorrewards

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// StoreConfig configures a validatorrewards Store.
type StoreConfig struct {
	Logger     *slog.Logger
	ClickHouse clickhouse.Client
}

// Validate returns an error if the config is missing required fields.
func (cfg *StoreConfig) Validate() error {
	if cfg.Logger == nil {
		return errors.New("logger is required")
	}
	if cfg.ClickHouse == nil {
		return errors.New("clickhouse connection is required")
	}
	return nil
}

// Store persists validator-rewards leaf snapshots into the
// `dim_dz_shred_validator_rewards_leaves_*` dim-type-2 dataset.
type Store struct {
	log *slog.Logger
	cfg StoreConfig
}

// NewStore constructs a Store from validated config.
func NewStore(cfg StoreConfig) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Store{
		log: cfg.Logger,
		cfg: cfg,
	}, nil
}

// HighestIndexedEpoch returns the largest subscription_epoch currently
// present in the leaves _current view, or 0 when the table is empty. Used
// as a coarse "have we indexed this epoch yet?" check by higher layers.
func (s *Store) HighestIndexedEpoch(ctx context.Context) (uint64, error) {
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	const q = `SELECT coalesce(max(subscription_epoch), 0) FROM dim_dz_shred_validator_rewards_leaves_current`

	rows, err := conn.Query(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("query highest indexed epoch: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, nil
	}
	var epoch uint64
	if err := rows.Scan(&epoch); err != nil {
		return 0, fmt.Errorf("scan highest indexed epoch: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate highest indexed epoch: %w", err)
	}
	return epoch, nil
}

// LeavesStatus reports what kind of leaves are already indexed for the given
// subscription_epoch. The two flags are independent: an epoch may have only
// unverified rows (indexed before the on-chain root posted), only verified
// rows (the normal path once the root is up), or be empty.
type LeavesStatus struct {
	HasVerified   bool
	HasUnverified bool
}

// LeavesStatusForEpoch returns the verification status of any leaves indexed
// for the given subscription_epoch. The view refresh loop uses this to:
//   - Skip epochs that are already verified (the on-chain root is immutable).
//   - Skip epochs with unverified rows when the root is still zero (we already
//     have a snapshot; next refresh will retry once the root posts).
//   - Replace unverified rows with verified ones the first time we see a
//     non-zero root for the epoch.
func (s *Store) LeavesStatusForEpoch(ctx context.Context, subscriptionEpoch uint64) (LeavesStatus, error) {
	var status LeavesStatus
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return status, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	const q = `
		SELECT
			countIf(is_verified = 1) > 0,
			countIf(is_verified = 0) > 0
		FROM dim_dz_shred_validator_rewards_leaves_current
		WHERE subscription_epoch = ?`
	rows, err := conn.Query(ctx, q, subscriptionEpoch)
	if err != nil {
		return status, fmt.Errorf("query leaves status for epoch: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return status, nil
	}
	var hasVerified, hasUnverified uint8
	if err := rows.Scan(&hasVerified, &hasUnverified); err != nil {
		return status, fmt.Errorf("scan leaves status for epoch: %w", err)
	}
	status.HasVerified = hasVerified != 0
	status.HasUnverified = hasUnverified != 0
	return status, nil
}

// LeafIdentity is the (node_id, client_id) pair a leaf_index resolves to. A
// validator (node_id) can own several leaves in one epoch — one per software
// client — so the claimable-bit projection must carry the client_id through
// to key its output rows at the same grain as the leaves table.
type LeafIdentity struct {
	NodeID   string
	ClientID uint16
}

// LeafIndexToNodeClient returns the leaf_index → (node_id, client_id) mapping
// for the given subscription_epoch from the leaves _current view. Returns an
// empty (non-nil) map when there are no leaves for that epoch (e.g., S3
// verifier hasn't run yet).
func (s *Store) LeafIndexToNodeClient(ctx context.Context, subscriptionEpoch uint64) (map[uint32]LeafIdentity, error) {
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	const q = `SELECT leaf_index, node_id, client_id
		FROM dim_dz_shred_validator_rewards_leaves_current
		WHERE subscription_epoch = ?`

	rows, err := conn.Query(ctx, q, subscriptionEpoch)
	if err != nil {
		return nil, fmt.Errorf("query leaf_index → (node_id, client_id) mapping: %w", err)
	}
	defer rows.Close()

	out := make(map[uint32]LeafIdentity)
	for rows.Next() {
		var (
			leafIndex uint32
			nodeID    string
			clientID  uint16
		)
		if err := rows.Scan(&leafIndex, &nodeID, &clientID); err != nil {
			return nil, fmt.Errorf("scan leaf_index → (node_id, client_id) row: %w", err)
		}
		out[leafIndex] = LeafIdentity{NodeID: nodeID, ClientID: clientID}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate leaf_index → (node_id, client_id) rows: %w", err)
	}
	return out, nil
}

// ExistingLeafMints returns the reward mint already recorded for each
// (node_id, client_id) leaf of an epoch, from the leaf_distribution_status
// _current view. The status projection uses this to tell an already-distributed
// leaf (its journal's publisher bit cleared) apart from a leaf that never
// belonged to the journal: only a leaf previously attributed to a token may be
// marked distributed in that token. Returns an empty (non-nil) map when no
// statuses exist yet for the epoch.
func (s *Store) ExistingLeafMints(ctx context.Context, subscriptionEpoch uint64) (map[LeafIdentity]string, error) {
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	const q = `SELECT node_id, client_id, journal_mint_key
		FROM dim_dz_shred_validator_leaf_distribution_status_current
		WHERE subscription_epoch = ?`

	rows, err := conn.Query(ctx, q, subscriptionEpoch)
	if err != nil {
		return nil, fmt.Errorf("query existing leaf mints: %w", err)
	}
	defer rows.Close()

	out := make(map[LeafIdentity]string)
	for rows.Next() {
		var (
			nodeID   string
			clientID uint16
			mint     string
		)
		if err := rows.Scan(&nodeID, &clientID, &mint); err != nil {
			return nil, fmt.Errorf("scan existing leaf mint row: %w", err)
		}
		out[LeafIdentity{NodeID: nodeID, ClientID: clientID}] = mint
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate existing leaf mint rows: %w", err)
	}
	return out, nil
}

// ReplaceLeaves writes the verified leaves as a fresh dim-type-2 snapshot
// for (subscriptionEpoch, associatedDZEpoch). Each leaf becomes one row
// with LeafIndex set to its position in the sorted slice.
//
// A nil or empty VerifiedLeaves is a no-op.
//
// MissingMeansDeleted is false: a missing epoch in some future call must
// not tombstone earlier epochs' leaves. Each call is additive snapshot of
// that epoch's set.
func (s *Store) ReplaceLeaves(
	ctx context.Context,
	subscriptionEpoch uint64,
	associatedDZEpoch uint64,
	v *VerifiedLeaves,
) error {
	if v == nil || len(v.Leaves) == 0 {
		return nil
	}
	if len(v.NodeIDStrings) != len(v.Leaves) {
		return fmt.Errorf("verified-leaves invariant: NodeIDStrings (%d) and Leaves (%d) lengths differ",
			len(v.NodeIDStrings), len(v.Leaves))
	}

	s.log.Debug("validatorrewards/store: replacing leaves",
		"subscription_epoch", subscriptionEpoch,
		"associated_dz_epoch", associatedDZEpoch,
		"count", len(v.Leaves),
	)

	ds, err := NewLeafDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create leaf dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	var verifiedFlag uint8
	if v.IsVerified {
		verifiedFlag = 1
	}

	toRow := func(i int) ([]any, error) {
		nodeID := v.NodeIDStrings[i]
		row := LeafRow{
			PK:                LeafPK(subscriptionEpoch, nodeID, v.Leaves[i].ClientID),
			SubscriptionEpoch: subscriptionEpoch,
			AssociatedDZEpoch: associatedDZEpoch,
			NodeID:            nodeID,
			LeaderSlots:       v.Leaves[i].LeaderSlots,
			ClientID:          v.Leaves[i].ClientID,
			LeafIndex:         uint32(i),
			IsVerified:        verifiedFlag,
		}
		return schema.ToRow(row), nil
	}

	if err := ds.WriteBatch(ctx, conn, len(v.Leaves), toRow, &dataset.DimensionType2DatasetWriteConfig{
		// Per-epoch snapshots are additive; absent rows in this batch
		// must not tombstone other epochs' leaves.
		MissingMeansDeleted: false,
	}); err != nil {
		return fmt.Errorf("failed to write leaves: %w", err)
	}

	s.log.Info("validatorrewards/store: wrote leaves",
		"subscription_epoch", subscriptionEpoch,
		"associated_dz_epoch", associatedDZEpoch,
		"count", len(v.Leaves),
	)

	return nil
}

// ReplaceLeafDistributionStatuses writes a fresh dim-type-2 snapshot of the
// per-(subscription_epoch, node_id) claimability flag derived from the
// on-chain publisher accumulation bitmap.
//
// A nil or empty rows slice is a no-op.
//
// MissingMeansDeleted is false: each call snapshots one journal (epoch),
// and rows from other epochs must not be tombstoned by this call.
func (s *Store) ReplaceLeafDistributionStatuses(
	ctx context.Context,
	rows []LeafDistributionStatusRow,
) error {
	if len(rows) == 0 {
		return nil
	}

	s.log.Debug("validatorrewards/store: replacing leaf distribution statuses",
		"count", len(rows),
	)

	ds, err := NewLeafDistributionStatusDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create leaf distribution status dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	toRow := func(i int) ([]any, error) {
		return leafDistributionStatusSchemaSingleton.ToRow(rows[i]), nil
	}

	if err := ds.WriteBatch(ctx, conn, len(rows), toRow, &dataset.DimensionType2DatasetWriteConfig{
		// Per-epoch snapshots are additive; absent rows in this batch
		// must not tombstone other epochs' statuses.
		MissingMeansDeleted: false,
	}); err != nil {
		return fmt.Errorf("failed to write leaf distribution statuses: %w", err)
	}

	s.log.Info("validatorrewards/store: wrote leaf distribution statuses",
		"count", len(rows),
	)

	return nil
}

// ReplaceDistribution2ZPools writes a fresh dim-type-2 snapshot of the
// per-subscription_epoch 2Z reward pool read from each epoch's 2Z journal.
//
// A nil or empty rows slice is a no-op.
//
// MissingMeansDeleted is false: pool rows accumulate across every epoch ever
// observed (all-time earnings must survive after an epoch's journal is swept),
// so a refresh that no longer sees an old journal must not tombstone its row.
func (s *Store) ReplaceDistribution2ZPools(
	ctx context.Context,
	rows []Distribution2ZPoolRow,
) error {
	if len(rows) == 0 {
		return nil
	}

	s.log.Debug("validatorrewards/store: replacing 2Z distribution pools",
		"count", len(rows),
	)

	ds, err := NewDistribution2ZPoolDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create 2Z distribution pool dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	toRow := func(i int) ([]any, error) {
		return distribution2ZPoolSchemaSingleton.ToRow(rows[i]), nil
	}

	if err := ds.WriteBatch(ctx, conn, len(rows), toRow, &dataset.DimensionType2DatasetWriteConfig{
		// Additive: absent epochs in this batch must not tombstone earlier
		// epochs' pool rows.
		MissingMeansDeleted: false,
	}); err != nil {
		return fmt.Errorf("failed to write 2Z distribution pools: %w", err)
	}

	s.log.Info("validatorrewards/store: wrote 2Z distribution pools",
		"count", len(rows),
	)

	return nil
}
