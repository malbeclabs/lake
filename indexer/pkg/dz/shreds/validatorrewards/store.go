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

// HasLeavesForEpoch reports whether any leaves are already indexed for the
// given subscription_epoch. Used by the view refresh loop to skip re-fetching
// and re-verifying epochs whose leaves we've already persisted (the on-chain
// merkle root is immutable once posted, so an existing leaf set for that
// epoch is already verified).
func (s *Store) HasLeavesForEpoch(ctx context.Context, subscriptionEpoch uint64) (bool, error) {
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	const q = `SELECT count() > 0 FROM dim_dz_shred_validator_rewards_leaves_current WHERE subscription_epoch = ?`
	rows, err := conn.Query(ctx, q, subscriptionEpoch)
	if err != nil {
		return false, fmt.Errorf("query has leaves for epoch: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return false, nil
	}
	var has uint8
	if err := rows.Scan(&has); err != nil {
		return false, fmt.Errorf("scan has leaves for epoch: %w", err)
	}
	return has != 0, nil
}

// LeafIndexToNodeID returns the leaf_index → node_id mapping for the given
// subscription_epoch from the leaves _current view. Returns an empty (non-nil)
// map when there are no leaves for that epoch (e.g., S3 verifier hasn't run
// yet).
func (s *Store) LeafIndexToNodeID(ctx context.Context, subscriptionEpoch uint64) (map[uint32]string, error) {
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	const q = `SELECT leaf_index, node_id
		FROM dim_dz_shred_validator_rewards_leaves_current
		WHERE subscription_epoch = ?`

	rows, err := conn.Query(ctx, q, subscriptionEpoch)
	if err != nil {
		return nil, fmt.Errorf("query leaf_index → node_id mapping: %w", err)
	}
	defer rows.Close()

	out := make(map[uint32]string)
	for rows.Next() {
		var (
			leafIndex uint32
			nodeID    string
		)
		if err := rows.Scan(&leafIndex, &nodeID); err != nil {
			return nil, fmt.Errorf("scan leaf_index → node_id row: %w", err)
		}
		out[leafIndex] = nodeID
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate leaf_index → node_id rows: %w", err)
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

	toRow := func(i int) ([]any, error) {
		nodeID := v.NodeIDStrings[i]
		row := LeafRow{
			PK:                LeafPK(subscriptionEpoch, nodeID),
			SubscriptionEpoch: subscriptionEpoch,
			AssociatedDZEpoch: associatedDZEpoch,
			NodeID:            nodeID,
			LeaderSlots:       v.Leaves[i].LeaderSlots,
			ClientID:          v.Leaves[i].ClientID,
			LeafIndex:         uint32(i),
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
