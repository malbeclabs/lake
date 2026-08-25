package feedsubscription

import (
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// FeedDistributionRow is one feed's revenue for one calendar month.
type FeedDistributionRow struct {
	// PK is the FeedDistribution account pubkey. It is also the entity_id, so a
	// month keeps its identity across refreshes.
	PK string
	// FeedKey is the serviceability Feed pubkey, which dz_feeds_current.pk
	// joins on for a code and name.
	FeedKey string
	Year    uint16
	Month   uint8
	// CollectedUSDC is collected_usdc_amount in USDC base units.
	CollectedUSDC uint64
}

// FeedDistributionSchema defines the schema for feed distributions.
//
// PayloadColumns order must match the physical column order of
// stg_dim_dz_shred_feed_distributions_snapshot: the staging insert is
// positional. store_test.go is the guard.
type FeedDistributionSchema struct{}

func (s *FeedDistributionSchema) Name() string { return "dz_shred_feed_distributions" }

func (s *FeedDistributionSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *FeedDistributionSchema) PayloadColumns() []string {
	return []string{
		"feed_key:VARCHAR",
		"year:INTEGER",
		"month:INTEGER",
		"collected_usdc:BIGINT",
	}
}

func (s *FeedDistributionSchema) ToRow(r FeedDistributionRow) []any {
	return []any{
		r.PK,
		r.FeedKey,
		r.Year,
		r.Month,
		r.CollectedUSDC,
	}
}

func (s *FeedDistributionSchema) GetPrimaryKey(r FeedDistributionRow) string {
	return r.PK
}

var feedDistributionSchema = &FeedDistributionSchema{}

func NewFeedDistributionDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, feedDistributionSchema)
}
