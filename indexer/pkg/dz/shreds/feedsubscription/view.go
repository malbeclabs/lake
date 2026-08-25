package feedsubscription

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	"github.com/malbeclabs/lake/indexer/pkg/metrics"
	"github.com/malbeclabs/lake/utils/pkg/logger"
)

// RawRPC is the one RPC call this view needs.
type RawRPC interface {
	GetProgramAccountsWithOpts(ctx context.Context, publicKey solana.PublicKey, opts *rpc.GetProgramAccountsOpts) (rpc.GetProgramAccountsResult, error)
}

type ViewConfig struct {
	Logger     *slog.Logger
	RPC        RawRPC
	ProgramID  solana.PublicKey
	ClickHouse clickhouse.Client
}

func (cfg *ViewConfig) Validate() error {
	if cfg.Logger == nil {
		return errors.New("logger is required")
	}
	if cfg.RPC == nil {
		return errors.New("rpc is required")
	}
	if cfg.ProgramID.IsZero() {
		return errors.New("program id is required")
	}
	if cfg.ClickHouse == nil {
		return errors.New("clickhouse connection is required")
	}
	return nil
}

type View struct {
	log       *slog.Logger
	cfg       ViewConfig
	store     *Store
	refreshMu sync.Mutex

	// esc escalates consecutive refresh failures from WARN to ERROR so a single
	// RPC blip doesn't page on-call (see logger.Escalator).
	esc logger.Escalator
}

func NewView(cfg ViewConfig) (*View, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	store, err := NewStore(StoreConfig{
		Logger:     cfg.Logger,
		ClickHouse: cfg.ClickHouse,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}
	return &View{log: cfg.Logger, cfg: cfg, store: store}, nil
}

// Refresh reads every FeedDistribution account and replaces the snapshot.
//
// The caller is Activities.RefreshShreds, not a self-owned ticker: production
// drives ingestion through DZIngestWorkflow, and adding a workflow activity
// would be replay-breaking against the previous deploy's run.
func (v *View) Refresh(ctx context.Context) (ingestionlog.RefreshResult, error) {
	v.refreshMu.Lock()
	defer v.refreshMu.Unlock()

	var result ingestionlog.RefreshResult

	refreshStart := time.Now()
	defer func() {
		duration := time.Since(refreshStart)
		v.log.Info("shreds/feed-subscription: refresh completed", "duration", duration.String())
		metrics.ViewRefreshDuration.WithLabelValues("shreds_feed_subscription").Observe(duration.Seconds())
	}()

	rows, err := v.fetchFeedDistributions(ctx)
	if err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("shreds_feed_subscription", "error").Inc()
		return result, err
	}

	// An empty fetch writes nothing, by design: ReplaceFeedDistributions treats an
	// empty batch as a no-op so a bad answer cannot tombstone real revenue. That
	// makes the good case and the bad case look identical from the outside — a
	// cluster without the program, and a mainnet RPC that returned nothing while
	// the table holds rows. Only the second is a problem, and it is the one that
	// would otherwise pass in complete silence while the page served stale totals.
	//
	// WARN, not an escalating failure: a single empty answer is most likely an RPC
	// blip, and the page is still serving the last good totals. Sustained
	// staleness is what an ingest-staleness alert is for, not this line.
	if len(rows) == 0 {
		if existing, countErr := v.store.CountFeedDistributions(ctx); countErr != nil {
			v.log.Warn("shreds/feed-subscription: fetched no feed distributions and could not check for existing rows",
				"error", countErr)
		} else if existing > 0 {
			v.log.Warn("shreds/feed-subscription: fetched no feed distributions while the table holds rows, keeping the existing totals",
				"existing_rows", existing)
		}
	}

	if err := v.store.ReplaceFeedDistributions(ctx, rows); err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("shreds_feed_subscription", "error").Inc()
		return result, fmt.Errorf("failed to replace feed distributions: %w", err)
	}

	fetchedAt := refreshStart.UTC()
	result.RowsAffected = int64(len(rows))
	result.SourceMaxEventTS = &fetchedAt

	v.log.Debug("shreds/feed-subscription: wrote feed distributions", "count", len(rows))
	metrics.ViewRefreshTotal.WithLabelValues("shreds_feed_subscription", "success").Inc()
	return result, nil
}

// fetchFeedDistributions makes one getProgramAccounts call and keeps only the
// FeedDistribution accounts.
//
// The program also owns a ProgramConfig account, so the split is by
// discriminator rather than by data size. An account that carries the
// FeedDistribution discriminator but does not decode is an error, not a skip:
// that discriminator match with a failed decode means the on-chain layout
// moved (for example a new ::v3 struct), and the refresh must stop loudly
// rather than write a partial snapshot that under-reports revenue.
//
// An empty list is a normal answer. Only Solana mainnet-beta has this program
// deployed, so every other cluster returns nothing.
//
// A non-empty list with no FeedDistribution in it is not a normal answer, and is
// an error for the same reason a failed decode is: it means the seed moved (a
// ::v3), and the alternative is silence. ReplaceFeedDistributions
// treats an empty batch as a no-op, so a snapshot that matched nothing would
// leave the table frozen at its last good totals, report success, and give the
// page no way to know it had stopped being current.
func (v *View) fetchFeedDistributions(ctx context.Context) ([]FeedDistributionRow, error) {
	accounts, err := v.cfg.RPC.GetProgramAccountsWithOpts(ctx, v.cfg.ProgramID, &rpc.GetProgramAccountsOpts{})
	if err != nil {
		return nil, fmt.Errorf("fetching feed-subscription program accounts: %w", err)
	}

	rows := make([]FeedDistributionRow, 0, len(accounts))
	for _, acct := range accounts {
		data := acct.Account.Data.GetBinary()
		if len(data) < 8 || [8]byte(data[:8]) != DiscriminatorFeedDistribution {
			continue
		}
		fd, err := DecodeFeedDistribution(data)
		if err != nil {
			return nil, fmt.Errorf("decoding feed distribution %s: %w", acct.Pubkey, err)
		}
		rows = append(rows, FeedDistributionRow{
			PK:            acct.Pubkey.String(),
			FeedKey:       fd.FeedKey.String(),
			Year:          fd.Year,
			Month:         fd.Month,
			CollectedUSDC: fd.CollectedUSDC,
		})
	}

	// The program always owns its ProgramConfig account, so an empty list means
	// the program is not on this cluster, while a non-empty list holding no
	// FeedDistribution means the discriminator no longer matches.
	if len(accounts) > 0 && len(rows) == 0 {
		return nil, fmt.Errorf("no feed distribution accounts among %d program accounts: discriminator %s no longer matches", len(accounts), hexDiscriminator())
	}
	return rows, nil
}

// Escalate records a refresh outcome so consecutive failures move from WARN to
// ERROR. Activities.RefreshShreds calls this instead of returning the error, so
// a feed-subscription failure never marks the shreds refresh itself failed.
func (v *View) Escalate(err error) {
	// A cancelled context is a pod shutdown or deploy, not a failure to page on.
	// Deliberately not filtering context.DeadlineExceeded here: a fetch that
	// keeps missing its deadline is a real ingest stall and should still page.
	if err != nil && errors.Is(err, context.Canceled) {
		return
	}
	v.esc.Observe(v.log, "refresh", "shreds/feed-subscription: refresh failed", err)
}
