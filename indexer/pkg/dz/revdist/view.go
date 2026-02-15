package dzrevdist

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/jonboulle/clockwork"
	revdist "github.com/malbeclabs/doublezero/sdk/revdist/go"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/metrics"
)

// RevDistClient wraps the revdist SDK client methods used by the view.
type RevDistClient interface {
	FetchConfig(ctx context.Context) (*revdist.ProgramConfig, error)
	FetchDistribution(ctx context.Context, epoch uint64) (*revdist.Distribution, error)
	FetchJournal(ctx context.Context) (*revdist.Journal, error)
	FetchAllValidatorDeposits(ctx context.Context) ([]revdist.SolanaValidatorDeposit, error)
	FetchAllContributorRewards(ctx context.Context) ([]revdist.ContributorRewards, error)
	FetchValidatorDebts(ctx context.Context, epoch uint64) (*revdist.ComputedSolanaValidatorDebts, error)
	FetchRewardShares(ctx context.Context, epoch uint64) (*revdist.ShapleyOutputStorage, error)
}

// PriceOracle fetches current SOL/2Z prices.
type PriceOracle interface {
	FetchSwapRate(ctx context.Context) (*revdist.SwapRate, error)
}

type ViewConfig struct {
	Logger          *slog.Logger
	Clock           clockwork.Clock
	RevDistClient   RevDistClient
	PriceOracle     PriceOracle // optional — if nil, price snapshots are skipped
	RefreshInterval time.Duration
	ClickHouse      clickhouse.Client
	ProgramID       solana.PublicKey
}

func (cfg *ViewConfig) Validate() error {
	if cfg.Logger == nil {
		return errors.New("logger is required")
	}
	if cfg.RevDistClient == nil {
		return errors.New("revdist client is required")
	}
	if cfg.ClickHouse == nil {
		return errors.New("clickhouse connection is required")
	}
	if cfg.RefreshInterval <= 0 {
		return errors.New("refresh interval must be greater than 0")
	}
	if cfg.ProgramID.IsZero() {
		return errors.New("program id is required")
	}
	if cfg.Clock == nil {
		cfg.Clock = clockwork.NewRealClock()
	}
	return nil
}

type View struct {
	log       *slog.Logger
	cfg       ViewConfig
	store     *Store
	refreshMu sync.Mutex

	lastFetchedEpoch uint64
	readyOnce        sync.Once
	readyCh          chan struct{}
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

	return &View{
		log:     cfg.Logger,
		cfg:     cfg,
		store:   store,
		readyCh: make(chan struct{}),
	}, nil
}

func (v *View) Ready() bool {
	select {
	case <-v.readyCh:
		return true
	default:
		return false
	}
}

func (v *View) WaitReady(ctx context.Context) error {
	select {
	case <-v.readyCh:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while waiting for revdist view: %w", ctx.Err())
	}
}

func (v *View) Start(ctx context.Context) {
	go func() {
		v.log.Info("revdist: starting refresh loop", "interval", v.cfg.RefreshInterval)

		v.safeRefresh(ctx)

		ticker := v.cfg.Clock.NewTicker(v.cfg.RefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.Chan():
				v.safeRefresh(ctx)
			}
		}
	}()
}

func (v *View) safeRefresh(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			v.log.Error("revdist: refresh panicked", "panic", r)
			metrics.ViewRefreshTotal.WithLabelValues("revdist", "panic").Inc()
		}
	}()

	if err := v.Refresh(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		v.log.Error("revdist: refresh failed", "error", err)
	}
}

func (v *View) Refresh(ctx context.Context) error {
	v.refreshMu.Lock()
	defer v.refreshMu.Unlock()

	refreshStart := time.Now()
	v.log.Debug("revdist: refresh started")
	defer func() {
		duration := time.Since(refreshStart)
		v.log.Info("revdist: refresh completed", "duration", duration.String())
		metrics.ViewRefreshDuration.WithLabelValues("revdist").Observe(duration.Seconds())
	}()

	// 1. Fetch program config to get NextCompletedDZEpoch
	programConfig, err := v.cfg.RevDistClient.FetchConfig(ctx)
	if err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("revdist", "error").Inc()
		return fmt.Errorf("failed to fetch config: %w", err)
	}

	programID := v.cfg.ProgramID.String()
	config := convertConfig(programID, programConfig)
	if err := v.store.ReplaceConfig(ctx, []Config{config}); err != nil {
		return fmt.Errorf("failed to replace config: %w", err)
	}

	// 2. Fetch current prices from oracle (optional, best-effort)
	var prices *PriceSnapshot
	if v.cfg.PriceOracle != nil {
		swapRate, err := v.cfg.PriceOracle.FetchSwapRate(ctx)
		if err != nil {
			v.log.Warn("revdist: failed to fetch prices from oracle", "error", err)
		} else {
			prices = convertPriceSnapshot(swapRate)

			// Store periodic price snapshot
			if err := v.store.InsertPriceSnapshot(ctx, *prices); err != nil {
				v.log.Warn("revdist: failed to insert price snapshot", "error", err)
			}
		}
	}

	// 3. Fetch newly completed epochs (distributions + off-chain data)
	nextCompleted := programConfig.NextCompletedDZEpoch
	if nextCompleted > 0 {
		for epoch := v.lastFetchedEpoch + 1; epoch < nextCompleted; epoch++ {
			if epoch == 0 {
				// Skip epoch 0 when lastFetchedEpoch is 0 (first run) — start from epoch 1
				continue
			}

			v.log.Debug("revdist: fetching epoch", "epoch", epoch)

			// Fetch on-chain distribution
			dist, err := v.cfg.RevDistClient.FetchDistribution(ctx, epoch)
			if err != nil {
				v.log.Warn("revdist: failed to fetch distribution", "epoch", epoch, "error", err)
				break
			}
			d := convertDistribution(dist)
			// Attach current prices to the distribution for historical USD context
			if prices != nil {
				d.SOLPriceUSD = prices.SOLPriceUSD
				d.TwoZPriceUSD = prices.TwoZPriceUSD
			}
			if err := v.store.ReplaceDistributions(ctx, []Distribution{d}); err != nil {
				return fmt.Errorf("failed to replace distribution for epoch %d: %w", epoch, err)
			}

			// Fetch off-chain validator debts
			debts, err := v.cfg.RevDistClient.FetchValidatorDebts(ctx, epoch)
			if err != nil {
				v.log.Warn("revdist: failed to fetch validator debts", "epoch", epoch, "error", err)
			} else {
				validatorDebts := convertValidatorDebts(epoch, debts)
				if err := v.store.InsertValidatorDebts(ctx, validatorDebts); err != nil {
					return fmt.Errorf("failed to insert validator debts for epoch %d: %w", epoch, err)
				}
			}

			// Fetch off-chain reward shares
			shares, err := v.cfg.RevDistClient.FetchRewardShares(ctx, epoch)
			if err != nil {
				v.log.Warn("revdist: failed to fetch reward shares", "epoch", epoch, "error", err)
			} else {
				rewardShares := convertRewardShares(epoch, shares)
				if err := v.store.InsertRewardShares(ctx, rewardShares); err != nil {
					return fmt.Errorf("failed to insert reward shares for epoch %d: %w", epoch, err)
				}
			}

			v.lastFetchedEpoch = epoch
		}
	}

	// 4. Fetch journal (singleton, always refreshed)
	journal, err := v.cfg.RevDistClient.FetchJournal(ctx)
	if err != nil {
		v.log.Warn("revdist: failed to fetch journal", "error", err)
	} else {
		if err := v.store.ReplaceJournal(ctx, []Journal{convertJournal(programID, journal)}); err != nil {
			return fmt.Errorf("failed to replace journal: %w", err)
		}
	}

	// 5. Fetch all validator deposits (snapshot, always refreshed)
	deposits, err := v.cfg.RevDistClient.FetchAllValidatorDeposits(ctx)
	if err != nil {
		v.log.Warn("revdist: failed to fetch validator deposits", "error", err)
	} else {
		if len(deposits) > 0 {
			if err := v.store.ReplaceValidatorDeposits(ctx, convertValidatorDeposits(deposits)); err != nil {
				return fmt.Errorf("failed to replace validator deposits: %w", err)
			}
		}
	}

	// 6. Fetch all contributor rewards (snapshot, always refreshed)
	rewards, err := v.cfg.RevDistClient.FetchAllContributorRewards(ctx)
	if err != nil {
		v.log.Warn("revdist: failed to fetch contributor rewards", "error", err)
	} else {
		if len(rewards) > 0 {
			if err := v.store.ReplaceContributorRewards(ctx, convertContributorRewards(rewards)); err != nil {
				return fmt.Errorf("failed to replace contributor rewards: %w", err)
			}
		}
	}

	v.readyOnce.Do(func() {
		close(v.readyCh)
		v.log.Info("revdist: view is now ready")
	})

	metrics.ViewRefreshTotal.WithLabelValues("revdist", "success").Inc()
	return nil
}

func convertConfig(programID string, pc *revdist.ProgramConfig) Config {
	return Config{
		ProgramID:                          programID,
		Flags:                              pc.Flags,
		NextCompletedEpoch:                 pc.NextCompletedDZEpoch,
		AdminKey:                           pc.AdminKey.String(),
		DebtAccountantKey:                  pc.DebtAccountantKey.String(),
		RewardsAccountantKey:               pc.RewardsAccountantKey.String(),
		ContributorManagerKey:              pc.ContributorManagerKey.String(),
		SOL2ZSwapProgramID:                 pc.SOL2ZSwapProgramID.String(),
		BurnRateLimit:                      pc.DistributionParameters.CommunityBurnRateParameters.Limit,
		BurnRateDZEpochsToIncreasing:       pc.DistributionParameters.CommunityBurnRateParameters.DZEpochsToIncreasing,
		BurnRateDZEpochsToLimit:            pc.DistributionParameters.CommunityBurnRateParameters.DZEpochsToLimit,
		BaseBlockRewardsPct:                pc.DistributionParameters.SolanaValidatorFeeParameters.BaseBlockRewardsPct,
		PriorityBlockRewardsPct:            pc.DistributionParameters.SolanaValidatorFeeParameters.PriorityBlockRewardsPct,
		InflationRewardsPct:                pc.DistributionParameters.SolanaValidatorFeeParameters.InflationRewardsPct,
		JitoTipsPct:                        pc.DistributionParameters.SolanaValidatorFeeParameters.JitoTipsPct,
		FixedSOLAmount:                     pc.DistributionParameters.SolanaValidatorFeeParameters.FixedSOLAmount,
		RelayPlaceholderLamports:           pc.RelayParameters.PlaceholderLamports,
		RelayDistributeRewardsLamports:     pc.RelayParameters.DistributeRewardsLamports,
		DebtWriteOffFeatureActivationEpoch: pc.DebtWriteOffFeatureActivationEpoch,
	}
}

func convertDistribution(d *revdist.Distribution) Distribution {
	return Distribution{
		DZEpoch:                          d.DZEpoch,
		Flags:                            d.Flags,
		CommunityBurnRate:                d.CommunityBurnRate,
		TotalSolanaValidators:            d.TotalSolanaValidators,
		SolanaValidatorPaymentsCount:     d.SolanaValidatorPaymentsCount,
		TotalSolanaValidatorDebt:         d.TotalSolanaValidatorDebt,
		CollectedSolanaValidatorPayments: d.CollectedSolanaValidatorPayments,
		TotalContributors:                d.TotalContributors,
		DistributedRewardsCount:          d.DistributedRewardsCount,
		CollectedPrepaid2ZPayments:       d.CollectedPrepaid2ZPayments,
		Collected2ZConvertedFromSOL:      d.Collected2ZConvertedFromSOL,
		UncollectibleSOLDebt:             d.UncollectibleSOLDebt,
		Distributed2ZAmount:              d.Distributed2ZAmount,
		Burned2ZAmount:                   d.Burned2ZAmount,
		SolanaValidatorWriteOffCount:     d.SolanaValidatorWriteOffCount,
		BaseBlockRewardsPct:              d.SolanaValidatorFeeParameters.BaseBlockRewardsPct,
		PriorityBlockRewardsPct:          d.SolanaValidatorFeeParameters.PriorityBlockRewardsPct,
		InflationRewardsPct:              d.SolanaValidatorFeeParameters.InflationRewardsPct,
		JitoTipsPct:                      d.SolanaValidatorFeeParameters.JitoTipsPct,
		FixedSOLAmount:                   d.SolanaValidatorFeeParameters.FixedSOLAmount,
	}
}

func convertJournal(programID string, j *revdist.Journal) Journal {
	return Journal{
		ProgramID:                programID,
		TotalSOLBalance:          j.TotalSOLBalance,
		Total2ZBalance:           j.Total2ZBalance,
		Swap2ZDestinationBalance: j.Swap2ZDestinationBalance,
		SwappedSOLAmount:         j.SwappedSOLAmount,
		NextDZEpochToSweepTokens: j.NextDZEpochToSweepTokens,
	}
}

func convertValidatorDeposits(deposits []revdist.SolanaValidatorDeposit) []ValidatorDeposit {
	result := make([]ValidatorDeposit, len(deposits))
	for i, d := range deposits {
		result[i] = ValidatorDeposit{
			NodeID:            d.NodeID.String(),
			WrittenOffSOLDebt: d.WrittenOffSOLDebt,
		}
	}
	return result
}

func convertContributorRewards(rewards []revdist.ContributorRewards) []ContributorReward {
	result := make([]ContributorReward, len(rewards))
	for i, r := range rewards {
		var shares []RecipientShare
		for _, s := range r.RecipientShares {
			if s.RecipientKey.IsZero() {
				continue
			}
			shares = append(shares, RecipientShare{
				RecipientKey: s.RecipientKey.String(),
				Share:        s.Share,
			})
		}
		result[i] = ContributorReward{
			ServiceKey:        r.ServiceKey.String(),
			RewardsManagerKey: r.RewardsManagerKey.String(),
			Flags:             r.Flags,
			RecipientShares:   shares,
		}
	}
	return result
}

func convertValidatorDebts(epoch uint64, debts *revdist.ComputedSolanaValidatorDebts) []ValidatorDebt {
	result := make([]ValidatorDebt, len(debts.Debts))
	for i, d := range debts.Debts {
		result[i] = ValidatorDebt{
			DZEpoch: epoch,
			NodeID:  d.NodeID.String(),
			Amount:  d.Amount,
		}
	}
	return result
}

func convertPriceSnapshot(sr *revdist.SwapRate) *PriceSnapshot {
	solPriceUSD, _ := strconv.ParseFloat(sr.SOLPriceUSD, 64)
	twoZPriceUSD, _ := strconv.ParseFloat(sr.TwoZPriceUSD, 64)
	return &PriceSnapshot{
		SOLPriceUSD:  solPriceUSD,
		TwoZPriceUSD: twoZPriceUSD,
		SwapRate:     sr.Rate,
	}
}

func convertRewardShares(epoch uint64, shares *revdist.ShapleyOutputStorage) []RewardShare {
	result := make([]RewardShare, len(shares.Rewards))
	for i, s := range shares.Rewards {
		// RemainingBytes encodes is_blocked (bit 31) and economic_burn_rate (bits 0-29)
		remaining := uint32(s.RemainingBytes[0]) |
			uint32(s.RemainingBytes[1])<<8 |
			uint32(s.RemainingBytes[2])<<16 |
			uint32(s.RemainingBytes[3])<<24
		isBlocked := (remaining >> 31) != 0
		economicBurnRate := remaining & 0x3FFFFFFF

		result[i] = RewardShare{
			DZEpoch:          epoch,
			ContributorKey:   s.ContributorKey.String(),
			UnitShare:        s.UnitShare,
			TotalUnitShares:  shares.TotalUnitShares,
			IsBlocked:        isBlocked,
			EconomicBurnRate: economicBurnRate,
		}
	}
	return result
}
