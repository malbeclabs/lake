package dzshreds

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/jonboulle/clockwork"
	shreds "github.com/malbeclabs/doublezero/sdk/shreds/go"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/dz/shreds/escrowevents"
	"github.com/malbeclabs/lake/indexer/pkg/dz/shreds/validatorrewards"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	"github.com/malbeclabs/lake/indexer/pkg/metrics"
)

// Row types for ClickHouse dimension tables.

type ExecutionControllerRow struct {
	PK                        string
	Phase                     string
	CurrentSubscriptionEpoch  uint64
	TotalMetros               uint16
	TotalEnabledDevices       uint16
	TotalClientSeats          uint32
	UpdatedDevicePricesCount  uint16
	SettledDevicesCount       uint16
	SettledClientSeatsCount   uint16
	LastSettledSlot           uint64
	LastUpdatingPricesSlot    uint64
	LastOpenForRequestsSlot   uint64
	LastClosedForRequestsSlot uint64
	NextSeatFundingIndex      uint64
}

type ClientSeatRow struct {
	PK                       string
	DeviceKey                string
	ClientIP                 string
	TenureEpochs             uint16
	FundedEpoch              uint64
	ActiveEpoch              uint64
	HasPriceOverride         bool
	OverrideUSDCPriceDollars uint16
	EscrowCount              uint32
	FundingAuthorityKey      string
	SubscriptionStartSlot    uint64
	LastUSDCPriceDollars     uint16
}

type PaymentEscrowRow struct {
	PK                   string
	ClientSeatKey        string
	WithdrawAuthorityKey string
	USDCBalance          uint64
}

type MetroHistoryRow struct {
	PK                      string
	ExchangeKey             string
	IsCurrentPriceFinalized bool
	TotalInitializedDevices uint16
	CurrentEpoch            uint64
	CurrentUSDCPriceDollars uint16
}

type DeviceHistoryRow struct {
	PK                             string
	DeviceKey                      string
	IsEnabled                      bool
	HasSettledSeats                bool
	MetroExchangeKey               string
	ActiveGrantedSeats             uint16
	ActiveTotalAvailableSeats      uint16
	CurrentEpoch                   uint64
	CurrentRequestedSeatCount      uint16
	CurrentGrantedSeatCount        uint16
	CurrentTotalAvailableSeats     uint16
	CurrentUSDCMetroPremiumDollars int16
}

type ValidatorClientRewardsRow struct {
	PK               string
	ClientID         uint16
	ManagerKey       string
	ShortDescription string
}

type ShredDistributionRow struct {
	PK                                 string
	SubscriptionEpoch                  uint64
	AssociatedDZEpoch                  uint64
	DeviceCount                        uint16
	ClientSeatCount                    uint16
	ValidatorRewardsProportion         uint16
	TotalPublishingValidators          uint32
	CollectedUSDCPayments              uint64
	Collected2ZConvertedFromUSDC       uint64
	DistributedValidatorRewardsCount   uint32
	DistributedContributorRewardsCount uint32
	DistributedValidator2ZAmount       uint64
	DistributedContributor2ZAmount     uint64
	Burned2ZAmount                     uint64
}

// DistributionClientProportionRow is one row of the per-epoch client-id reward
// proportion snapshot taken from ShredDistribution.validator_client_rewards_config.
type DistributionClientProportionRow struct {
	PK                string // {subscription_epoch}:{client_id}
	SubscriptionEpoch uint64
	ClientID          uint16
	Proportion        uint16 // UnitShare16, 0..10_000; zero means "use default"
	DefaultProportion uint16 // per-epoch fallback resolved at projection time
}

// ShredsRPC abstracts the shreds SDK client for singleton account fetches.
type ShredsRPC interface {
	FetchExecutionController(ctx context.Context) (*shreds.ExecutionController, error)
	FetchShredDistribution(ctx context.Context, subscriptionEpoch uint64) (*shreds.ShredDistribution, error)
}

// ShredsRawRPC provides low-level RPC access for batch-fetching all program accounts
// in a single call.
type ShredsRawRPC interface {
	GetProgramAccountsWithOpts(ctx context.Context, publicKey solana.PublicKey, opts *rpc.GetProgramAccountsOpts) (rpc.GetProgramAccountsResult, error)
}

type ViewConfig struct {
	Logger          *slog.Logger
	Clock           clockwork.Clock
	ShredsRPC       ShredsRPC
	ShredsRawRPC    ShredsRawRPC
	ProgramID       solana.PublicKey
	RefreshInterval time.Duration
	ClickHouse      clickhouse.Client
}

func (cfg *ViewConfig) Validate() error {
	if cfg.Logger == nil {
		return errors.New("logger is required")
	}
	if cfg.ShredsRPC == nil {
		return errors.New("shreds rpc is required")
	}
	if cfg.ShredsRawRPC == nil {
		return errors.New("shreds raw rpc is required")
	}
	if cfg.ProgramID.IsZero() {
		return errors.New("program id is required")
	}
	if cfg.ClickHouse == nil {
		return errors.New("clickhouse connection is required")
	}
	if cfg.RefreshInterval <= 0 {
		return errors.New("refresh interval must be greater than 0")
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

	readyOnce sync.Once
	readyCh   chan struct{}

	// cachedEscrowInfos holds the escrow list from the last successful refresh,
	// used by the escrow events view to know which accounts to fetch history for.
	cachedEscrowInfos []escrowevents.EscrowInfo

	// s3Client and leafStore handle fetching, verifying, and persisting the
	// per-epoch validator-rewards merkle leaves from the off-chain S3 export.
	s3Client  *validatorrewards.S3Client
	leafStore *validatorrewards.Store
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

	leafStore, err := validatorrewards.NewStore(validatorrewards.StoreConfig{
		Logger:     cfg.Logger,
		ClickHouse: cfg.ClickHouse,
	})
	if err != nil {
		return nil, fmt.Errorf("create validator rewards store: %w", err)
	}

	return &View{
		log:       cfg.Logger,
		cfg:       cfg,
		store:     store,
		readyCh:   make(chan struct{}),
		s3Client:  validatorrewards.NewS3Client(),
		leafStore: leafStore,
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
		return fmt.Errorf("context cancelled while waiting for shreds view: %w", ctx.Err())
	}
}

func (v *View) Start(ctx context.Context) {
	go func() {
		v.log.Info("shreds: starting refresh loop", "interval", v.cfg.RefreshInterval)

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
			v.log.Error("shreds: refresh panicked", "panic", r)
			metrics.ViewRefreshTotal.WithLabelValues("shreds", "panic").Inc()
		}
	}()

	if _, err := v.Refresh(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		v.log.Error("shreds: refresh failed", "error", err)
	}
}

func (v *View) Refresh(ctx context.Context) (ingestionlog.RefreshResult, error) {
	v.refreshMu.Lock()
	defer v.refreshMu.Unlock()

	var result ingestionlog.RefreshResult

	refreshStart := time.Now()
	v.log.Debug("shreds: refresh started")
	defer func() {
		duration := time.Since(refreshStart)
		v.log.Info("shreds: refresh completed", "duration", duration.String())
		metrics.ViewRefreshDuration.WithLabelValues("shreds").Observe(duration.Seconds())
	}()

	// Fetch execution controller (singleton).
	ec, err := v.cfg.ShredsRPC.FetchExecutionController(ctx)
	if err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("shreds", "error").Inc()
		return result, fmt.Errorf("fetch execution controller: %w", err)
	}

	// Fetch all program accounts in a single RPC call. ShredDistribution
	// accounts are demuxed alongside the other dim types, so we get every
	// epoch's distribution in one shot rather than fetching only the current
	// epoch — important because past epochs' merkle roots land asynchronously
	// (after the oracle posts PostValidatorRewardsData) and we'd otherwise
	// miss the window where a past epoch is finalized.
	var allAccounts *AllProgramAccounts
	if a, err := FetchAllProgramAccounts(ctx, v.cfg.ShredsRawRPC, v.cfg.ProgramID); err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("shreds", "error").Inc()
		return result, fmt.Errorf("fetch shreds accounts: %w", err)
	} else {
		allAccounts = a
	}

	clientSeats := allAccounts.ClientSeats
	paymentEscrows := allAccounts.PaymentEscrows
	metroHistories := allAccounts.MetroHistories
	deviceHistories := allAccounts.DeviceHistories
	validatorRewards := allAccounts.ValidatorRewards

	// Convert every on-chain ShredDistribution into both the dim row and the
	// per-epoch client-proportions snapshot. Sort by subscription_epoch
	// ascending so log lines and downstream iteration are deterministic.
	distributions := make([]ShredDistributionRow, 0, len(allAccounts.ShredDistributions))
	clientProportions := make([]DistributionClientProportionRow, 0)
	for _, kd := range allAccounts.ShredDistributions {
		d := kd.ShredDistribution
		distributions = append(distributions, convertShredDistribution(&d))
		clientProportions = append(clientProportions, convertDistributionClientProportions(&d)...)
	}

	v.log.Debug("shreds: fetched program data",
		"client_seats", len(clientSeats),
		"payment_escrows", len(paymentEscrows),
		"metro_histories", len(metroHistories),
		"device_histories", len(deviceHistories),
		"validator_rewards", len(validatorRewards),
		"distributions", len(distributions),
		"journals", len(allAccounts.Journals),
	)

	// Validate that we received data — empty responses would tombstone all existing entities.
	if len(metroHistories) == 0 {
		metrics.ViewRefreshTotal.WithLabelValues("shreds", "error").Inc()
		return result, fmt.Errorf("refusing to write snapshot: RPC returned no metro histories (possible RPC issue)")
	}
	if len(deviceHistories) == 0 {
		metrics.ViewRefreshTotal.WithLabelValues("shreds", "error").Inc()
		return result, fmt.Errorf("refusing to write snapshot: RPC returned no device histories (possible RPC issue)")
	}

	// Convert and write each entity type.
	ecRows := []ExecutionControllerRow{convertExecutionController(ec)}
	if err := v.store.ReplaceExecutionControllers(ctx, ecRows); err != nil {
		return result, fmt.Errorf("failed to replace execution controllers: %w", err)
	}

	csRows := convertClientSeats(clientSeats)
	if err := v.store.ReplaceClientSeats(ctx, csRows); err != nil {
		return result, fmt.Errorf("failed to replace client seats: %w", err)
	}

	peRows := convertPaymentEscrows(paymentEscrows)
	if err := v.store.ReplacePaymentEscrows(ctx, peRows); err != nil {
		return result, fmt.Errorf("failed to replace payment escrows: %w", err)
	}

	// Cache escrow info for the escrow events view.
	escrowInfos := make([]escrowevents.EscrowInfo, len(paymentEscrows))
	for i, e := range paymentEscrows {
		escrowInfos[i] = escrowevents.EscrowInfo{
			EscrowPK:     e.Pubkey.String(),
			ClientSeatPK: e.ClientSeatKey.String(),
		}
	}
	v.cachedEscrowInfos = escrowInfos

	mhRows := convertMetroHistories(metroHistories)
	if err := v.store.ReplaceMetroHistories(ctx, mhRows); err != nil {
		return result, fmt.Errorf("failed to replace metro histories: %w", err)
	}

	dhRows := convertDeviceHistories(deviceHistories)
	if err := v.store.ReplaceDeviceHistories(ctx, dhRows); err != nil {
		return result, fmt.Errorf("failed to replace device histories: %w", err)
	}

	vrRows := convertValidatorClientRewards(validatorRewards)
	if err := v.store.ReplaceValidatorClientRewards(ctx, vrRows); err != nil {
		return result, fmt.Errorf("failed to replace validator client rewards: %w", err)
	}

	if len(distributions) > 0 {
		if err := v.store.ReplaceShredDistributions(ctx, distributions); err != nil {
			return result, fmt.Errorf("failed to replace shred distributions: %w", err)
		}
	}

	if len(clientProportions) > 0 {
		if err := v.store.ReplaceDistributionClientProportions(ctx, clientProportions); err != nil {
			return result, fmt.Errorf("failed to replace distribution client proportions: %w", err)
		}
	}

	// Fetch and persist the per-epoch validator-rewards merkle leaves from the
	// off-chain S3 export.
	//
	// Two paths run per distribution:
	//   - If `ValidatorRewardsMerkleRoot` is non-zero (the oracle has posted
	//     the on-chain root): fetch, verify, persist as verified. Replaces
	//     any previously-indexed unverified rows for the epoch.
	//   - If the root is still zero (the chain hasn't caught up yet): fetch
	//     the S3 export without verification and persist with
	//     IsVerified=false. Lets the rewards page surface publishing
	//     validators with their leader slots immediately; the verified pass
	//     overwrites these rows once the root lands.
	//
	// The S3 export is keyed by Solana epoch. The shred-subscription program's
	// `subscription_epoch` counter equals the Solana epoch (the program
	// creates one distribution per Solana epoch starting from its launch
	// epoch). `associated_dz_epoch` is the parent revenue-distribution
	// program's epoch counter — a slower, independent counter — and must NOT
	// be used as the Solana epoch.
	var leafCount int
	var zeroRoot [32]byte
	for _, kd := range allAccounts.ShredDistributions {
		d := kd.ShredDistribution
		expectedRoot := d.ValidatorRewardsMerkleRoot
		solanaEpoch := d.SubscriptionEpoch

		status, err := v.leafStore.LeavesStatusForEpoch(ctx, solanaEpoch)
		if err != nil {
			v.log.Warn("shreds: leaves status check failed",
				"solana_epoch", solanaEpoch, "error", err)
			continue
		}

		if expectedRoot == zeroRoot {
			// Chain hasn't posted a root yet. Index unverified once; subsequent
			// refreshes skip until the root lands.
			if status.HasVerified || status.HasUnverified {
				continue
			}
			leaves, ok, fErr := validatorrewards.FetchForEpoch(ctx, v.s3Client, solanaEpoch)
			switch {
			case fErr != nil:
				v.log.Warn("shreds: leaf fetch failed",
					"solana_epoch", solanaEpoch, "error", fErr)
			case ok:
				if err := v.leafStore.ReplaceLeaves(ctx, d.SubscriptionEpoch, uint64(d.AssociatedDZEpoch), leaves); err != nil {
					return result, fmt.Errorf("failed to replace unverified leaves: %w", err)
				}
				leafCount += int(leaves.TotalPublishingValidators)
			default:
				// 404 — S3 export not yet published for this epoch.
				v.log.Debug("shreds: leaf export not yet available",
					"solana_epoch", solanaEpoch, "verified", false)
			}
			continue
		}

		// Root is posted. Skip if we already have verified leaves (root is
		// immutable). Otherwise fetch+verify and persist; this overwrites
		// any prior unverified entries because PK is the same per-leaf.
		if status.HasVerified {
			continue
		}
		verified, ok, vErr := validatorrewards.FetchAndVerifyForEpoch(ctx, v.s3Client, solanaEpoch, expectedRoot)
		switch {
		case vErr != nil:
			v.log.Warn("shreds: leaf verify failed",
				"solana_epoch", solanaEpoch,
				"associated_dz_epoch", d.AssociatedDZEpoch,
				"error", vErr)
		case ok:
			if err := v.leafStore.ReplaceLeaves(ctx, d.SubscriptionEpoch, uint64(d.AssociatedDZEpoch), verified); err != nil {
				return result, fmt.Errorf("failed to replace validator rewards leaves: %w", err)
			}
			leafCount += int(verified.TotalPublishingValidators)
		default:
			// 404 — S3 export not yet published for this epoch. Will retry next refresh.
			v.log.Debug("shreds: leaf export not yet available",
				"solana_epoch", solanaEpoch, "verified", true)
		}
	}

	// Project per-leaf claimable bits from in-flight ShredDistributionJournal
	// accounts to the leaf_distribution_status table. We restrict to the most
	// recent subscription epochs so we don't scan the entire history every
	// refresh.
	//
	// Only project journal-status rows for the last 12 subscription epochs
	// (10 visible on the page + 2 in-flight buffer). Older epochs are out of
	// the "immediately claimable" window per the spec and don't need bitmap
	// tracking.
	const recentJournalWindow uint64 = 12
	var statusRows []validatorrewards.LeafDistributionStatusRow
	if len(allAccounts.Journals) > 0 {
		var minEpoch uint64
		if ec.CurrentSubscriptionEpoch > recentJournalWindow {
			minEpoch = ec.CurrentSubscriptionEpoch - recentJournalWindow
		}
		// Cache leaf-index → node-id maps per subscription_epoch so we only
		// hit ClickHouse once per epoch when multiple journals share it.
		leafMaps := make(map[uint64]map[uint32]string)
		for _, kj := range allAccounts.Journals {
			view := kj.View
			if view == nil {
				continue
			}
			// Only the 2Z (DoubleZero) mint journal carries the publisher
			// accumulation bitmap relevant to the rewards page.
			if view.MintKey.String() != validatorrewards.DoubleZeroMintKey {
				continue
			}
			if view.SubscriptionEpoch < minEpoch {
				continue
			}
			leafMap, ok := leafMaps[view.SubscriptionEpoch]
			if !ok {
				m, err := v.leafStore.LeafIndexToNodeID(ctx, view.SubscriptionEpoch)
				if err != nil {
					return result, fmt.Errorf("load leaf_index → node_id mapping for epoch %d: %w",
						view.SubscriptionEpoch, err)
				}
				leafMaps[view.SubscriptionEpoch] = m
				leafMap = m
			}
			if len(leafMap) == 0 {
				// Leaves not yet indexed for this epoch (S3 export pending,
				// or out of the leaf window). Skip — next refresh will retry.
				continue
			}
			statusRows = append(statusRows,
				validatorrewards.ProjectStatuses(view, view.SubscriptionEpoch, leafMap)...)
		}
		if len(statusRows) > 0 {
			if err := v.leafStore.ReplaceLeafDistributionStatuses(ctx, statusRows); err != nil {
				return result, fmt.Errorf("failed to replace leaf distribution statuses: %w", err)
			}
		}
	}

	totalRows := len(ecRows) + len(csRows) + len(peRows) + len(mhRows) + len(dhRows) + len(vrRows) + len(distributions) + len(clientProportions) + leafCount + len(statusRows)
	result.RowsAffected = int64(totalRows)
	fetchedAt := time.Now().UTC()
	result.SourceMaxEventTS = &fetchedAt

	v.readyOnce.Do(func() {
		close(v.readyCh)
		v.log.Info("shreds: view is now ready")
	})

	metrics.ViewRefreshTotal.WithLabelValues("shreds", "success").Inc()
	return result, nil
}

// ipFromBits converts a uint32 IP representation to a dotted-decimal string.
func ipFromBits(bits uint32) string {
	ip := net.IPv4(byte(bits>>24), byte(bits>>16), byte(bits>>8), byte(bits))
	return ip.String()
}

func convertExecutionController(ec *shreds.ExecutionController) ExecutionControllerRow {
	return ExecutionControllerRow{
		PK:                        "singleton",
		Phase:                     ec.GetPhase().String(),
		CurrentSubscriptionEpoch:  ec.CurrentSubscriptionEpoch,
		TotalMetros:               ec.TotalMetros,
		TotalEnabledDevices:       ec.TotalEnabledDevices,
		TotalClientSeats:          ec.TotalClientSeats,
		UpdatedDevicePricesCount:  ec.UpdatedDevicePricesCount,
		SettledDevicesCount:       ec.SettledDevicesCount,
		SettledClientSeatsCount:   ec.SettledClientSeatsCount,
		LastSettledSlot:           ec.LastSettledSlot,
		LastUpdatingPricesSlot:    ec.LastUpdatingPricesSlot,
		LastOpenForRequestsSlot:   ec.LastOpenForRequestsSlot,
		LastClosedForRequestsSlot: ec.LastClosedForRequestsSlot,
		NextSeatFundingIndex:      ec.NextSeatFundingIndex,
	}
}

func convertClientSeats(seats []shreds.KeyedClientSeat) []ClientSeatRow {
	rows := make([]ClientSeatRow, len(seats))
	for i, s := range seats {
		rows[i] = ClientSeatRow{
			PK:                       s.Pubkey.String(),
			DeviceKey:                s.DeviceKey.String(),
			ClientIP:                 ipFromBits(s.ClientIPBits),
			TenureEpochs:             s.TenureEpochs,
			FundedEpoch:              s.FundedEpoch,
			ActiveEpoch:              s.ActiveEpoch,
			HasPriceOverride:         s.HasPriceOverride(),
			OverrideUSDCPriceDollars: s.OverrideUSDCPriceDollars,
			EscrowCount:              s.EscrowCount,
			FundingAuthorityKey:      s.FundingAuthorityKey.String(),
			SubscriptionStartSlot:    s.SubscriptionStartSlot,
			LastUSDCPriceDollars:     s.LastUSDCPriceDollars,
		}
	}
	return rows
}

func convertPaymentEscrows(escrows []shreds.KeyedPaymentEscrow) []PaymentEscrowRow {
	rows := make([]PaymentEscrowRow, len(escrows))
	for i, e := range escrows {
		rows[i] = PaymentEscrowRow{
			PK:                   e.Pubkey.String(),
			ClientSeatKey:        e.ClientSeatKey.String(),
			WithdrawAuthorityKey: e.WithdrawAuthorityKey.String(),
			USDCBalance:          e.USDCBalance,
		}
	}
	return rows
}

func convertMetroHistories(metros []shreds.KeyedMetroHistory) []MetroHistoryRow {
	rows := make([]MetroHistoryRow, len(metros))
	for i, m := range metros {
		var currentEpoch uint64
		var currentPrice uint16
		if m.Prices.TotalCount > 0 {
			entry := m.Prices.Entries[m.Prices.CurrentIndex]
			currentEpoch = entry.Epoch
			currentPrice = entry.Price.USDCPriceDollars
		}
		rows[i] = MetroHistoryRow{
			PK:                      m.Pubkey.String(),
			ExchangeKey:             m.ExchangeKey.String(),
			IsCurrentPriceFinalized: m.IsCurrentPriceFinalized(),
			TotalInitializedDevices: m.TotalInitializedDevices,
			CurrentEpoch:            currentEpoch,
			CurrentUSDCPriceDollars: currentPrice,
		}
	}
	return rows
}

func convertDeviceHistories(devices []shreds.KeyedDeviceHistory) []DeviceHistoryRow {
	rows := make([]DeviceHistoryRow, len(devices))
	for i, d := range devices {
		var currentEpoch uint64
		var reqSeats, grantedSeats, totalSeats uint16
		var metroPremium int16
		if d.Subscriptions.TotalCount > 0 {
			entry := d.Subscriptions.Entries[d.Subscriptions.CurrentIndex]
			currentEpoch = entry.Epoch
			reqSeats = entry.Subscription.RequestedSeatCount
			grantedSeats = entry.Subscription.GrantedSeatCount
			totalSeats = entry.Subscription.TotalAvailableSeats
			metroPremium = entry.Subscription.USDCMetroPremiumDollars
		}
		rows[i] = DeviceHistoryRow{
			PK:                             d.Pubkey.String(),
			DeviceKey:                      d.DeviceKey.String(),
			IsEnabled:                      d.IsEnabled(),
			HasSettledSeats:                d.HasSettledSeats(),
			MetroExchangeKey:               d.MetroExchangeKey.String(),
			ActiveGrantedSeats:             d.ActiveGrantedSeats,
			ActiveTotalAvailableSeats:      d.ActiveTotalAvailableSeats,
			CurrentEpoch:                   currentEpoch,
			CurrentRequestedSeatCount:      reqSeats,
			CurrentGrantedSeatCount:        grantedSeats,
			CurrentTotalAvailableSeats:     totalSeats,
			CurrentUSDCMetroPremiumDollars: metroPremium,
		}
	}
	return rows
}

func convertValidatorClientRewards(rewards []shreds.KeyedValidatorClientRewards) []ValidatorClientRewardsRow {
	rows := make([]ValidatorClientRewardsRow, len(rewards))
	for i, r := range rewards {
		rows[i] = ValidatorClientRewardsRow{
			PK:               r.Pubkey.String(),
			ClientID:         r.ClientID,
			ManagerKey:       r.ManagerKey.String(),
			ShortDescription: r.ShortDescription(),
		}
	}
	return rows
}

func convertShredDistribution(d *shreds.ShredDistribution) ShredDistributionRow {
	return ShredDistributionRow{
		PK:                                 fmt.Sprintf("epoch-%d", d.SubscriptionEpoch),
		SubscriptionEpoch:                  d.SubscriptionEpoch,
		AssociatedDZEpoch:                  d.AssociatedDZEpoch,
		DeviceCount:                        d.DeviceCount,
		ClientSeatCount:                    d.ClientSeatCount,
		ValidatorRewardsProportion:         d.ValidatorRewardsProportion,
		TotalPublishingValidators:          d.TotalPublishingValidators,
		CollectedUSDCPayments:              d.CollectedUSDCPayments,
		Collected2ZConvertedFromUSDC:       d.Collected2ZConvertedFromUSDC,
		DistributedValidatorRewardsCount:   d.DistributedValidatorRewardsCount,
		DistributedContributorRewardsCount: d.DistributedContributorRewardsCount,
		DistributedValidator2ZAmount:       d.DistributedValidator2ZAmount,
		DistributedContributor2ZAmount:     d.DistributedContributor2ZAmount,
		Burned2ZAmount:                     d.Burned2ZAmount,
	}
}

// legacyDefaultClientProportion mirrors the on-chain constant
// LEGACY_DEFAULT_PROPORTION (35%). See
// programs/shred-subscription/src/types/validator_client_rewards_proportion.rs:69-77.
const legacyDefaultClientProportion uint16 = 3_500

// convertDistributionClientProportions projects the per-epoch snapshot of
// the validator-client rewards proportions from a ShredDistribution into one
// row per (subscription_epoch, client_id). Zero-padded slots in the on-chain
// fixed-size array are skipped (client_id=0 is reserved on-chain).
//
// NOTE: The on-chain ShredDistribution now snapshots a full
// ValidatorClientRewardsConfig (proportions + default_proportion), but the
// generated Go SDK binding still exposes only the proportions array as
// ShredDistribution.ValidatorClientRewardProportions (no DefaultProportion
// field). Until the SDK is regenerated, we apply legacyDefaultClientProportion
// as the per-epoch default. When the SDK catches up, replace the fallback
// with the snapshot's default_proportion (zero -> legacy fallback).
func convertDistributionClientProportions(d *shreds.ShredDistribution) []DistributionClientProportionRow {
	if d == nil {
		return nil
	}
	defaultProp := legacyDefaultClientProportion
	props := d.ValidatorClientRewardProportions
	rows := make([]DistributionClientProportionRow, 0, len(props))
	for _, p := range props {
		// Skip zero-padded entries (the on-chain array is fixed-size and
		// client_id=0 is reserved per the on-chain non-zero constraint).
		if p.ID == 0 && p.RewardProportion == 0 {
			continue
		}
		rows = append(rows, DistributionClientProportionRow{
			PK:                fmt.Sprintf("epoch-%d:client-%d", d.SubscriptionEpoch, p.ID),
			SubscriptionEpoch: d.SubscriptionEpoch,
			ClientID:          p.ID,
			Proportion:        p.RewardProportion,
			DefaultProportion: defaultProp,
		})
	}
	return rows
}

// PaymentEscrowInfos returns the escrow list from the last successful refresh.
// Used by the escrow events view to know which accounts to fetch history for.
func (v *View) PaymentEscrowInfos() []escrowevents.EscrowInfo {
	v.refreshMu.Lock()
	defer v.refreshMu.Unlock()
	return v.cachedEscrowInfos
}

// Compile-time checks.
var _ ShredsRPC = (*shreds.Client)(nil)
