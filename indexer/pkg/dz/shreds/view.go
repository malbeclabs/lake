package dzshreds

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	shreds "github.com/malbeclabs/doublezero/sdk/shreds/go"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	"github.com/malbeclabs/lake/indexer/pkg/metrics"
	"golang.org/x/sync/errgroup"
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

// ShredsRPC abstracts the shreds SDK client for testing.
type ShredsRPC interface {
	FetchExecutionController(ctx context.Context) (*shreds.ExecutionController, error)
	FetchAllClientSeats(ctx context.Context) ([]shreds.KeyedClientSeat, error)
	FetchAllPaymentEscrows(ctx context.Context) ([]shreds.KeyedPaymentEscrow, error)
	FetchAllMetroHistories(ctx context.Context) ([]shreds.KeyedMetroHistory, error)
	FetchAllDeviceHistories(ctx context.Context) ([]shreds.KeyedDeviceHistory, error)
	FetchAllValidatorClientRewards(ctx context.Context) ([]shreds.KeyedValidatorClientRewards, error)
	FetchShredDistribution(ctx context.Context, subscriptionEpoch uint64) (*shreds.ShredDistribution, error)
}

type ViewConfig struct {
	Logger          *slog.Logger
	Clock           clockwork.Clock
	ShredsRPC       ShredsRPC
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

	// Fetch all batch-fetchable account types in parallel.
	var (
		clientSeats      []shreds.KeyedClientSeat
		paymentEscrows   []shreds.KeyedPaymentEscrow
		metroHistories   []shreds.KeyedMetroHistory
		deviceHistories  []shreds.KeyedDeviceHistory
		validatorRewards []shreds.KeyedValidatorClientRewards
		distributions    []ShredDistributionRow
	)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		clientSeats, err = v.cfg.ShredsRPC.FetchAllClientSeats(gctx)
		return err
	})
	g.Go(func() error {
		var err error
		paymentEscrows, err = v.cfg.ShredsRPC.FetchAllPaymentEscrows(gctx)
		return err
	})
	g.Go(func() error {
		var err error
		metroHistories, err = v.cfg.ShredsRPC.FetchAllMetroHistories(gctx)
		return err
	})
	g.Go(func() error {
		var err error
		deviceHistories, err = v.cfg.ShredsRPC.FetchAllDeviceHistories(gctx)
		return err
	})
	g.Go(func() error {
		var err error
		validatorRewards, err = v.cfg.ShredsRPC.FetchAllValidatorClientRewards(gctx)
		return err
	})
	g.Go(func() error {
		if ec.CurrentSubscriptionEpoch == 0 {
			return nil
		}
		dist, err := v.cfg.ShredsRPC.FetchShredDistribution(gctx, ec.CurrentSubscriptionEpoch)
		if err != nil {
			// Distribution may not exist yet for the current epoch; log and continue.
			v.log.Warn("shreds: failed to fetch current distribution, skipping",
				"epoch", ec.CurrentSubscriptionEpoch, "error", err)
			return nil
		}
		distributions = []ShredDistributionRow{convertShredDistribution(dist)}
		return nil
	})

	if err := g.Wait(); err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("shreds", "error").Inc()
		return result, fmt.Errorf("fetch shreds accounts: %w", err)
	}

	v.log.Debug("shreds: fetched program data",
		"client_seats", len(clientSeats),
		"payment_escrows", len(paymentEscrows),
		"metro_histories", len(metroHistories),
		"device_histories", len(deviceHistories),
		"validator_rewards", len(validatorRewards),
		"distributions", len(distributions),
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

	totalRows := len(ecRows) + len(csRows) + len(peRows) + len(mhRows) + len(dhRows) + len(vrRows) + len(distributions)
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

// Compile-time check that *shreds.Client implements ShredsRPC.
var _ ShredsRPC = (*shreds.Client)(nil)
