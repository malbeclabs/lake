package dzsvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/jonboulle/clockwork"
	"github.com/malbeclabs/doublezero/smartcontract/sdk/go/serviceability"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	"github.com/malbeclabs/lake/indexer/pkg/metrics"
)

type Contributor struct {
	PK   string
	Code string
	Name string
}

var (
	contributorNamesByCode = map[string]string{
		"allnodes": "Allnodes",
		"jump_":    "Jump Crypto",
		"dgt":      "Distributed Global",
		"cherry":   "Cherry Servers",
		"cdrw":     "Cumberland/DRW",
		"glxy":     "Galaxy",
		"infiber":  "InFiber",
		"laconic":  "Laconic",
		"latitude": "Latitude",
		"rox":      "RockawayX",
		"s3v":      "S3V",
		"stakefac": "Staking Facilities",
		"tsw":      "Teraswitch",
		"velia":    "Velia",
	}
)

type Interface struct {
	Name   string `json:"name"`
	IP     string `json:"ip"`
	Status string `json:"status"`
}

type DeviceInterface struct {
	DevicePK           string
	Intf               string
	Status             string
	InterfaceType      string
	CYOAType           string
	DIAType            string
	LoopbackType       string
	RoutingMode        string
	Bandwidth          uint64
	Cir                uint64
	Mtu                uint16
	VlanID             uint16
	NodeSegmentIdx     uint16
	UserTunnelEndpoint bool
}

type Device struct {
	PK                        string
	Status                    string
	DeviceType                string
	Code                      string
	PublicIP                  string
	ContributorPK             string
	MetroPK                   string
	LocationPK                string
	MaxUsers                  uint16
	MaxUnicastUsers           uint16
	MaxMulticastSubscribers   uint16
	MaxMulticastPublishers    uint16
	UnicastUsersCount         uint16
	MulticastSubscribersCount uint16
	ReservedSeats             uint16
	MulticastPublishersCount  uint16
	Interfaces                []Interface
}

type Metro struct {
	PK        string
	Code      string
	Name      string
	Longitude float64
	Latitude  float64
}

type Link struct {
	PK                  string
	Status              string
	Code                string
	TunnelNet           string
	ContributorPK       string
	SideAPK             string
	SideZPK             string
	SideAIfaceName      string
	SideZIfaceName      string
	SideAIP             string
	SideZIP             string
	LinkType            string
	CommittedRTTNs      uint64
	CommittedJitterNs   uint64
	Bandwidth           uint64
	ISISDelayOverrideNs uint64
	LinkTopologies      string // JSON array of topology names
	UnicastDrained      bool
}

type Topology struct {
	PK             string
	Name           string
	AdminGroupBit  uint8
	FlexAlgoNumber uint8
	Color          uint8
	Constraint     string
}

type User struct {
	PK          string
	OwnerPubkey string
	Status      string
	Kind        string
	ClientIP    net.IP
	DZIP        net.IP
	DevicePK    string
	TenantPK    string
	TunnelID    uint16
	Publishers  []string // multicast group PKs this user publishes to
	Subscribers []string // multicast group PKs this user subscribes to
	// BgpStatus is the onchain BGP session status as last reported by the
	// device agent: always one of "up" | "down" | "unknown".
	BgpStatus string
}

type MulticastGroup struct {
	PK              string
	OwnerPubkey     string
	Code            string
	MulticastIP     net.IP
	MaxBandwidth    uint64
	Status          string
	PublisherCount  uint32
	SubscriberCount uint32
}

type Tenant struct {
	PK                string
	OwnerPubkey       string
	Code              string
	PaymentStatus     string
	VrfID             uint16
	MetroRouting      bool
	RouteLiveness     bool
	BillingRate       uint64
	IncludeTopologies string // JSON array of topology names
}

type AccessPass struct {
	PK                 string
	OwnerPubkey        string
	TypeTag            string
	AssociatedPubkey   string
	OthersTypeName     string
	OthersKey          string
	ClientIP           net.IP
	UserPayer          string
	LastAccessEpoch    uint64
	ConnectionCount    uint16
	Status             string
	MGroupPubAllowlist []string
	MGroupSubAllowlist []string
	Flags              uint8
}

type Location struct {
	PK             string
	Owner          string
	Lat            float64
	Lng            float64
	LocId          uint32
	Status         string
	Code           string
	Name           string
	Country        string
	ReferenceCount uint32
}

type ServiceabilityRPC interface {
	GetProgramData(ctx context.Context) (*serviceability.ProgramData, error)
}

type ViewConfig struct {
	Logger            *slog.Logger
	Clock             clockwork.Clock
	ServiceabilityRPC ServiceabilityRPC
	RefreshInterval   time.Duration
	ClickHouse        clickhouse.Client
}

func (cfg *ViewConfig) Validate() error {
	if cfg.Logger == nil {
		return errors.New("logger is required")
	}
	if cfg.ServiceabilityRPC == nil {
		return errors.New("serviceability rpc is required")
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
	refreshMu sync.Mutex // prevents concurrent refreshes

	fetchedAt time.Time
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

	v := &View{
		log:     cfg.Logger,
		cfg:     cfg,
		store:   store,
		readyCh: make(chan struct{}),
	}

	// Tables are created automatically by SCDTableBatch on first refresh
	return v, nil
}

func (v *View) Store() *Store {
	return v.store
}

// Ready returns true if the view has completed at least one successful refresh
func (v *View) Ready() bool {
	select {
	case <-v.readyCh:
		return true
	default:
		return false
	}
}

// WaitReady waits for the view to be ready (has completed at least one successful refresh)
// It returns immediately if already ready, or blocks until ready or context is cancelled.
func (v *View) WaitReady(ctx context.Context) error {
	select {
	case <-v.readyCh:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while waiting for serviceability view: %w", ctx.Err())
	}
}

func (v *View) Start(ctx context.Context) {
	go func() {
		v.log.Info("serviceability: starting refresh loop", "interval", v.cfg.RefreshInterval)

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

// safeRefresh wraps Refresh with panic recovery to prevent the refresh loop from dying
func (v *View) safeRefresh(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			v.log.Error("serviceability: refresh panicked", "panic", r)
			metrics.ViewRefreshTotal.WithLabelValues("serviceability", "panic").Inc()
		}
	}()

	if _, err := v.Refresh(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		v.log.Error("serviceability: refresh failed", "error", err)
	}
}

func (v *View) Refresh(ctx context.Context) (ingestionlog.RefreshResult, error) {
	v.refreshMu.Lock()
	defer v.refreshMu.Unlock()

	var result ingestionlog.RefreshResult

	refreshStart := time.Now()
	v.log.Debug("serviceability: refresh started", "start_time", refreshStart)
	defer func() {
		duration := time.Since(refreshStart)
		v.log.Info("serviceability: refresh completed", "duration", duration.String())
		metrics.ViewRefreshDuration.WithLabelValues("serviceability").Observe(duration.Seconds())
	}()

	v.log.Debug("serviceability: starting refresh")

	pd, err := v.cfg.ServiceabilityRPC.GetProgramData(ctx)
	if err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("serviceability", "error").Inc()
		return result, err
	}

	v.log.Debug("serviceability: fetched program data",
		"contributors", len(pd.Contributors),
		"devices", len(pd.Devices),
		"users", len(pd.Users),
		"links", len(pd.Links),
		"metros", len(pd.Exchanges),
		"locations", len(pd.Locations),
		"multicast_groups", len(pd.MulticastGroups),
		"tenants", len(pd.Tenants),
		"access_passes", len(pd.AccessPasses),
		"topologies", len(pd.Topologies))

	// Validate that we received data for each entity type - empty responses would tombstone all existing entities.
	// Check each independently since they're written separately with MissingMeansDeleted=true.
	if len(pd.Contributors) == 0 {
		metrics.ViewRefreshTotal.WithLabelValues("serviceability", "error").Inc()
		return result, fmt.Errorf("refusing to write snapshot: RPC returned no contributors (possible RPC issue)")
	}
	if len(pd.Devices) == 0 {
		metrics.ViewRefreshTotal.WithLabelValues("serviceability", "error").Inc()
		return result, fmt.Errorf("refusing to write snapshot: RPC returned no devices (possible RPC issue)")
	}
	if len(pd.Exchanges) == 0 {
		metrics.ViewRefreshTotal.WithLabelValues("serviceability", "error").Inc()
		return result, fmt.Errorf("refusing to write snapshot: RPC returned no metros (possible RPC issue)")
	}

	contributors := convertContributors(pd.Contributors)
	devices := convertDevices(pd.Devices)
	deviceInterfaces := convertDeviceInterfaces(pd.Devices)
	users := convertUsers(pd.Users)
	links := convertLinks(pd.Links, pd.Devices, pd.Topologies)
	topologies := convertTopologies(pd.Topologies)
	metros := convertMetros(pd.Exchanges)
	locations := convertLocations(pd.Locations)
	multicastGroups := convertMulticastGroups(pd.MulticastGroups)
	tenants := convertTenants(pd.Tenants, pd.Topologies)
	accessPasses := convertAccessPasses(pd.AccessPasses)

	fetchedAt := time.Now().UTC()

	if err := v.store.ReplaceContributors(ctx, contributors); err != nil {
		return result, fmt.Errorf("failed to replace contributors: %w", err)
	}

	if err := v.store.ReplaceDevices(ctx, devices); err != nil {
		return result, fmt.Errorf("failed to replace devices: %w", err)
	}

	if err := v.store.ReplaceDeviceInterfaces(ctx, deviceInterfaces); err != nil {
		return result, fmt.Errorf("failed to replace device interfaces: %w", err)
	}

	if err := v.store.ReplaceUsers(ctx, users); err != nil {
		return result, fmt.Errorf("failed to replace users: %w", err)
	}

	if err := v.store.ReplaceMetros(ctx, metros); err != nil {
		return result, fmt.Errorf("failed to replace metros: %w", err)
	}

	if err := v.store.ReplaceLocations(ctx, locations); err != nil {
		return result, fmt.Errorf("failed to replace locations: %w", err)
	}

	if err := v.store.ReplaceLinks(ctx, links); err != nil {
		return result, fmt.Errorf("failed to replace links: %w", err)
	}

	if err := v.store.ReplaceTopologies(ctx, topologies); err != nil {
		return result, fmt.Errorf("failed to replace topologies: %w", err)
	}

	if err := v.store.ReplaceMulticastGroups(ctx, multicastGroups); err != nil {
		return result, fmt.Errorf("failed to replace multicast groups: %w", err)
	}

	if err := v.store.ReplaceTenants(ctx, tenants); err != nil {
		return result, fmt.Errorf("failed to replace tenants: %w", err)
	}

	if err := v.store.ReplaceAccessPasses(ctx, accessPasses); err != nil {
		return result, fmt.Errorf("failed to replace access passes: %w", err)
	}

	result.RowsAffected = int64(len(contributors) + len(devices) + len(deviceInterfaces) + len(users) + len(metros) + len(locations) + len(links) + len(topologies) + len(multicastGroups) + len(tenants) + len(accessPasses))
	result.SourceMaxEventTS = &fetchedAt

	v.fetchedAt = fetchedAt
	v.readyOnce.Do(func() {
		close(v.readyCh)
		v.log.Info("serviceability: view is now ready")
	})

	v.log.Debug("serviceability: refresh completed", "fetched_at", fetchedAt)
	metrics.ViewRefreshTotal.WithLabelValues("serviceability", "success").Inc()
	return result, nil
}

// statusString strips the SDK's " (deprecated)" suffix so the strings stored in
// ClickHouse keep lake's stable status vocabulary (e.g. "pending", "expired")
// regardless of upstream deprecation annotations.
func statusString(s fmt.Stringer) string {
	return strings.TrimSuffix(s.String(), " (deprecated)")
}

func convertContributors(onchain []serviceability.Contributor) []Contributor {
	result := make([]Contributor, len(onchain))
	for i, contributor := range onchain {
		name := contributorNamesByCode[contributor.Code]
		result[i] = Contributor{
			PK:   solana.PublicKeyFromBytes(contributor.PubKey[:]).String(),
			Code: contributor.Code,
			Name: name,
		}
	}
	return result
}

func convertDevices(onchain []serviceability.Device) []Device {
	result := make([]Device, len(onchain))
	for i, device := range onchain {
		// Convert interfaces
		interfaces := make([]Interface, 0, len(device.Interfaces))
		for _, iface := range device.Interfaces {
			var ip string
			if iface.IpNet[4] > 0 && iface.IpNet[4] <= 32 {
				ip = fmt.Sprintf("%s/%d", net.IP(iface.IpNet[:4]).String(), iface.IpNet[4])
			}
			interfaces = append(interfaces, Interface{
				Name:   iface.Name,
				IP:     ip,
				Status: statusString(iface.Status),
			})
		}

		result[i] = Device{
			PK:                        solana.PublicKeyFromBytes(device.PubKey[:]).String(),
			Status:                    statusString(device.Status),
			DeviceType:                device.DeviceType.String(),
			Code:                      device.Code,
			PublicIP:                  net.IP(device.PublicIp[:]).String(),
			ContributorPK:             solana.PublicKeyFromBytes(device.ContributorPubKey[:]).String(),
			MetroPK:                   solana.PublicKeyFromBytes(device.ExchangePubKey[:]).String(),
			LocationPK:                solana.PublicKeyFromBytes(device.LocationPubKey[:]).String(),
			MaxUsers:                  device.MaxUsers,
			MaxUnicastUsers:           device.MaxUnicastUsers,
			MaxMulticastSubscribers:   device.MaxMulticastSubscribers,
			MaxMulticastPublishers:    device.MaxMulticastPublishers,
			UnicastUsersCount:         device.UnicastUsersCount,
			MulticastSubscribersCount: device.MulticastSubscribersCount,
			ReservedSeats:             device.ReservedSeats,
			MulticastPublishersCount:  device.MulticastPublishersCount,
			Interfaces:                interfaces,
		}
	}
	return result
}

func convertDeviceInterfaces(onchain []serviceability.Device) []DeviceInterface {
	var result []DeviceInterface
	for _, device := range onchain {
		devicePK := solana.PublicKeyFromBytes(device.PubKey[:]).String()
		for _, iface := range device.Interfaces {
			result = append(result, DeviceInterface{
				DevicePK:           devicePK,
				Intf:               iface.Name,
				Status:             statusString(iface.Status),
				InterfaceType:      iface.InterfaceType.String(),
				CYOAType:           iface.InterfaceCYOA.String(),
				DIAType:            iface.InterfaceDIA.String(),
				LoopbackType:       iface.LoopbackType.String(),
				RoutingMode:        iface.RoutingMode.String(),
				Bandwidth:          iface.Bandwidth,
				Cir:                iface.Cir,
				Mtu:                iface.Mtu,
				VlanID:             iface.VlanId,
				NodeSegmentIdx:     iface.NodeSegmentIdx,
				UserTunnelEndpoint: iface.UserTunnelEndpoint,
			})
		}
	}
	return result
}

func convertUsers(onchain []serviceability.User) []User {
	result := make([]User, len(onchain))
	for i, user := range onchain {
		// Convert publisher group PKs
		publishers := make([]string, len(user.Publishers))
		for j, pub := range user.Publishers {
			publishers[j] = solana.PublicKeyFromBytes(pub[:]).String()
		}
		// Convert subscriber group PKs
		subscribers := make([]string, len(user.Subscribers))
		for j, sub := range user.Subscribers {
			subscribers[j] = solana.PublicKeyFromBytes(sub[:]).String()
		}

		result[i] = User{
			PK:          solana.PublicKeyFromBytes(user.PubKey[:]).String(),
			OwnerPubkey: solana.PublicKeyFromBytes(user.Owner[:]).String(),
			Status:      statusString(user.Status),
			Kind:        user.UserType.String(),
			ClientIP:    net.IP(user.ClientIp[:]),
			DZIP:        net.IP(user.DzIp[:]),
			DevicePK:    solana.PublicKeyFromBytes(user.DevicePubKey[:]).String(),
			TenantPK:    solana.PublicKeyFromBytes(user.TenantPubKey[:]).String(),
			TunnelID:    user.TunnelId,
			Publishers:  publishers,
			Subscribers: subscribers,
			BgpStatus:   bgpStatusString(user.BgpStatus),
		}
	}
	return result
}

// bgpStatusString maps the onchain BGP status enum to the documented
// "up" | "down" | "unknown" vocabulary. Any value outside the known enum (a
// future onchain variant) normalizes to "unknown" so consumers never see an
// out-of-vocabulary string like "BGPStatus(3)".
func bgpStatusString(status uint8) string {
	switch serviceability.BGPStatus(status) {
	case serviceability.BGPStatusUp:
		return "up"
	case serviceability.BGPStatusDown:
		return "down"
	default:
		return "unknown"
	}
}

func convertLinks(onchain []serviceability.Link, devices []serviceability.Device, topologies []serviceability.TopologyInfo) []Link {
	// Build a map of topology pubkey -> name for quick lookup
	topologyNames := make(map[[32]byte]string, len(topologies))
	for _, t := range topologies {
		topologyNames[t.PubKey] = t.Name
	}

	// Build a map of device pubkey -> interface name -> IP for quick lookup
	deviceIfaceIPs := make(map[string]map[string]string)
	for _, device := range devices {
		devicePK := solana.PublicKeyFromBytes(device.PubKey[:]).String()
		ifaceMap := make(map[string]string)
		for _, iface := range device.Interfaces {
			if iface.IpNet[4] > 0 && iface.IpNet[4] <= 32 {
				ip := net.IP(iface.IpNet[:4]).String()
				ifaceMap[iface.Name] = ip
			}
		}
		deviceIfaceIPs[devicePK] = ifaceMap
	}

	result := make([]Link, len(onchain))
	for i, link := range onchain {
		tunnelNet := net.IPNet{
			IP:   net.IP(link.TunnelNet[:4]),
			Mask: net.CIDRMask(int(link.TunnelNet[4]), 32),
		}
		sideAPK := solana.PublicKeyFromBytes(link.SideAPubKey[:]).String()
		sideZPK := solana.PublicKeyFromBytes(link.SideZPubKey[:]).String()

		// Look up interface IPs
		var sideAIP, sideZIP string
		if ifaceMap, ok := deviceIfaceIPs[sideAPK]; ok {
			sideAIP = ifaceMap[link.SideAIfaceName]
		}
		if ifaceMap, ok := deviceIfaceIPs[sideZPK]; ok {
			sideZIP = ifaceMap[link.SideZIfaceName]
		}

		names := make([]string, 0, len(link.LinkTopologies))
		for _, pk := range link.LinkTopologies {
			if name, ok := topologyNames[pk]; ok {
				names = append(names, name)
			}
		}
		topologiesJSON, _ := json.Marshal(names)

		result[i] = Link{
			PK:                  solana.PublicKeyFromBytes(link.PubKey[:]).String(),
			Status:              statusString(link.Status),
			Code:                link.Code,
			SideAPK:             sideAPK,
			SideZPK:             sideZPK,
			ContributorPK:       solana.PublicKeyFromBytes(link.ContributorPubKey[:]).String(),
			SideAIfaceName:      link.SideAIfaceName,
			SideZIfaceName:      link.SideZIfaceName,
			SideAIP:             sideAIP,
			SideZIP:             sideZIP,
			TunnelNet:           tunnelNet.String(),
			LinkType:            link.LinkType.String(),
			CommittedRTTNs:      link.DelayNs,
			CommittedJitterNs:   link.JitterNs,
			Bandwidth:           link.Bandwidth,
			ISISDelayOverrideNs: link.DelayOverrideNs,
			LinkTopologies:      string(topologiesJSON),
			UnicastDrained:      link.LinkFlags&serviceability.LinkFlagUnicastDrained != 0,
		}
	}
	return result
}

func convertTopologies(onchain []serviceability.TopologyInfo) []Topology {
	result := make([]Topology, len(onchain))
	for i, t := range onchain {
		result[i] = Topology{
			PK:             solana.PublicKeyFromBytes(t.PubKey[:]).String(),
			Name:           t.Name,
			AdminGroupBit:  t.AdminGroupBit,
			FlexAlgoNumber: t.FlexAlgoNumber,
			Color:          t.AdminGroupBit + 1,
			Constraint:     t.Constraint.String(),
		}
	}
	return result
}

func convertMetros(onchain []serviceability.Exchange) []Metro {
	result := make([]Metro, len(onchain))
	for i, exchange := range onchain {
		result[i] = Metro{
			PK:        solana.PublicKeyFromBytes(exchange.PubKey[:]).String(),
			Code:      exchange.Code,
			Name:      exchange.Name,
			Longitude: float64(exchange.Lng),
			Latitude:  float64(exchange.Lat),
		}
	}
	return result
}

func convertTenants(onchain []serviceability.Tenant, topologies []serviceability.TopologyInfo) []Tenant {
	// Build a map of topology pubkey -> name for quick lookup
	topologyNames := make(map[[32]byte]string, len(topologies))
	for _, topo := range topologies {
		topologyNames[topo.PubKey] = topo.Name
	}

	result := make([]Tenant, len(onchain))
	for i, t := range onchain {
		names := make([]string, 0, len(t.IncludeTopologies))
		for _, pk := range t.IncludeTopologies {
			if name, ok := topologyNames[pk]; ok {
				names = append(names, name)
			}
		}
		topologiesJSON, _ := json.Marshal(names)

		result[i] = Tenant{
			PK:                solana.PublicKeyFromBytes(t.PubKey[:]).String(),
			OwnerPubkey:       solana.PublicKeyFromBytes(t.Owner[:]).String(),
			Code:              t.Code,
			PaymentStatus:     statusString(t.PaymentStatus),
			VrfID:             t.VrfId,
			MetroRouting:      t.MetroRouting,
			RouteLiveness:     t.RouteLiveness,
			BillingRate:       t.BillingRate,
			IncludeTopologies: string(topologiesJSON),
		}
	}
	return result
}

func convertLocations(onchain []serviceability.Location) []Location {
	result := make([]Location, len(onchain))
	for i, loc := range onchain {
		result[i] = Location{
			PK:             solana.PublicKeyFromBytes(loc.PubKey[:]).String(),
			Owner:          solana.PublicKeyFromBytes(loc.Owner[:]).String(),
			Lat:            loc.Lat,
			Lng:            loc.Lng,
			LocId:          loc.LocId,
			Status:         statusString(loc.Status),
			Code:           loc.Code,
			Name:           loc.Name,
			Country:        loc.Country,
			ReferenceCount: loc.ReferenceCount,
		}
	}
	return result
}

func convertMulticastGroups(onchain []serviceability.MulticastGroup) []MulticastGroup {
	result := make([]MulticastGroup, len(onchain))
	for i, group := range onchain {
		var status string
		switch group.Status {
		case 0:
			status = "pending"
		case 1:
			status = "activated"
		case 2:
			status = "suspended"
		case 3:
			status = "deleted"
		default:
			status = "unknown"
		}
		result[i] = MulticastGroup{
			PK:              solana.PublicKeyFromBytes(group.PubKey[:]).String(),
			OwnerPubkey:     solana.PublicKeyFromBytes(group.Owner[:]).String(),
			Code:            group.Code,
			MulticastIP:     net.IP(group.MulticastIp[:]),
			MaxBandwidth:    group.MaxBandwidth,
			Status:          status,
			PublisherCount:  group.PublisherCount,
			SubscriberCount: group.SubscriberCount,
		}
	}
	return result
}

func accessPassTypeTagString(t serviceability.AccessPassTypeTag) string {
	switch t {
	case serviceability.AccessPassTypePrepaid:
		return "prepaid"
	case serviceability.AccessPassTypeSolanaValidator:
		return "solana_validator"
	case serviceability.AccessPassTypeSolanaRPC:
		return "solana_rpc"
	case serviceability.AccessPassTypeOthers:
		return "others"
	case serviceability.AccessPassTypeEdgeSeat:
		return "edge_seat"
	default:
		return "unknown"
	}
}

func convertAccessPasses(onchain []serviceability.AccessPass) []AccessPass {
	result := make([]AccessPass, len(onchain))
	for i, ap := range onchain {
		// Only SolanaValidator and SolanaRPC passes carry an associated pubkey onchain.
		var associatedPubkey string
		if ap.AccessPassTypeTag == serviceability.AccessPassTypeSolanaValidator ||
			ap.AccessPassTypeTag == serviceability.AccessPassTypeSolanaRPC {
			associatedPubkey = solana.PublicKeyFromBytes(ap.AssociatedPubkey[:]).String()
		}

		pubAllowlist := make([]string, len(ap.MGroupPubAllowlist))
		for j, pk := range ap.MGroupPubAllowlist {
			pubAllowlist[j] = solana.PublicKeyFromBytes(pk[:]).String()
		}
		subAllowlist := make([]string, len(ap.MGroupSubAllowlist))
		for j, pk := range ap.MGroupSubAllowlist {
			subAllowlist[j] = solana.PublicKeyFromBytes(pk[:]).String()
		}

		result[i] = AccessPass{
			PK:                 solana.PublicKeyFromBytes(ap.PubKey[:]).String(),
			OwnerPubkey:        solana.PublicKeyFromBytes(ap.Owner[:]).String(),
			TypeTag:            accessPassTypeTagString(ap.AccessPassTypeTag),
			AssociatedPubkey:   associatedPubkey,
			OthersTypeName:     ap.OthersTypeName,
			OthersKey:          ap.OthersKey,
			ClientIP:           net.IP(ap.ClientIp[:]),
			UserPayer:          solana.PublicKeyFromBytes(ap.UserPayer[:]).String(),
			LastAccessEpoch:    ap.LastAccessEpoch,
			ConnectionCount:    ap.ConnectionCount,
			Status:             statusString(ap.Status),
			MGroupPubAllowlist: pubAllowlist,
			MGroupSubAllowlist: subAllowlist,
			Flags:              ap.Flags,
		}
	}
	return result
}
