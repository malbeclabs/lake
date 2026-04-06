package dzsvc

import (
	"encoding/json"
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// ContributorSchema defines the schema for contributors
type ContributorSchema struct{}

func (s *ContributorSchema) Name() string {
	return "dz_contributors"
}

func (s *ContributorSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *ContributorSchema) PayloadColumns() []string {
	return []string{"code:VARCHAR", "name:VARCHAR"}
}

func (s *ContributorSchema) ToRow(c Contributor) []any {
	return []any{c.PK, c.Code, c.Name}
}

func (s *ContributorSchema) GetPrimaryKey(c Contributor) string {
	return c.PK
}

// DeviceSchema defines the schema for devices
type DeviceSchema struct{}

func (s *DeviceSchema) Name() string {
	return "dz_devices"
}

func (s *DeviceSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *DeviceSchema) PayloadColumns() []string {
	return []string{
		"status:VARCHAR",
		"device_type:VARCHAR",
		"code:VARCHAR",
		"public_ip:VARCHAR",
		"contributor_pk:VARCHAR",
		"metro_pk:VARCHAR",
		"max_users:INTEGER",
		"interfaces:VARCHAR",
	}
}

func (s *DeviceSchema) ToRow(d Device) []any {
	interfacesJSON, _ := json.Marshal(d.Interfaces)
	return []any{
		d.PK,
		d.Status,
		d.DeviceType,
		d.Code,
		d.PublicIP,
		d.ContributorPK,
		d.MetroPK,
		d.MaxUsers,
		string(interfacesJSON),
	}
}

func (s *DeviceSchema) GetPrimaryKey(d Device) string {
	return d.PK
}

// DeviceInterfaceSchema defines the schema for device interfaces
type DeviceInterfaceSchema struct{}

func (s *DeviceInterfaceSchema) Name() string {
	return "dz_device_interfaces"
}

func (s *DeviceInterfaceSchema) PrimaryKeyColumns() []string {
	return []string{"device_pk:VARCHAR", "intf:VARCHAR"}
}

func (s *DeviceInterfaceSchema) PayloadColumns() []string {
	return []string{
		"status:VARCHAR",
		"interface_type:VARCHAR",
		"cyoa_type:VARCHAR",
		"dia_type:VARCHAR",
		"loopback_type:VARCHAR",
		"routing_mode:VARCHAR",
		"bandwidth:BIGINT",
		"cir:BIGINT",
		"mtu:INTEGER",
		"vlan_id:INTEGER",
		"node_segment_idx:INTEGER",
		"user_tunnel_endpoint:BOOLEAN",
	}
}

func (s *DeviceInterfaceSchema) ToRow(di DeviceInterface) []any {
	return []any{
		di.DevicePK,
		di.Intf,
		di.Status,
		di.InterfaceType,
		di.CYOAType,
		di.DIAType,
		di.LoopbackType,
		di.RoutingMode,
		di.Bandwidth,
		di.Cir,
		di.Mtu,
		di.VlanID,
		di.NodeSegmentIdx,
		di.UserTunnelEndpoint,
	}
}

func (s *DeviceInterfaceSchema) GetPrimaryKey(di DeviceInterface) string {
	return di.DevicePK + ":" + di.Intf
}

// UserSchema defines the schema for users
type UserSchema struct{}

func (s *UserSchema) Name() string {
	return "dz_users"
}

func (s *UserSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *UserSchema) PayloadColumns() []string {
	return []string{
		"owner_pubkey:VARCHAR",
		"status:VARCHAR",
		"kind:VARCHAR",
		"client_ip:VARCHAR",
		"dz_ip:VARCHAR",
		"device_pk:VARCHAR",
		"tenant_pk:VARCHAR",
		"tunnel_id:INTEGER",
		"publishers:VARCHAR",
		"subscribers:VARCHAR",
	}
}

func (s *UserSchema) ToRow(u User) []any {
	publishersJSON, _ := json.Marshal(u.Publishers)
	subscribersJSON, _ := json.Marshal(u.Subscribers)
	return []any{
		u.PK,
		u.OwnerPubkey,
		u.Status,
		u.Kind,
		u.ClientIP.String(),
		u.DZIP.String(),
		u.DevicePK,
		u.TenantPK,
		u.TunnelID,
		string(publishersJSON),
		string(subscribersJSON),
	}
}

func (s *UserSchema) GetPrimaryKey(u User) string {
	return u.PK
}

// MetroSchema defines the schema for metros
type MetroSchema struct{}

func (s *MetroSchema) Name() string {
	return "dz_metros"
}

func (s *MetroSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *MetroSchema) PayloadColumns() []string {
	return []string{
		"code:VARCHAR",
		"name:VARCHAR",
		"longitude:DOUBLE",
		"latitude:DOUBLE",
	}
}

func (s *MetroSchema) ToRow(m Metro) []any {
	return []any{
		m.PK,
		m.Code,
		m.Name,
		m.Longitude,
		m.Latitude,
	}
}

func (s *MetroSchema) GetPrimaryKey(m Metro) string {
	return m.PK
}

// LinkSchema defines the schema for links
type LinkSchema struct{}

func (s *LinkSchema) Name() string {
	return "dz_links"
}

func (s *LinkSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *LinkSchema) PayloadColumns() []string {
	return []string{
		"status:VARCHAR",
		"code:VARCHAR",
		"tunnel_net:VARCHAR",
		"contributor_pk:VARCHAR",
		"side_a_pk:VARCHAR",
		"side_z_pk:VARCHAR",
		"side_a_iface_name:VARCHAR",
		"side_z_iface_name:VARCHAR",
		"side_a_ip:VARCHAR",
		"side_z_ip:VARCHAR",
		"link_type:VARCHAR",
		"committed_rtt_ns:BIGINT",
		"committed_jitter_ns:BIGINT",
		"bandwidth_bps:BIGINT",
		"isis_delay_override_ns:BIGINT",
	}
}

func (s *LinkSchema) ToRow(l Link) []any {
	return []any{
		l.PK,
		l.Status,
		l.Code,
		l.TunnelNet,
		l.ContributorPK,
		l.SideAPK,
		l.SideZPK,
		l.SideAIfaceName,
		l.SideZIfaceName,
		l.SideAIP,
		l.SideZIP,
		l.LinkType,
		l.CommittedRTTNs,
		l.CommittedJitterNs,
		l.Bandwidth,
		l.ISISDelayOverrideNs,
	}
}

func (s *LinkSchema) GetPrimaryKey(l Link) string {
	return l.PK
}

// MulticastGroupSchema defines the schema for multicast groups
type MulticastGroupSchema struct{}

func (s *MulticastGroupSchema) Name() string {
	return "dz_multicast_groups"
}

func (s *MulticastGroupSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *MulticastGroupSchema) PayloadColumns() []string {
	return []string{
		"owner_pubkey:VARCHAR",
		"code:VARCHAR",
		"multicast_ip:VARCHAR",
		"max_bandwidth:BIGINT",
		"status:VARCHAR",
		"publisher_count:INTEGER",
		"subscriber_count:INTEGER",
	}
}

func (s *MulticastGroupSchema) ToRow(m MulticastGroup) []any {
	return []any{
		m.PK,
		m.OwnerPubkey,
		m.Code,
		m.MulticastIP.String(),
		m.MaxBandwidth,
		m.Status,
		m.PublisherCount,
		m.SubscriberCount,
	}
}

func (s *MulticastGroupSchema) GetPrimaryKey(m MulticastGroup) string {
	return m.PK
}

// TenantSchema defines the schema for tenants
type TenantSchema struct{}

func (s *TenantSchema) Name() string {
	return "dz_tenants"
}

func (s *TenantSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *TenantSchema) PayloadColumns() []string {
	return []string{
		"owner_pubkey:VARCHAR",
		"code:VARCHAR",
		"payment_status:VARCHAR",
		"vrf_id:INTEGER",
		"metro_routing:BOOLEAN",
		"route_liveness:BOOLEAN",
		"billing_rate:BIGINT",
	}
}

func (s *TenantSchema) ToRow(t Tenant) []any {
	return []any{
		t.PK,
		t.OwnerPubkey,
		t.Code,
		t.PaymentStatus,
		t.VrfID,
		t.MetroRouting,
		t.RouteLiveness,
		t.BillingRate,
	}
}

func (s *TenantSchema) GetPrimaryKey(t Tenant) string {
	return t.PK
}

var (
	contributorSchema     = &ContributorSchema{}
	deviceSchema          = &DeviceSchema{}
	deviceInterfaceSchema = &DeviceInterfaceSchema{}
	userSchema            = &UserSchema{}
	metroSchema           = &MetroSchema{}
	linkSchema            = &LinkSchema{}
	multicastGroupSchema  = &MulticastGroupSchema{}
	tenantSchema          = &TenantSchema{}
)

func NewContributorDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, contributorSchema)
}

func NewDeviceDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, deviceSchema)
}

func NewDeviceInterfaceDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, deviceInterfaceSchema)
}

func NewUserDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, userSchema)
}

func NewMetroDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, metroSchema)
}

func NewLinkDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, linkSchema)
}

func NewMulticastGroupDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, multicastGroupSchema)
}

func NewTenantDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, tenantSchema)
}
