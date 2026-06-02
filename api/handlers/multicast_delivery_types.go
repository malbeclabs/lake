package handlers

import "time"

const (
	multicastDeliveryFreshSeconds  = 5 * 60
	multicastDeliveryStaleSeconds  = 30 * 60
	multicastDeliveryMaxRouteRows  = 5000
	multicastDeliveryMaxOIFRows    = 20000
	multicastDeliveryMaxMemberRows = 10000
)

type MulticastDeliveryParams struct {
	Mode       string
	Sources    []string
	Publishers []string
	Devices    []string
	Links      []string
	Includes   map[string]bool
}

// MulticastDeliveryStateResponse is the semantic delivery-state response for a multicast group.
type MulticastDeliveryStateResponse struct {
	Group              MulticastDeliveryGroup             `json:"group"`
	SourceAvailable    bool                               `json:"source_available"`
	GeneratedAt        string                             `json:"generated_at"`
	Mode               string                             `json:"mode"`
	Freshness          MulticastDeliveryFreshness         `json:"freshness"`
	Publishers         []MulticastDeliveryMember          `json:"publishers"`
	Subscribers        []MulticastDeliveryMember          `json:"subscribers"`
	Routes             []MulticastDeliveryRoute           `json:"routes"`
	OIFs               []MulticastDeliveryOIF             `json:"oifs"`
	MSDP               MulticastDeliveryMSDP              `json:"msdp"`
	ObservedSegments   []MulticastDeliverySegment         `json:"observed_segments"`
	ExpectedSegments   []MulticastDeliverySegment         `json:"expected_segments"`
	SubscriberOutcomes []MulticastDeliverySubscriberState `json:"subscriber_outcomes"`
	Anomalies          []MulticastDeliveryAnomaly         `json:"anomalies"`
}

type MulticastDeliveryGroup struct {
	PK              string `json:"pk"`
	Code            string `json:"code"`
	MulticastIP     string `json:"multicast_ip"`
	MaxBandwidth    uint64 `json:"max_bandwidth"`
	Status          string `json:"status"`
	PublisherCount  uint32 `json:"publisher_count"`
	SubscriberCount uint32 `json:"subscriber_count"`
}

type MulticastDeliveryFreshness struct {
	Mroute         MulticastDeliverySourceFreshness `json:"mroute"`
	MSDPPeers      MulticastDeliverySourceFreshness `json:"msdp_peers"`
	MSDPPimSACache MulticastDeliverySourceFreshness `json:"msdp_pim_sa_cache"`
	MSDPSACache    MulticastDeliverySourceFreshness `json:"msdp_sa_cache"`
	PIMNeighbors   MulticastDeliverySourceFreshness `json:"pim_neighbors"`
}

type MulticastDeliverySourceFreshness struct {
	Available        bool    `json:"available"`
	Status           string  `json:"status"`
	RowCount         int     `json:"row_count"`
	LatestSnapshotTS *string `json:"latest_snapshot_ts,omitempty"`
	LatestIngestedAt *string `json:"latest_ingested_at,omitempty"`
	AgeSeconds       *int    `json:"age_seconds,omitempty"`
	Note             string  `json:"note,omitempty"`
}

type MulticastDeliveryMember struct {
	UserPK      string `json:"user_pk"`
	Mode        string `json:"mode"`
	DevicePK    string `json:"device_pk"`
	DeviceCode  string `json:"device_code"`
	MetroPK     string `json:"metro_pk"`
	MetroCode   string `json:"metro_code"`
	ClientIP    string `json:"client_ip"`
	DZIP        string `json:"dz_ip"`
	OwnerPubkey string `json:"owner_pubkey"`
	TunnelID    int32  `json:"tunnel_id"`
}

type MulticastDeliveryRoute struct {
	RouteID                  string `json:"route_id"`
	EntityID                 string `json:"entity_id"`
	SnapshotTS               string `json:"snapshot_ts"`
	IngestedAt               string `json:"ingested_at"`
	AgeSeconds               int    `json:"age_seconds"`
	FreshnessStatus          string `json:"freshness_status"`
	DevicePK                 string `json:"device_pk"`
	DeviceCode               string `json:"device_code"`
	DeviceStatus             string `json:"device_status"`
	DeviceType               string `json:"device_type"`
	MetroCode                string `json:"metro_code"`
	ContributorCode          string `json:"contributor_code"`
	VRF                      string `json:"vrf"`
	Mode                     string `json:"mode"`
	GroupAddress             string `json:"group_address"`
	SourceAddress            string `json:"source_address"`
	RouteFlags               string `json:"route_flags"`
	RegisterInOIFList        bool   `json:"register_in_oif_list"`
	RPFInterface             string `json:"rpf_interface"`
	RPFRIB                   string `json:"rpf_rib"`
	RPFPrefix                string `json:"rpf_prefix"`
	RPFPreference            int64  `json:"rpf_preference"`
	RPFMetric                int64  `json:"rpf_metric"`
	RPFNeighbor              string `json:"rpf_neighbor"`
	RPFAttached              bool   `json:"rpf_attached"`
	RPFHasBlock              bool   `json:"rpf_has_block"`
	OIFList                  string `json:"oif_list"`
	OIFCount                 int64  `json:"oif_count"`
	CreationTime             string `json:"creation_time"`
	PublisherUserPK          string `json:"publisher_user_pk"`
	PublisherDevicePK        string `json:"publisher_device_pk"`
	PublisherDeviceCode      string `json:"publisher_device_code"`
	PublisherMetroCode       string `json:"publisher_metro_code"`
	PublisherContributorCode string `json:"publisher_contributor_code"`
	PublisherTunnelID        int32  `json:"publisher_tunnel_id"`
	PublisherOwnerPubkey     string `json:"publisher_owner_pubkey"`
	PublisherDZIP            string `json:"publisher_dz_ip"`
	SourceMatchStatus        string `json:"source_match_status"`
}

type MulticastDeliveryOIF struct {
	RouteID               string `json:"route_id"`
	EntityID              string `json:"entity_id"`
	SnapshotTS            string `json:"snapshot_ts"`
	AgeSeconds            int    `json:"age_seconds"`
	FreshnessStatus       string `json:"freshness_status"`
	DevicePK              string `json:"device_pk"`
	DeviceCode            string `json:"device_code"`
	GroupAddress          string `json:"group_address"`
	SourceAddress         string `json:"source_address"`
	PublisherUserPK       string `json:"publisher_user_pk"`
	PublisherDevicePK     string `json:"publisher_device_pk"`
	OIFName               string `json:"oif_name"`
	OIFKind               string `json:"oif_kind"`
	ObservedDeliveryRole  string `json:"observed_delivery_role"`
	LinkPK                string `json:"link_pk,omitempty"`
	LinkCode              string `json:"link_code,omitempty"`
	LinkSide              string `json:"link_side,omitempty"`
	PeerDevicePK          string `json:"peer_device_pk,omitempty"`
	PeerDeviceCode        string `json:"peer_device_code,omitempty"`
	PeerInterfaceName     string `json:"peer_interface_name,omitempty"`
	LinkType              string `json:"link_type,omitempty"`
	BandwidthBPS          int64  `json:"bandwidth_bps,omitempty"`
	LinkTopologies        string `json:"link_topologies,omitempty"`
	UnicastDrained        bool   `json:"unicast_drained,omitempty"`
	InterfaceType         string `json:"interface_type,omitempty"`
	RoutingMode           string `json:"routing_mode,omitempty"`
	InterfaceBandwidth    uint64 `json:"interface_bandwidth,omitempty"`
	InterfaceMTU          uint16 `json:"interface_mtu,omitempty"`
	UserTunnelEndpoint    bool   `json:"user_tunnel_endpoint,omitempty"`
	SubscriberUserPK      string `json:"subscriber_user_pk,omitempty"`
	SubscriberDevicePK    string `json:"subscriber_device_pk,omitempty"`
	SubscriberDeviceCode  string `json:"subscriber_device_code,omitempty"`
	SubscriberTunnelID    int32  `json:"subscriber_tunnel_id,omitempty"`
	SubscriberOwnerPubkey string `json:"subscriber_owner_pubkey,omitempty"`
	SubscriberDZIP        string `json:"subscriber_dz_ip,omitempty"`
	SubscriberClientIP    string `json:"subscriber_client_ip,omitempty"`
}

type MulticastDeliveryMSDP struct {
	Peers        []MulticastDeliveryMSDPPeer    `json:"peers"`
	PimSACache   []MulticastDeliveryMSDPSA      `json:"pim_sa_cache"`
	SACache      []MulticastDeliveryMSDPSA      `json:"sa_cache"`
	PIMNeighbors []MulticastDeliveryPIMNeighbor `json:"pim_neighbors"`
}

type MulticastDeliveryMSDPPeer struct {
	EntityID         string `json:"entity_id"`
	SnapshotTS       string `json:"snapshot_ts"`
	AgeSeconds       int    `json:"age_seconds"`
	FreshnessStatus  string `json:"freshness_status"`
	DevicePK         string `json:"device_pk"`
	DeviceCode       string `json:"device_code"`
	PeerAddress      string `json:"peer_address"`
	State            string `json:"state"`
	SessionStartTime string `json:"session_start_time"`
	SACount          int64  `json:"sa_count"`
	ResetCount       int64  `json:"reset_count"`
}

type MulticastDeliveryMSDPSA struct {
	EntityID          string `json:"entity_id"`
	SnapshotTS        string `json:"snapshot_ts"`
	AgeSeconds        int    `json:"age_seconds"`
	FreshnessStatus   string `json:"freshness_status"`
	DevicePK          string `json:"device_pk"`
	DeviceCode        string `json:"device_code"`
	GroupAddress      string `json:"group_address"`
	SourceAddress     string `json:"source_address"`
	PublisherUserPK   string `json:"publisher_user_pk"`
	PublisherDevicePK string `json:"publisher_device_pk"`
	RemoteAddress     string `json:"remote_address,omitempty"`
	Status            string `json:"status,omitempty"`
	RPAddress         string `json:"rp_address"`
	SourceMatchStatus string `json:"source_match_status"`
}

type MulticastDeliveryPIMNeighbor struct{}

type MulticastDeliverySegment struct {
	Source              string   `json:"source"`
	Status              string   `json:"status"`
	FromDevicePK        string   `json:"from_device_pk"`
	FromDeviceCode      string   `json:"from_device_code"`
	ToDevicePK          string   `json:"to_device_pk"`
	ToDeviceCode        string   `json:"to_device_code"`
	LinkPK              string   `json:"link_pk,omitempty"`
	LinkCode            string   `json:"link_code,omitempty"`
	PublisherUserPKs    []string `json:"publisher_user_pks"`
	PublisherDevicePKs  []string `json:"publisher_device_pks"`
	SubscriberUserPKs   []string `json:"subscriber_user_pks"`
	SubscriberDevicePKs []string `json:"subscriber_device_pks"`
	OIFNames            []string `json:"oif_names,omitempty"`
}

type MulticastDeliverySubscriberState struct {
	SubscriberUserPK     string `json:"subscriber_user_pk"`
	SubscriberDevicePK   string `json:"subscriber_device_pk"`
	SubscriberDeviceCode string `json:"subscriber_device_code"`
	Status               string `json:"status"`
	Reason               string `json:"reason"`
}

type MulticastDeliveryAnomaly struct {
	ID        string            `json:"id"`
	Severity  string            `json:"severity"`
	Kind      string            `json:"kind"`
	Scope     string            `json:"scope"`
	ObjectIDs map[string]string `json:"object_ids"`
	Message   string            `json:"message"`
}

type multicastRouteScan struct {
	Route             MulticastDeliveryRoute
	snapshotTS        time.Time
	ingestedAt        time.Time
	creationTime      time.Time
	registerInOIFList uint8
	rpfAttached       uint8
	rpfHasBlock       uint8
}

type multicastOIFScan struct {
	OIF                MulticastDeliveryOIF
	snapshotTS         time.Time
	unicastDrained     uint8
	userTunnelEndpoint uint8
}

type multicastMSDPPeerScan struct {
	Peer             MulticastDeliveryMSDPPeer
	snapshotTS       time.Time
	sessionStartTime time.Time
}

type multicastMSDPSAScan struct {
	SA         MulticastDeliveryMSDPSA
	snapshotTS time.Time
}
