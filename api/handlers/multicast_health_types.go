package handlers

// Response shapes for the multicast health endpoints (infra#1501 Track 2).
// Each is a thin packaging around the underlying health_* ClickHouse views.

type MulticastHealthGroupSummaryResponse struct {
	Group           MulticastDeliveryGroup `json:"group"`
	SourceAvailable bool                   `json:"source_available"`
	GeneratedAt     string                 `json:"generated_at"`
	Counts          MulticastHealthCounts  `json:"counts"`
}

// MulticastHealthCounts breaks the per-status totals down across the three
// view granularities for a group. A consumer can read this to render a
// per-group health dashboard summary without fetching the full row sets.
type MulticastHealthCounts struct {
	Mroutes MulticastHealthStatusCounts `json:"mroutes"`
	Users   MulticastHealthStatusCounts `json:"users"`
	Paths   MulticastHealthStatusCounts `json:"paths"`
}

type MulticastHealthStatusCounts struct {
	Healthy   uint64 `json:"healthy"`
	Degraded  uint64 `json:"degraded"`
	Unhealthy uint64 `json:"unhealthy"`
	Total     uint64 `json:"total"`
}

// MulticastHealthUserItem matches one row of health_multicast_user.
type MulticastHealthUserItem struct {
	UserPK                string `json:"user_pk"`
	UserOwnerPubkey       string `json:"user_owner_pubkey"`
	UserDZIP              string `json:"user_dz_ip"`
	UserTunnelID          int32  `json:"user_tunnel_id"`
	UserDevicePK          string `json:"user_device_pk"`
	UserDeviceCode        string `json:"user_device_code"`
	MulticastGroupPK      string `json:"multicast_group_pk"`
	MulticastGroupCode    string `json:"multicast_group_code"`
	GroupAddress          string `json:"group_address"`
	Mode                  string `json:"mode"`
	ExpectedTunnelPos     string `json:"expected_tunnel_position"`
	PublisherIIFObserved  bool   `json:"publisher_iif_observed"`
	SubscriberOIFObserved bool   `json:"subscriber_oif_observed"`
	Reconciled            bool   `json:"reconciled"`
	HealthStatus          string `json:"health_status"`
	MismatchReason        string `json:"mismatch_reason,omitempty"`
}

type MulticastHealthGroupUsersResponse struct {
	Group       MulticastDeliveryGroup    `json:"group"`
	GeneratedAt string                    `json:"generated_at"`
	Items       []MulticastHealthUserItem `json:"items"`
	Total       int                       `json:"total"`
	Limit       int                       `json:"limit"`
	Offset      int                       `json:"offset"`
}

type MulticastHealthUserResponse struct {
	UserPK          string                    `json:"user_pk"`
	UserOwnerPubkey string                    `json:"user_owner_pubkey"`
	UserDZIP        string                    `json:"user_dz_ip"`
	UserTunnelID    int32                     `json:"user_tunnel_id"`
	UserDevicePK    string                    `json:"user_device_pk"`
	UserDeviceCode  string                    `json:"user_device_code"`
	GeneratedAt     string                    `json:"generated_at"`
	Items           []MulticastHealthUserItem `json:"items"`
}

// MulticastHealthPathItem matches one row of health_publisher_subscriber_path.
type MulticastHealthPathItem struct {
	PublisherUserPK            string   `json:"publisher_user_pk"`
	PublisherOwnerPubkey       string   `json:"publisher_owner_pubkey"`
	PublisherDZIP              string   `json:"publisher_dz_ip"`
	PublisherTunnelID          int32    `json:"publisher_tunnel_id"`
	PublisherDevicePK          string   `json:"publisher_device_pk"`
	PublisherDeviceCode        string   `json:"publisher_device_code"`
	SubscriberUserPK           string   `json:"subscriber_user_pk"`
	SubscriberOwnerPubkey      string   `json:"subscriber_owner_pubkey"`
	SubscriberDZIP             string   `json:"subscriber_dz_ip"`
	SubscriberTunnelID         int32    `json:"subscriber_tunnel_id"`
	SubscriberDevicePK         string   `json:"subscriber_device_pk"`
	SubscriberDeviceCode       string   `json:"subscriber_device_code"`
	PublisherEndpointObserved  bool     `json:"publisher_endpoint_observed"`
	SubscriberEndpointObserved bool     `json:"subscriber_endpoint_observed"`
	EndpointsReconciled        bool     `json:"endpoints_reconciled"`
	HealthStatus               string   `json:"health_status"`
	VerificationMethod         string   `json:"verification_method"`
	MissingEndpointReasons     []string `json:"missing_endpoint_reasons,omitempty"`
}

type MulticastHealthGroupPathsResponse struct {
	Group       MulticastDeliveryGroup    `json:"group"`
	GeneratedAt string                    `json:"generated_at"`
	Items       []MulticastHealthPathItem `json:"items"`
	Total       int                       `json:"total"`
	Limit       int                       `json:"limit"`
	Offset      int                       `json:"offset"`
}
