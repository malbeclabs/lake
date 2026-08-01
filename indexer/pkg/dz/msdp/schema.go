package msdp

import (
	"log/slog"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// ------------------------------------------------------------------
// Peer (`show ip msdp summary`) — one row per MSDP peer
// ------------------------------------------------------------------

// PeerRow is the ClickHouse-shaped representation of a Peer.
type PeerRow struct {
	DevicePubkey     string
	PeerIPAddress    string
	State            string
	SessionStartTime time.Time
	SACount          int64
	ResetCount       int64
}

// PeerToRow flattens a Peer into a PeerRow for the given device.
func PeerToRow(devicePubkey string, p Peer) PeerRow {
	return PeerRow{
		DevicePubkey:     devicePubkey,
		PeerIPAddress:    p.PeerIPAddress,
		State:            p.State,
		SessionStartTime: p.SessionStartTime,
		SACount:          p.SACount,
		ResetCount:       p.ResetCount,
	}
}

// PeerSchema defines the dimension schema for MSDP peers.
type PeerSchema struct{}

func (s *PeerSchema) Name() string { return "dz_ip_msdp_peers" }

func (s *PeerSchema) PrimaryKeyColumns() []string {
	return []string{
		"device_pubkey:VARCHAR",
		"peer_address:VARCHAR",
	}
}

func (s *PeerSchema) PayloadColumns() []string {
	return []string{
		"state:VARCHAR",
		"session_start_time:DATETIME",
		"sa_count:BIGINT",
		"reset_count:BIGINT",
	}
}

func (s *PeerSchema) ToRow(r PeerRow) []any {
	return []any{
		r.DevicePubkey,
		r.PeerIPAddress,
		r.State,
		r.SessionStartTime,
		r.SACount,
		r.ResetCount,
	}
}

func (s *PeerSchema) GetPrimaryKey(r PeerRow) string {
	return r.DevicePubkey + "/" + r.PeerIPAddress
}

var peerSchema = &PeerSchema{}

// NewPeerDataset creates a Type-2 dimension dataset for MSDP peers.
func NewPeerDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, peerSchema)
}

// ------------------------------------------------------------------
// PimSACacheEntry (`show ip msdp pim sa-cache`)
// ------------------------------------------------------------------

// PimSACacheRow is the ClickHouse-shaped representation of a PimSACacheEntry.
type PimSACacheRow struct {
	DevicePubkey  string
	GroupAddress  string
	SourceAddress string
	RPAddress     string
}

// PimSACacheToRow flattens a PimSACacheEntry into a PimSACacheRow.
func PimSACacheToRow(devicePubkey string, e PimSACacheEntry) PimSACacheRow {
	return PimSACacheRow{
		DevicePubkey:  devicePubkey,
		GroupAddress:  e.GroupAddress,
		SourceAddress: e.SourceAddress,
		RPAddress:     e.RPAddress,
	}
}

// PimSACacheSchema defines the dimension schema for the local PIM SA cache.
type PimSACacheSchema struct{}

func (s *PimSACacheSchema) Name() string { return "dz_ip_msdp_pim_sa_cache" }

func (s *PimSACacheSchema) PrimaryKeyColumns() []string {
	return []string{
		"device_pubkey:VARCHAR",
		"group_address:VARCHAR",
		"source_address:VARCHAR",
	}
}

func (s *PimSACacheSchema) PayloadColumns() []string {
	return []string{
		"rp_address:VARCHAR",
	}
}

func (s *PimSACacheSchema) ToRow(r PimSACacheRow) []any {
	return []any{
		r.DevicePubkey,
		r.GroupAddress,
		r.SourceAddress,
		r.RPAddress,
	}
}

func (s *PimSACacheSchema) GetPrimaryKey(r PimSACacheRow) string {
	return r.DevicePubkey + "/" + r.GroupAddress + "/" + r.SourceAddress
}

var pimSACacheSchema = &PimSACacheSchema{}

// NewPimSACacheDataset creates a Type-2 dimension dataset for the local
// PIM SA cache entries.
func NewPimSACacheDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, pimSACacheSchema)
}

// ------------------------------------------------------------------
// SACacheEntry (`show ip msdp sa-cache rejected`) — combined accepted +
// rejected SAs, distinguished by the status column in the PK.
// ------------------------------------------------------------------

// SACacheRow is the ClickHouse-shaped representation of a SACacheEntry.
type SACacheRow struct {
	DevicePubkey  string
	GroupAddress  string
	SourceAddress string
	RemoteAddress string
	Status        string // 'accepted' | 'rejected'
	RPAddress     string
}

// SACacheToRow flattens a SACacheEntry into a SACacheRow.
func SACacheToRow(devicePubkey string, e SACacheEntry) SACacheRow {
	return SACacheRow{
		DevicePubkey:  devicePubkey,
		GroupAddress:  e.GroupAddress,
		SourceAddress: e.SourceAddress,
		RemoteAddress: e.RemoteAddress,
		Status:        string(e.Status),
		RPAddress:     e.RPAddress,
	}
}

// SACacheSchema defines the dimension schema for MSDP SA-cache entries.
type SACacheSchema struct{}

func (s *SACacheSchema) Name() string { return "dz_ip_msdp_sa_cache" }

func (s *SACacheSchema) PrimaryKeyColumns() []string {
	return []string{
		"device_pubkey:VARCHAR",
		"group_address:VARCHAR",
		"source_address:VARCHAR",
		"remote_address:VARCHAR",
		"status:VARCHAR",
	}
}

func (s *SACacheSchema) PayloadColumns() []string {
	return []string{
		"rp_address:VARCHAR",
	}
}

func (s *SACacheSchema) ToRow(r SACacheRow) []any {
	return []any{
		r.DevicePubkey,
		r.GroupAddress,
		r.SourceAddress,
		r.RemoteAddress,
		r.Status,
		r.RPAddress,
	}
}

func (s *SACacheSchema) GetPrimaryKey(r SACacheRow) string {
	return r.DevicePubkey + "/" + r.GroupAddress + "/" + r.SourceAddress + "/" + r.RemoteAddress + "/" + r.Status
}

var saCacheSchema = &SACacheSchema{}

// NewSACacheDataset creates a Type-2 dimension dataset for MSDP SA-cache entries.
func NewSACacheDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, saCacheSchema)
}
