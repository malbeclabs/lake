package msdp

import "time"

// Kind names that this package consumes from S3. They match the
// state-ingest server's `defaultStateToCollectShowCommands` map keys.
const (
	// SnapshotKindSummary corresponds to `show ip msdp summary | json`.
	SnapshotKindSummary = "ip-msdp-summary"

	// SnapshotKindPimSACache corresponds to
	// `show ip msdp pim sa-cache | json`. The local PIM SA cache that
	// this device is advertising over MSDP — no remoteAddress field.
	SnapshotKindPimSACache = "ip-msdp-pim-sa-cache"

	// SnapshotKindSACacheRejected corresponds to
	// `show ip msdp sa-cache rejected | json`. Per Arista docs, the
	// `rejected` keyword returns rejected SAs *in addition to* the
	// accepted ones, so this single command supplies both arrays.
	SnapshotKindSACacheRejected = "ip-msdp-sa-cache-rejected"
)

// SACacheStatus distinguishes accepted vs rejected SAs in the
// sa_cache table. The same source command (`sa-cache rejected`)
// produces both via the `acceptedSaMsg` and `rejectedSaMsg` arrays.
type SACacheStatus string

const (
	SACacheStatusAccepted SACacheStatus = "accepted"
	SACacheStatusRejected SACacheStatus = "rejected"
)

// Dump represents a single device's raw MSDP snapshot of one kind.
type Dump struct {
	Kind         string
	FetchedAt    time.Time
	SnapshotTS   time.Time
	DevicePubkey string
	RawJSON      []byte
	FileName     string
}

// Peer is one parsed row from `show ip msdp summary | json`. One row per
// MSDP peer; (device_pubkey, peer_address) is the natural primary key.
type Peer struct {
	DevicePubkey     string
	PeerIPAddress    string
	State            string    // "established" | "connecting" | "listen" | ...
	SessionStartTime time.Time // zero when state is not "established"
	SACount          int64
	ResetCount       int64
}

// PimSACacheEntry is one parsed row from
// `show ip msdp pim sa-cache | json`. PIM SA cache being advertised
// locally via MSDP. (device_pubkey, group_address, source_address) is
// the natural primary key.
type PimSACacheEntry struct {
	DevicePubkey  string
	GroupAddress  string
	SourceAddress string
	RPAddress     string
}

// SACacheEntry is one parsed row from
// `show ip msdp sa-cache rejected | json`, combining both the
// `acceptedSaMsg` array (Status="accepted") and the `rejectedSaMsg`
// array (Status="rejected"). (device_pubkey, group_address,
// source_address, remote_address, status) is the natural primary key —
// the same (S,G) can legitimately appear from multiple remote_addresses
// and in multiple status buckets.
type SACacheEntry struct {
	DevicePubkey  string
	GroupAddress  string
	SourceAddress string
	RemoteAddress string
	RPAddress     string
	Status        SACacheStatus
}
