package mroute

import "time"

// Mode is the PIM operating mode for a multicast group.
type Mode string

const (
	ModeSparse        Mode = "sparse"
	ModeBidirectional Mode = "bidirectional"
)

// Dump represents a single device's raw mroute snapshot.
type Dump struct {
	FetchedAt    time.Time
	SnapshotTS   time.Time
	DevicePubkey string
	RawJSON      []byte
	FileName     string
}

// Entry is one parsed (vrf, mode, group, source) row from `show ip mroute | json`.
type Entry struct {
	DevicePubkey      string
	VRF               string
	Mode              Mode
	GroupAddress      string
	SourceAddress     string
	CreationTime      time.Time
	RouteFlags        string
	RegisterInOifList bool
	RpfInterface      string

	// Optional RPF detail block. Present only when the device has installed an
	// active RPF towards the source (RpfInterface != "Null0").
	RPF *RPF

	// OifList may be empty for ME/E entries.
	OifList []string
}

// RPF holds the reverse-path-forwarding detail Arista emits when an mroute
// has an installed RPF interface.
type RPF struct {
	Rib              string
	Prefix           string
	Preference       uint32
	Metric           uint32
	Neighbor         string
	Attached         bool
	EvpnTenantDomain bool
	Mvpn             bool
}
