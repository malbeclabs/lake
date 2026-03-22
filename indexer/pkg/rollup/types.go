package rollup

import "time"

const (
	// TaskQueue is the Temporal task queue for rollup workflows.
	TaskQueue = "indexer-rollup"
	// WorkflowID is the Temporal workflow ID for the long-running rollup workflow.
	WorkflowID = "indexer-rollup"
)

// LinkLatencyStats holds latency/loss percentiles for one probe direction.
type LinkLatencyStats struct {
	AvgRttUs float64 `json:"avg_rtt_us"`
	MinRttUs float64 `json:"min_rtt_us"`
	P50RttUs float64 `json:"p50_rtt_us"`
	P90RttUs float64 `json:"p90_rtt_us"`
	P95RttUs float64 `json:"p95_rtt_us"`
	P99RttUs float64 `json:"p99_rtt_us"`
	MaxRttUs float64 `json:"max_rtt_us"`
	LossPct  float64 `json:"loss_pct"`
	Samples  uint32  `json:"samples"`
}

// LinkBucket represents a single link latency/loss rollup for a 5-minute interval.
// Each direction (A→Z, Z→A) has its own full set of percentiles.
// Sourced from fact_dz_device_link_latency (probe data).
type LinkBucket struct {
	BucketTS   time.Time `json:"bucket_ts"`
	LinkPK     string    `json:"link_pk"`
	IngestedAt time.Time `json:"ingested_at"`

	// Direction A→Z (probes originating from side_a)
	A LinkLatencyStats `json:"a"`
	// Direction Z→A (probes originating from side_z)
	Z LinkLatencyStats `json:"z"`

	// Entity state resolved from history tables at write time
	Status       string `json:"status"`       // activated, soft-drained, hard-drained, suspended
	Provisioning bool   `json:"provisioning"` // true when committed_rtt_ns = 1000000000
	ISISDown     bool   `json:"isis_down"`    // true when no ISIS adjacency at bucket time
}

// InterfaceRateStats holds percentile distribution for a traffic rate metric
// (e.g. in_bps, out_pps).
type InterfaceRateStats struct {
	Avg float64 `json:"avg"`
	Min float64 `json:"min"`
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

// DeviceInterfaceBucket represents a single device interface rollup for a 5-minute interval.
// Sourced from fact_dz_device_interface_counters.
type DeviceInterfaceBucket struct {
	BucketTS   time.Time `json:"bucket_ts"`
	DevicePK   string    `json:"device_pk"`
	Intf       string    `json:"intf"`
	IngestedAt time.Time `json:"ingested_at"`

	// Link context from fact table
	LinkPK   string `json:"link_pk"`
	LinkSide string `json:"link_side"`

	// User context
	UserTunnelID *int64 `json:"user_tunnel_id"` // Nullable
	UserPK       string `json:"user_pk"`

	// Error/discard counters
	InErrors           uint64 `json:"in_errors"`
	OutErrors          uint64 `json:"out_errors"`
	InFcsErrors        uint64 `json:"in_fcs_errors"`
	InDiscards         uint64 `json:"in_discards"`
	OutDiscards        uint64 `json:"out_discards"`
	CarrierTransitions uint64 `json:"carrier_transitions"`

	// Traffic rates
	InBps  InterfaceRateStats `json:"in_bps"`
	OutBps InterfaceRateStats `json:"out_bps"`
	InPps  InterfaceRateStats `json:"in_pps"`
	OutPps InterfaceRateStats `json:"out_pps"`

	// Entity state resolved from history tables at write time
	Status          string `json:"status"`           // activated, soft-drained, hard-drained, suspended
	ISISOverload    bool   `json:"isis_overload"`    // device has ISIS overload bit set
	ISISUnreachable bool   `json:"isis_unreachable"` // device is ISIS unreachable
}

// BackfillInput configures a backfill run.
type BackfillInput struct {
	StartTime      time.Time
	EndTime        time.Time
	ChunkSize      time.Duration // default 1h
	SourceDatabase string        // if set, read from this database (e.g. remote proxy tables)
}

// BackfillChunkInput configures a single backfill chunk.
type BackfillChunkInput struct {
	WindowStart    time.Time
	WindowEnd      time.Time
	SourceDatabase string // if set, read from this database (e.g. remote proxy tables)
}
