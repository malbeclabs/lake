// Package statecollect describes the on-disk format used by the
// doublezero-telemetry state collector. Per-kind packages (e.g. mroute,
// msdp) parse the inner Data payload after the envelope has been
// unwrapped.
package statecollect

import "encoding/json"

// Envelope is the JSON shape every state-collect snapshot uploads to S3.
// It mirrors controlplane/telemetry/internal/state/snapshot.go in the
// doublezero repo; any change to that producer must be matched here.
//
// Wire format:
//
//	{
//	  "metadata": {
//	    "kind":      "ip-mroute",
//	    "timestamp": "2026-05-29T15:34:56Z",
//	    "device":    "<solana pubkey>"
//	  },
//	  "data": { ... raw Arista eAPI JSON ... }
//	}
type Envelope struct {
	Metadata Metadata        `json:"metadata"`
	Data     json.RawMessage `json:"data"`
}

// Metadata is the envelope's identifying fields.
type Metadata struct {
	Kind      string `json:"kind"`      // matches state-ingest kind name, e.g. "ip-mroute"
	Timestamp string `json:"timestamp"` // RFC3339 UTC, capture time on the device
	Device    string `json:"device"`    // device pubkey (base58)
}
