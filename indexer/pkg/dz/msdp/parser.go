package msdp

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

// ParseSummary parses `show ip msdp summary | json`. DevicePubkey on the
// returned Peers is left zero — the caller (Sync) fills it from the
// Dump's DevicePubkey.
func ParseSummary(raw []byte) ([]Peer, error) {
	var d struct {
		PeerList []struct {
			PeerIPAddress    string  `json:"peerIpAddress"`
			State            string  `json:"state"`
			SessionStartTime float64 `json:"sessionStartTime"`
			SACount          int64   `json:"saCount"`
			ResetCount       int64   `json:"resetCount"`
		} `json:"peerList"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("msdp: parse summary: %w", err)
	}
	out := make([]Peer, 0, len(d.PeerList))
	for _, p := range d.PeerList {
		out = append(out, Peer{
			PeerIPAddress:    p.PeerIPAddress,
			State:            p.State,
			SessionStartTime: fractionalToTime(p.SessionStartTime),
			SACount:          p.SACount,
			ResetCount:       p.ResetCount,
		})
	}
	return out, nil
}

// ParsePimSACache parses `show ip msdp pim sa-cache | json`. Returns the
// local PIM SA cache being advertised over MSDP.
func ParsePimSACache(raw []byte) ([]PimSACacheEntry, error) {
	var d struct {
		SACache []struct {
			SourceGroupPair struct {
				SourceAddress string `json:"sourceAddress"`
				GroupAddress  string `json:"groupAddress"`
			} `json:"sourceGroupPair"`
			RPAddress string `json:"rpAddress"`
		} `json:"saCache"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("msdp: parse pim sa-cache: %w", err)
	}
	out := make([]PimSACacheEntry, 0, len(d.SACache))
	for _, e := range d.SACache {
		out = append(out, PimSACacheEntry{
			SourceAddress: e.SourceGroupPair.SourceAddress,
			GroupAddress:  e.SourceGroupPair.GroupAddress,
			RPAddress:     e.RPAddress,
		})
	}
	return out, nil
}

// ParseSACacheRejected parses `show ip msdp sa-cache rejected | json`.
// Returns one slice combining both the `acceptedSaMsg` (Status="accepted")
// and `rejectedSaMsg` (Status="rejected") arrays from the response.
func ParseSACacheRejected(raw []byte) ([]SACacheEntry, error) {
	type saMsg struct {
		SourceGroupPair struct {
			SourceAddress string `json:"sourceAddress"`
			GroupAddress  string `json:"groupAddress"`
		} `json:"sourceGroupPair"`
		RPAddress     string `json:"rpAddress"`
		RemoteAddress string `json:"remoteAddress"`
	}
	var d struct {
		AcceptedSaMsg []saMsg `json:"acceptedSaMsg"`
		RejectedSaMsg []saMsg `json:"rejectedSaMsg"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("msdp: parse sa-cache rejected: %w", err)
	}
	out := make([]SACacheEntry, 0, len(d.AcceptedSaMsg)+len(d.RejectedSaMsg))
	for _, e := range d.AcceptedSaMsg {
		out = append(out, SACacheEntry{
			SourceAddress: e.SourceGroupPair.SourceAddress,
			GroupAddress:  e.SourceGroupPair.GroupAddress,
			RemoteAddress: e.RemoteAddress,
			RPAddress:     e.RPAddress,
			Status:        SACacheStatusAccepted,
		})
	}
	for _, e := range d.RejectedSaMsg {
		out = append(out, SACacheEntry{
			SourceAddress: e.SourceGroupPair.SourceAddress,
			GroupAddress:  e.SourceGroupPair.GroupAddress,
			RemoteAddress: e.RemoteAddress,
			RPAddress:     e.RPAddress,
			Status:        SACacheStatusRejected,
		})
	}
	return out, nil
}

// fractionalToTime converts a `sessionStartTime` value (seconds since
// epoch as a float64) to time.Time at nanosecond resolution. Returns
// the zero time when the value is missing or non-finite.
func fractionalToTime(secs float64) time.Time {
	if secs == 0 || math.IsNaN(secs) || math.IsInf(secs, 0) {
		return time.Time{}
	}
	whole, frac := math.Modf(secs)
	return time.Unix(int64(whole), int64(math.Round(frac*1e9))).UTC()
}
