package mroute

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

// rawDump matches the top-level shape of `show ip mroute | json` on Arista EOS.
type rawDump struct {
	VRFs map[string]rawVRF `json:"vrfs"`
}

type rawVRF struct {
	SparseMode    rawModeGroups `json:"sparseMode"`
	Bidirectional rawModeGroups `json:"bidirectional"`
}

type rawModeGroups struct {
	Groups map[string]rawGroup `json:"groups"`
}

type rawGroup struct {
	GroupSources map[string]rawSource `json:"groupSources"`
}

type rawSource struct {
	SourceAddress     string   `json:"sourceAddress"`
	CreationTime      float64  `json:"creationTime"`
	RouteFlags        string   `json:"routeFlags"`
	RegisterInOifList bool     `json:"registerInOifList"`
	RpfInterface      string   `json:"rpfInterface"`
	RPF               *rawRPF  `json:"rpf,omitempty"`
	OifList           []string `json:"oifList"`
}

type rawRPF struct {
	RpfRib              string `json:"rpfRib"`
	RpfPrefix           string `json:"rpfPrefix"`
	RpfPreference       uint32 `json:"rpfPreference"`
	RpfMetric           uint32 `json:"rpfMetric"`
	RpfNeighbor         string `json:"rpfNeighbor"`
	RpfAttached         bool   `json:"rpfAttached"`
	RpfEvpnTenantDomain bool   `json:"rpfEvpnTenantDomain"`
	RpfMvpn             bool   `json:"rpfMvpn"`
}

// Parse converts a single device's `show ip mroute | json` dump into a flat
// slice of Entry records. The DevicePubkey field is left zero — the caller
// fills it from the snapshot key. Empty `groups` maps produce zero entries.
func Parse(raw []byte) ([]Entry, error) {
	var d rawDump
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("mroute: unmarshal: %w", err)
	}

	var out []Entry
	for vrfName, vrf := range d.VRFs {
		out = appendEntries(out, vrfName, ModeSparse, vrf.SparseMode)
		out = appendEntries(out, vrfName, ModeBidirectional, vrf.Bidirectional)
	}
	return out, nil
}

func appendEntries(out []Entry, vrf string, mode Mode, mg rawModeGroups) []Entry {
	for group, g := range mg.Groups {
		for _, src := range g.GroupSources {
			out = append(out, toEntry(vrf, mode, group, src))
		}
	}
	return out
}

func toEntry(vrf string, mode Mode, group string, src rawSource) Entry {
	e := Entry{
		VRF:               vrf,
		Mode:              mode,
		GroupAddress:      group,
		SourceAddress:     src.SourceAddress,
		CreationTime:      fractionalToTime(src.CreationTime),
		RouteFlags:        src.RouteFlags,
		RegisterInOifList: src.RegisterInOifList,
		RpfInterface:      src.RpfInterface,
		OifList:           src.OifList,
	}
	if src.RPF != nil {
		e.RPF = &RPF{
			Rib:              src.RPF.RpfRib,
			Prefix:           src.RPF.RpfPrefix,
			Preference:       src.RPF.RpfPreference,
			Metric:           src.RPF.RpfMetric,
			Neighbor:         src.RPF.RpfNeighbor,
			Attached:         src.RPF.RpfAttached,
			EvpnTenantDomain: src.RPF.RpfEvpnTenantDomain,
			Mvpn:             src.RPF.RpfMvpn,
		}
	}
	return e
}

// fractionalToTime converts an Arista `creationTime` (seconds since epoch as a
// float64, often with fractional precision) to a time.Time at nanosecond
// resolution.
func fractionalToTime(secs float64) time.Time {
	if secs == 0 || math.IsNaN(secs) || math.IsInf(secs, 0) {
		return time.Time{}
	}
	whole, frac := math.Modf(secs)
	return time.Unix(int64(whole), int64(math.Round(frac*1e9))).UTC()
}
