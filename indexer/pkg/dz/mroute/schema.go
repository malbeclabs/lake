package mroute

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// Row is a flattened, ClickHouse-friendly representation of an mroute Entry
// joined with the device pubkey and snapshot timestamp. One row per
// (device, vrf, mode, group, source) per change observed.
type Row struct {
	DevicePubkey      string
	VRF               string
	Mode              string
	GroupAddress      string
	SourceAddress     string
	RouteFlags        string
	RegisterInOifList uint8
	RpfInterface      string
	RpfRib            string
	RpfPrefix         string
	RpfPreference     int64
	RpfMetric         int64
	RpfNeighbor       string
	RpfAttached       uint8
	RpfHasBlock       uint8
	OifList           string // JSON array
	OifCount          int64
	CreationTime      time.Time
}

// EntryToRow flattens an Entry into a ClickHouse Row for the given device pubkey.
func EntryToRow(devicePubkey string, e Entry) Row {
	r := Row{
		DevicePubkey:      devicePubkey,
		VRF:               e.VRF,
		Mode:              string(e.Mode),
		GroupAddress:      e.GroupAddress,
		SourceAddress:     e.SourceAddress,
		RouteFlags:        e.RouteFlags,
		RegisterInOifList: boolToU8(e.RegisterInOifList),
		RpfInterface:      e.RpfInterface,
		OifList:           oifListJSON(e.OifList),
		OifCount:          int64(len(e.OifList)),
		CreationTime:      e.CreationTime,
	}
	if e.RPF != nil {
		r.RpfHasBlock = 1
		r.RpfRib = e.RPF.Rib
		r.RpfPrefix = e.RPF.Prefix
		r.RpfPreference = int64(e.RPF.Preference)
		r.RpfMetric = int64(e.RPF.Metric)
		r.RpfNeighbor = e.RPF.Neighbor
		r.RpfAttached = boolToU8(e.RPF.Attached)
	}
	return r
}

// EntrySchema defines the dimension schema for mroute entries.
type EntrySchema struct{}

func (s *EntrySchema) Name() string {
	return "dz_ip_mroute_entries"
}

func (s *EntrySchema) PrimaryKeyColumns() []string {
	return []string{
		"device_pubkey:VARCHAR",
		"vrf:VARCHAR",
		"mode:VARCHAR",
		"group_address:VARCHAR",
		"source_address:VARCHAR",
	}
}

func (s *EntrySchema) PayloadColumns() []string {
	return []string{
		"route_flags:VARCHAR",
		"register_in_oif_list:BOOLEAN",
		"rpf_interface:VARCHAR",
		"rpf_rib:VARCHAR",
		"rpf_prefix:VARCHAR",
		"rpf_preference:BIGINT",
		"rpf_metric:BIGINT",
		"rpf_neighbor:VARCHAR",
		"rpf_attached:BOOLEAN",
		"rpf_has_block:BOOLEAN",
		"oif_list:VARCHAR",
		"oif_count:BIGINT",
		"creation_time:DATETIME",
	}
}

func (s *EntrySchema) ToRow(r Row) []any {
	return []any{
		r.DevicePubkey,
		r.VRF,
		r.Mode,
		r.GroupAddress,
		r.SourceAddress,
		r.RouteFlags,
		r.RegisterInOifList,
		r.RpfInterface,
		r.RpfRib,
		r.RpfPrefix,
		r.RpfPreference,
		r.RpfMetric,
		r.RpfNeighbor,
		r.RpfAttached,
		r.RpfHasBlock,
		r.OifList,
		r.OifCount,
		r.CreationTime,
	}
}

func (s *EntrySchema) GetPrimaryKey(r Row) string {
	return r.DevicePubkey + "/" + r.VRF + "/" + r.Mode + "/" + r.GroupAddress + "/" + r.SourceAddress
}

var entrySchema = &EntrySchema{}

// NewEntryDataset creates a new Type2 dimension dataset for mroute entries.
func NewEntryDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, entrySchema)
}

func boolToU8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

func oifListJSON(oifs []string) string {
	if len(oifs) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(oifs)
	return string(b)
}
