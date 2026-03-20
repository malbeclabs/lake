package isis

import (
	"encoding/json"
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// Adjacency represents a row in the isis_adjacencies dimension table.
type Adjacency struct {
	SystemID         string
	NeighborSystemID string
	NeighborAddr     string
	DevicePK         string
	LinkPK           string
	Hostname         string
	RouterID         string
	LocalAddr        string
	Metric           int64
	AdjSIDs          string // JSON array
}

// Device represents a row in the isis_devices dimension table.
type Device struct {
	SystemID        string
	DevicePK        string
	Hostname        string
	RouterID        string
	Overload        uint8
	NodeUnreachable uint8
	Sequence        int64
}

// AdjacencySchema defines the schema for ISIS adjacencies.
type AdjacencySchema struct{}

func (s *AdjacencySchema) Name() string {
	return "isis_adjacencies"
}

func (s *AdjacencySchema) PrimaryKeyColumns() []string {
	return []string{"system_id:VARCHAR", "neighbor_system_id:VARCHAR", "neighbor_addr:VARCHAR"}
}

func (s *AdjacencySchema) PayloadColumns() []string {
	return []string{
		"device_pk:VARCHAR",
		"link_pk:VARCHAR",
		"hostname:VARCHAR",
		"router_id:VARCHAR",
		"local_addr:VARCHAR",
		"metric:BIGINT",
		"adj_sids:VARCHAR",
	}
}

func (s *AdjacencySchema) ToRow(a Adjacency) []any {
	return []any{
		a.SystemID,
		a.NeighborSystemID,
		a.NeighborAddr,
		a.DevicePK,
		a.LinkPK,
		a.Hostname,
		a.RouterID,
		a.LocalAddr,
		a.Metric,
		a.AdjSIDs,
	}
}

func (s *AdjacencySchema) GetPrimaryKey(a Adjacency) string {
	return a.SystemID + "/" + a.NeighborSystemID + "/" + a.NeighborAddr
}

// DeviceSchema defines the schema for ISIS devices.
type DeviceSchema struct{}

func (s *DeviceSchema) Name() string {
	return "isis_devices"
}

func (s *DeviceSchema) PrimaryKeyColumns() []string {
	return []string{"system_id:VARCHAR"}
}

func (s *DeviceSchema) PayloadColumns() []string {
	return []string{
		"device_pk:VARCHAR",
		"hostname:VARCHAR",
		"router_id:VARCHAR",
		"overload:BOOLEAN",
		"node_unreachable:BOOLEAN",
		"sequence:BIGINT",
	}
}

func (s *DeviceSchema) ToRow(d Device) []any {
	return []any{
		d.SystemID,
		d.DevicePK,
		d.Hostname,
		d.RouterID,
		d.Overload,
		d.NodeUnreachable,
		d.Sequence,
	}
}

func (s *DeviceSchema) GetPrimaryKey(d Device) string {
	return d.SystemID
}

var (
	adjacencySchema = &AdjacencySchema{}
	deviceSchema    = &DeviceSchema{}
)

// NewAdjacencyDataset creates a new dimension dataset for ISIS adjacencies.
func NewAdjacencyDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, adjacencySchema)
}

// NewDeviceDataset creates a new dimension dataset for ISIS devices.
func NewDeviceDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, deviceSchema)
}

// AdjSIDsToJSON converts a slice of adjSIDs to a JSON string.
func AdjSIDsToJSON(sids []uint32) string {
	if sids == nil {
		return "[]"
	}
	b, _ := json.Marshal(sids)
	return string(b)
}
