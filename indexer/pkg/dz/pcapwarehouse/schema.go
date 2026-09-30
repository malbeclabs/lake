package pcapwarehouse

import (
	"log/slog"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// FileRow is one pcap file in the warehouse, as its offload manifest describes it.
// Column order in ToRow must match the migration DDL (first_packet_ts first, as the
// TimeColumn).
type FileRow struct {
	FirstPacketTS  time.Time
	IngestedAt     time.Time
	Recorder       string // host name, from the recorder's key prefix
	RecorderIP     string // public IP, from the recorder's key prefix
	HourTS         time.Time
	S3Key          string
	MulticastGroup string
	MulticastPort  uint16
	SizeBytes      uint64
	Packets        uint64
	LastPacketTS   time.Time
	MD5            string
	ManifestKey    string
	OffloadTS      time.Time
}

type fileSchema struct{}

func (s *fileSchema) Name() string { return "dz_edge_pcap_file" }

func (s *fileSchema) UniqueKeyColumns() []string {
	return []string{"multicast_group", "recorder", "recorder_ip", "s3_key", "manifest_key"}
}

func (s *fileSchema) Columns() []string {
	return []string{
		"ingested_at:TIMESTAMP",
		"recorder:VARCHAR",
		"recorder_ip:VARCHAR",
		"hour_ts:TIMESTAMP",
		"s3_key:VARCHAR",
		"multicast_group:VARCHAR",
		"multicast_port:INTEGER",
		"size_bytes:BIGINT",
		"packets:BIGINT",
		"last_packet_ts:TIMESTAMP",
		"md5:VARCHAR",
		"manifest_key:VARCHAR",
		"offload_ts:TIMESTAMP",
	}
}

func (s *fileSchema) TimeColumn() string           { return "first_packet_ts" }
func (s *fileSchema) PartitionByTime() bool        { return true }
func (s *fileSchema) DedupMode() dataset.DedupMode { return dataset.DedupReplacing }
func (s *fileSchema) DedupVersionColumn() string   { return "ingested_at" }

func (s *fileSchema) ToRow(row FileRow) []any {
	return []any{
		row.FirstPacketTS.UTC(), // first_packet_ts
		row.IngestedAt,          // ingested_at
		row.Recorder,            // recorder
		row.RecorderIP,          // recorder_ip
		row.HourTS.UTC(),        // hour_ts
		row.S3Key,               // s3_key
		row.MulticastGroup,      // multicast_group
		row.MulticastPort,       // multicast_port
		row.SizeBytes,           // size_bytes
		row.Packets,             // packets
		row.LastPacketTS.UTC(),  // last_packet_ts
		row.MD5,                 // md5
		row.ManifestKey,         // manifest_key
		row.OffloadTS.UTC(),     // offload_ts
	}
}

var schema = &fileSchema{}

func newDataset(log *slog.Logger) (*dataset.FactDataset, error) {
	return dataset.NewFactDataset(log, schema)
}
