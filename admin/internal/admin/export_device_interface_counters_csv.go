package admin

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	dztelemusage "github.com/malbeclabs/lake/indexer/pkg/dz/telemetry/usage"
)

// ExportDeviceInterfaceCountersCSV exports intfCounters data from InfluxDB to a CSV file.
// The time range is split into chunks to avoid timeouts on large ranges.
func ExportDeviceInterfaceCountersCSV(
	log *slog.Logger,
	influxURL, influxToken, influxBucket string,
	startStr, endStr string,
	outputPath string,
	chunkSize time.Duration,
) error {
	ctx := context.Background()

	startTime, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		return fmt.Errorf("invalid --start-time: %w", err)
	}
	endTime, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		return fmt.Errorf("invalid --end-time: %w", err)
	}
	if !startTime.Before(endTime) {
		return fmt.Errorf("--start-time must be before --end-time")
	}

	client := dztelemusage.NewHTTPInfluxDBClient(influxURL, influxToken, influxBucket)

	f, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %w", outputPath, err)
	}
	defer f.Close()

	chunkStart := startTime.UTC()
	first := true
	for chunkStart.Before(endTime.UTC()) {
		chunkEnd := chunkStart.Add(chunkSize)
		if chunkEnd.After(endTime.UTC()) {
			chunkEnd = endTime.UTC()
		}

		sqlQuery := fmt.Sprintf(`
			SELECT
				time,
				dzd_pubkey,
				host,
				intf,
				model_name,
				serial_number,
				"carrier-transitions",
				"in-broadcast-pkts",
				"in-discards",
				"in-errors",
				"in-fcs-errors",
				"in-multicast-pkts",
				"in-octets",
				"in-pkts",
				"in-unicast-pkts",
				"out-broadcast-pkts",
				"out-discards",
				"out-errors",
				"out-multicast-pkts",
				"out-octets",
				"out-pkts",
				"out-unicast-pkts"
			FROM "intfCounters"
			WHERE time >= '%s' AND time < '%s'
		`, chunkStart.Format(time.RFC3339Nano), chunkEnd.Format(time.RFC3339Nano))

		log.Info("exporting chunk", "start", chunkStart.Format(time.RFC3339), "end", chunkEnd.Format(time.RFC3339))
		if err := client.QueryRawCSV(ctx, sqlQuery, f, !first); err != nil {
			return fmt.Errorf("query failed for chunk %s: %w", chunkStart.Format(time.RFC3339), err)
		}

		first = false
		chunkStart = chunkEnd
	}

	log.Info("export complete", "output", outputPath)
	return nil
}
