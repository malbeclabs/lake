package dztelemusage

import (
	"context"
	"fmt"
	"time"
)

// buildLinkLookup builds a map from "device_pk:intf" to LinkInfo by querying the dz_links_history table
func (s *Store) buildLinkLookup(ctx context.Context) (map[string]LinkInfo, error) {
	lookup := make(map[string]LinkInfo)

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	query := `
		WITH ranked AS (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
			FROM dim_dz_links_history
		)
		SELECT
			pk,
			side_a_pk,
			side_a_iface_name,
			side_z_pk,
			side_z_iface_name
		FROM ranked
		WHERE rn = 1 AND is_deleted = 0`
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query links: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var linkPK, sideAPK, sideAIface, sideZPK, sideZIface *string
		if err := rows.Scan(&linkPK, &sideAPK, &sideAIface, &sideZPK, &sideZIface); err != nil {
			return nil, fmt.Errorf("failed to scan link row: %w", err)
		}

		if sideAPK != nil && sideAIface != nil && *sideAPK != "" && *sideAIface != "" {
			key := fmt.Sprintf("%s:%s", *sideAPK, *sideAIface)
			linkPKVal := ""
			if linkPK != nil {
				linkPKVal = *linkPK
			}
			lookup[key] = LinkInfo{LinkPK: linkPKVal, LinkSide: "A"}
		}

		if sideZPK != nil && sideZIface != nil && *sideZPK != "" && *sideZIface != "" {
			key := fmt.Sprintf("%s:%s", *sideZPK, *sideZIface)
			linkPKVal := ""
			if linkPK != nil {
				linkPKVal = *linkPK
			}
			lookup[key] = LinkInfo{LinkPK: linkPKVal, LinkSide: "Z"}
		}
	}

	return lookup, nil
}

// queryBaselineCountersFromClickHouse queries ClickHouse for the last non-null counter values
// before the window start for each device/interface combination.
func (s *Store) queryBaselineCountersFromClickHouse(ctx context.Context, windowStart time.Time) (*CounterBaselines, error) {
	lookbackStart := windowStart.Add(-90 * 24 * time.Hour)

	baselines := &CounterBaselines{
		InDiscards:  make(map[string]*int64),
		InErrors:    make(map[string]*int64),
		InFCSErrors: make(map[string]*int64),
		OutDiscards: make(map[string]*int64),
		OutErrors:   make(map[string]*int64),
	}

	counterFields := []struct {
		field    string
		baseline map[string]*int64
	}{
		{"in_discards", baselines.InDiscards},
		{"in_errors", baselines.InErrors},
		{"in_fcs_errors", baselines.InFCSErrors},
		{"out_discards", baselines.OutDiscards},
		{"out_errors", baselines.OutErrors},
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	for _, cf := range counterFields {
		sqlQuery := fmt.Sprintf(`
			SELECT
				device_pk,
				intf,
				argMax(%s, (event_ts)) as value
			FROM fact_dz_device_interface_counters
			WHERE event_ts >= ? AND event_ts < ? AND %s IS NOT NULL
			GROUP BY device_pk, intf
		`, cf.field, cf.field)

		rows, err := conn.Query(ctx, sqlQuery, lookbackStart, windowStart)
		if err != nil {
			s.log.Warn("telemetry/usage: failed to query baseline for counter from clickhouse", "counter", cf.field, "error", err)
			continue
		}

		for rows.Next() {
			var devicePK, intf *string
			var val *int64
			if err := rows.Scan(&devicePK, &intf, &val); err != nil {
				s.log.Warn("telemetry/usage: failed to scan baseline row", "counter", cf.field, "error", err)
				continue
			}

			if devicePK == nil || intf == nil {
				continue
			}

			key := fmt.Sprintf("%s:%s", *devicePK, *intf)
			if val != nil {
				cf.baseline[key] = val
			}
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			s.log.Warn("telemetry/usage: error iterating baseline rows", "counter", cf.field, "error", err)
		}
	}

	return baselines, nil
}

// convertRowsToUsage converts raw InfluxDB/CSV rows into InterfaceUsage records.
// It computes deltas, forward-fills nulls, and enriches with link information.
// If alreadyWritten is provided, rows already stored in ClickHouse are skipped.
func (s *Store) convertRowsToUsage(rows []map[string]any, baselines *CounterBaselines, linkLookup map[string]LinkInfo, alreadyWritten MaxTimestampsByKey) ([]InterfaceUsage, error) {
	lastKnownValues := make(map[string]map[string]*int64)
	firstRowSeen := make(map[string]bool)
	lastTime := make(map[string]time.Time)

	counterFieldNames := []string{
		"carrier-transitions", "in-broadcast-pkts", "in-discards", "in-errors",
		"in-fcs-errors", "in-multicast-pkts", "in-octets", "in-pkts", "in-unicast-pkts",
		"out-broadcast-pkts", "out-discards", "out-errors", "out-multicast-pkts",
		"out-octets", "out-pkts", "out-unicast-pkts",
	}

	var usage []InterfaceUsage
	totalRows := len(rows)
	logInterval := totalRows / 10
	if logInterval < 100 {
		logInterval = 100
	}

	for i, row := range rows {
		if i > 0 && i%logInterval == 0 {
			s.log.Debug("telemetry/usage: converting rows", "progress", fmt.Sprintf("%d/%d (%.1f%%)", i, totalRows, float64(i)/float64(totalRows)*100))
		}
		u := &InterfaceUsage{}

		timeStr := extractStringFromRow(row, "time")
		if timeStr == nil {
			continue
		}

		var t time.Time
		var err error
		timeFormats := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999999 -0700 UTC",
			"2006-01-02 15:04:05.999999999 +0700 UTC",
			"2006-01-02 15:04:05.999999999 +0000 UTC",
			"2006-01-02 15:04:05.999999 -0700 UTC",
			"2006-01-02 15:04:05.999999 +0700 UTC",
			"2006-01-02 15:04:05.999999 +0000 UTC",
			"2006-01-02 15:04:05.999 -0700 UTC",
			"2006-01-02 15:04:05.999 +0700 UTC",
			"2006-01-02 15:04:05.999 +0000 UTC",
			"2006-01-02 15:04:05 -0700 UTC",
			"2006-01-02 15:04:05 +0700 UTC",
			"2006-01-02 15:04:05 +0000 UTC",
		}

		parsed := false
		for _, format := range timeFormats {
			t, err = time.Parse(format, *timeStr)
			if err == nil {
				parsed = true
				break
			}
		}

		if !parsed {
			continue
		}
		u.Time = t

		u.DevicePK = extractStringFromRow(row, "dzd_pubkey")
		u.Host = extractStringFromRow(row, "host")
		u.Intf = extractStringFromRow(row, "intf")
		u.ModelName = extractStringFromRow(row, "model_name")
		u.SerialNumber = extractStringFromRow(row, "serial_number")

		if u.Intf != nil {
			u.UserTunnelID = extractTunnelIDFromInterface(*u.Intf)
		}

		var key string
		if u.DevicePK != nil && u.Intf != nil {
			key = fmt.Sprintf("%s:%s", *u.DevicePK, *u.Intf)
		}

		if key != "" && lastKnownValues[key] == nil {
			lastKnownValues[key] = make(map[string]*int64)
			if baselines != nil {
				if val := baselines.InDiscards[key]; val != nil {
					lastKnownValues[key]["in-discards"] = val
				}
				if val := baselines.InErrors[key]; val != nil {
					lastKnownValues[key]["in-errors"] = val
				}
				if val := baselines.InFCSErrors[key]; val != nil {
					lastKnownValues[key]["in-fcs-errors"] = val
				}
				if val := baselines.OutDiscards[key]; val != nil {
					lastKnownValues[key]["out-discards"] = val
				}
				if val := baselines.OutErrors[key]; val != nil {
					lastKnownValues[key]["out-errors"] = val
				}
			}
		}

		if key != "" && alreadyWritten != nil {
			if maxTS, exists := alreadyWritten[key]; exists {
				if !t.After(maxTS) {
					for _, field := range counterFieldNames {
						value := extractInt64FromRow(row, field)
						if value != nil {
							lastKnownValues[key][field] = value
						}
					}
					lastTime[key] = t
					firstRowSeen[key] = true
					continue
				}
			}
		}

		if key != "" {
			if linkInfo, ok := linkLookup[key]; ok {
				u.LinkPK = &linkInfo.LinkPK
				u.LinkSide = &linkInfo.LinkSide
			}
		}

		isFirstRow := key != "" && !firstRowSeen[key]

		allCounterFields := []struct {
			field     string
			dest      **int64
			deltaDest **int64
			baseline  map[string]*int64
			isSparse  bool
		}{
			{"carrier-transitions", &u.CarrierTransitions, &u.CarrierTransitionsDelta, nil, false},
			{"in-broadcast-pkts", &u.InBroadcastPkts, &u.InBroadcastPktsDelta, nil, false},
			{"in-discards", &u.InDiscards, &u.InDiscardsDelta, baselines.InDiscards, true},
			{"in-errors", &u.InErrors, &u.InErrorsDelta, baselines.InErrors, true},
			{"in-fcs-errors", &u.InFCSErrors, &u.InFCSErrorsDelta, baselines.InFCSErrors, true},
			{"in-multicast-pkts", &u.InMulticastPkts, &u.InMulticastPktsDelta, nil, false},
			{"in-octets", &u.InOctets, &u.InOctetsDelta, nil, false},
			{"in-pkts", &u.InPkts, &u.InPktsDelta, nil, false},
			{"in-unicast-pkts", &u.InUnicastPkts, &u.InUnicastPktsDelta, nil, false},
			{"out-broadcast-pkts", &u.OutBroadcastPkts, &u.OutBroadcastPktsDelta, nil, false},
			{"out-discards", &u.OutDiscards, &u.OutDiscardsDelta, baselines.OutDiscards, true},
			{"out-errors", &u.OutErrors, &u.OutErrorsDelta, baselines.OutErrors, true},
			{"out-multicast-pkts", &u.OutMulticastPkts, &u.OutMulticastPktsDelta, nil, false},
			{"out-octets", &u.OutOctets, &u.OutOctetsDelta, nil, false},
			{"out-pkts", &u.OutPkts, &u.OutPktsDelta, nil, false},
			{"out-unicast-pkts", &u.OutUnicastPkts, &u.OutUnicastPktsDelta, nil, false},
		}

		if isFirstRow {
			hasNonSparseValues := false
			for _, cf := range allCounterFields {
				if !cf.isSparse {
					if extractInt64FromRow(row, cf.field) != nil {
						hasNonSparseValues = true
						break
					}
				}
			}

			if hasNonSparseValues {
				for _, cf := range allCounterFields {
					value := extractInt64FromRow(row, cf.field)
					if value != nil && key != "" {
						lastKnownValues[key][cf.field] = value
					}
				}
				lastTime[key] = t
				firstRowSeen[key] = true
				continue
			}
			firstRowSeen[key] = true
		}

		for _, cf := range allCounterFields {
			var currentValue *int64
			value := extractInt64FromRow(row, cf.field)
			if value != nil {
				currentValue = value
			} else if key != "" {
				if lastKnown, ok := lastKnownValues[key][cf.field]; ok && lastKnown != nil {
					currentValue = lastKnown
				}
			}

			*cf.dest = currentValue

			if currentValue != nil && key != "" {
				var previousValue *int64
				if lastKnown, ok := lastKnownValues[key][cf.field]; ok && lastKnown != nil {
					previousValue = lastKnown
				}

				if previousValue != nil {
					delta := *currentValue - *previousValue
					*cf.deltaDest = &delta
				}

				lastKnownValues[key][cf.field] = currentValue
			}
		}

		if key != "" {
			if lastT, ok := lastTime[key]; ok {
				duration := t.Sub(lastT).Seconds()
				u.DeltaDuration = &duration
			}
			lastTime[key] = t
		}

		usage = append(usage, *u)
	}

	return usage, nil
}
