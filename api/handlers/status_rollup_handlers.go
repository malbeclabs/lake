package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/malbeclabs/lake/api/health"
	"golang.org/x/sync/errgroup"
)

// fetchLinkHistoryFromRollup performs the link history data fetch using rollup tables.
// filters is optional server-side search filtering.
func (a *API) fetchLinkHistoryFromRollup(ctx context.Context, timeRange string, requestedBuckets int, filters ...statusFilter) (*LinkHistoryResponse, error) {
	start := time.Now()
	db := a.envDB(ctx)

	params := parseBucketParams(timeRange, requestedBuckets)
	params.UseRaw = isRawSource(ctx)
	bucketDuration := time.Duration(params.BucketMinutes) * time.Minute
	now := time.Now().UTC()

	var (
		linkMeta      map[string]*statusLinkMeta
		linkRollupMap map[linkBucketKey]*linkRollupRow
		intfRows      []interfaceRollupRow
	)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(5)

	// 1. Fetch link metadata
	g.Go(func() error {
		var err error
		linkMeta, err = queryStatusLinkMeta(ctx, db)
		if err != nil {
			return fmt.Errorf("link metadata: %w", err)
		}
		return nil
	})

	// 2. Fetch link rollup data (latency/loss per direction)
	g.Go(func() error {
		var err error
		linkRollupMap, err = queryLinkRollup(ctx, db, params)
		if err != nil {
			return fmt.Errorf("link rollup: %w", err)
		}
		return nil
	})

	// 3. Fetch interface rollup data grouped by (link_pk, link_side)
	g.Go(func() error {
		var err error
		intfRows, err = queryInterfaceRollup(ctx, db, params, interfaceRollupOpts{
			GroupBy: groupByLinkSide,
		})
		if err != nil {
			return fmt.Errorf("interface rollup: %w", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Index interface data by (link_pk, bucket) → side → counters/traffic
	type sideCounters struct {
		inErrors           uint64
		outErrors          uint64
		inFcsErrors        uint64
		inDiscards         uint64
		outDiscards        uint64
		carrierTransitions uint64
		inBps              float64
		outBps             float64
	}
	type intfBucketKey struct {
		linkPK   string
		bucketTS time.Time
	}
	intfIndex := make(map[intfBucketKey]map[string]*sideCounters)
	for _, row := range intfRows {
		ibk := intfBucketKey{linkPK: row.LinkPK, bucketTS: row.BucketTS}
		if intfIndex[ibk] == nil {
			intfIndex[ibk] = make(map[string]*sideCounters)
		}
		intfIndex[ibk][row.LinkSide] = &sideCounters{
			inErrors:           row.InErrors,
			outErrors:          row.OutErrors,
			inFcsErrors:        row.InFcsErrors,
			inDiscards:         row.InDiscards,
			outDiscards:        row.OutDiscards,
			carrierTransitions: row.CarrierTransitions,
			inBps:              row.AvgInBps,
			outBps:             row.AvgOutBps,
		}
	}

	// Apply search filters to metadata
	if len(filters) > 0 {
		for pk, meta := range linkMeta {
			if !linkMatchesFilters(meta, filters) {
				delete(linkMeta, pk)
			}
		}
	}

	// Build per-link history
	var links []LinkHistory
	for pk, meta := range linkMeta {
		issueReasons := make(map[string]bool)
		currentDrainStatus := ""
		if health.IsDrainedStatus(meta.Status) {
			currentDrainStatus = meta.Status
		}
		isProvisioning := meta.CommittedRttNs == committedRttProvisioningNs

		var hourStatuses []LinkHourStatus
		for i := params.BucketCount - 1; i >= 0; i-- {
			bucketStart := now.Truncate(bucketDuration).Add(-time.Duration(i) * bucketDuration)
			key := bucketStart.Format(time.RFC3339)
			isCollecting := i == 0

			bk := linkBucketKey{LinkPK: pk, BucketTS: bucketStart}
			rollup := linkRollupMap[bk]

			// Drain status from rollup (baked in at write time).
			// Use WasDrained to catch drains that started and ended within the bucket.
			drainStatus := ""
			if rollup != nil && (health.IsDrainedStatus(rollup.Status) || rollup.WasDrained) {
				if health.IsDrainedStatus(rollup.Status) {
					drainStatus = rollup.Status
				} else {
					drainStatus = "soft-drained" // was drained at some point in this bucket
				}
			} else if rollup == nil && currentDrainStatus != "" {
				drainStatus = currentDrainStatus
			}

			if rollup != nil && (rollup.ASamples > 0 || rollup.ZSamples > 0) {
				// Compute combined avg latency (sample-weighted across both directions)
				avgLatency := float64(0)
				lossPct := float64(0)
				totalSamples := rollup.ASamples + rollup.ZSamples
				if totalSamples > 0 {
					avgLatency = (rollup.AAvgRttUs*float64(rollup.ASamples) + rollup.ZAvgRttUs*float64(rollup.ZSamples)) / float64(totalSamples)
				}
				if rollup.ALossPct > lossPct {
					lossPct = rollup.ALossPct
				}
				if rollup.ZLossPct > lossPct {
					lossPct = rollup.ZLossPct
				}

				// Classify
				committedRtt := meta.CommittedRttUs
				if meta.LinkType != "WAN" || meta.SideAMetro == meta.SideZMetro {
					committedRtt = 0
				}
				status := health.ClassifyLinkStatus(avgLatency, lossPct, committedRtt)

				// One-sided reporting
				if drainStatus != "hard-drained" && (rollup.ASamples == 0) != (rollup.ZSamples == 0) {
					if isCollecting {
						status = "no_data"
					} else if status == "healthy" || status == "degraded" {
						status = "unhealthy"
					}
				}

				if lossPct >= health.LossWarningPct {
					issueReasons["packet_loss"] = true
				}
				if committedRtt > 0 && avgLatency > committedRtt*(1+health.LatencyWarningPct/100) {
					issueReasons["high_latency"] = true
				}

				hourStatus := LinkHourStatus{
					Hour:           key,
					Status:         status,
					Collecting:     isCollecting,
					DrainStatus:    drainStatus,
					AvgLatencyUs:   avgLatency,
					AvgLossPct:     lossPct,
					Samples:        totalSamples,
					SideALatencyUs: rollup.AAvgRttUs,
					SideALossPct:   rollup.ALossPct,
					SideASamples:   rollup.ASamples,
					SideZLatencyUs: rollup.ZAvgRttUs,
					SideZLossPct:   rollup.ZLossPct,
					SideZSamples:   rollup.ZSamples,
					ISISDown:       rollup.ISISDown,
				}

				// Add interface counters per side
				ibk := intfBucketKey{linkPK: pk, bucketTS: bucketStart}
				hasErrors := false
				hasDiscards := false
				hasCarrier := false
				if sides, ok := intfIndex[ibk]; ok {
					if a, ok := sides["A"]; ok {
						hourStatus.SideAInErrors = a.inErrors
						hourStatus.SideAOutErrors = a.outErrors
						hourStatus.SideAInFcsErrors = a.inFcsErrors
						hourStatus.SideAInDiscards = a.inDiscards
						hourStatus.SideAOutDiscards = a.outDiscards
						hourStatus.SideACarrierTransitions = a.carrierTransitions
						if a.inErrors > 0 || a.outErrors > 0 {
							issueReasons["interface_errors"] = true
							hasErrors = true
						}
						if a.inFcsErrors > 0 {
							issueReasons["fcs_errors"] = true
							hasErrors = true
						}
						if a.inDiscards > 0 || a.outDiscards > 0 {
							issueReasons["discards"] = true
							hasDiscards = true
						}
						if a.carrierTransitions > 0 {
							issueReasons["carrier_transitions"] = true
							hasCarrier = true
						}
					}
					if z, ok := sides["Z"]; ok {
						hourStatus.SideZInErrors = z.inErrors
						hourStatus.SideZOutErrors = z.outErrors
						hourStatus.SideZInFcsErrors = z.inFcsErrors
						hourStatus.SideZInDiscards = z.inDiscards
						hourStatus.SideZOutDiscards = z.outDiscards
						hourStatus.SideZCarrierTransitions = z.carrierTransitions
						if z.inErrors > 0 || z.outErrors > 0 {
							issueReasons["interface_errors"] = true
							hasErrors = true
						}
						if z.inFcsErrors > 0 {
							issueReasons["fcs_errors"] = true
							hasErrors = true
						}
						if z.inDiscards > 0 || z.outDiscards > 0 {
							issueReasons["discards"] = true
							hasDiscards = true
						}
						if z.carrierTransitions > 0 {
							issueReasons["carrier_transitions"] = true
							hasCarrier = true
						}
					}
				}

				// Upgrade status based on interface issues
				const InterfaceUnhealthyThreshold = uint64(100)
				totalErrors := hourStatus.SideAInErrors + hourStatus.SideAOutErrors + hourStatus.SideZInErrors + hourStatus.SideZOutErrors
				totalDiscards := hourStatus.SideAInDiscards + hourStatus.SideAOutDiscards + hourStatus.SideZInDiscards + hourStatus.SideZOutDiscards
				totalCarrier := hourStatus.SideACarrierTransitions + hourStatus.SideZCarrierTransitions

				if totalErrors >= InterfaceUnhealthyThreshold || totalDiscards >= InterfaceUnhealthyThreshold || totalCarrier >= InterfaceUnhealthyThreshold {
					if hourStatus.Status == "healthy" || hourStatus.Status == "degraded" {
						hourStatus.Status = "unhealthy"
					}
				} else if (hasErrors || hasDiscards || hasCarrier) && hourStatus.Status == "healthy" {
					hourStatus.Status = "degraded"
				}

				// Utilization
				if meta.BandwidthBps > 0 {
					if sides, ok := intfIndex[ibk]; ok {
						var totalInBps, totalOutBps float64
						for _, side := range sides {
							totalInBps += side.inBps
							totalOutBps += side.outBps
						}
						hourStatus.UtilizationInPct = (totalInBps / float64(meta.BandwidthBps)) * 100
						hourStatus.UtilizationOutPct = (totalOutBps / float64(meta.BandwidthBps)) * 100
						const HighUtilizationThreshold = 80.0
						if hourStatus.UtilizationInPct > HighUtilizationThreshold || hourStatus.UtilizationOutPct > HighUtilizationThreshold {
							issueReasons["high_utilization"] = true
							if hourStatus.Status == "healthy" {
								hourStatus.Status = "degraded"
							}
						}
					}
				}

				if rollup.ISISDown {
					issueReasons["missing_adjacency"] = true
				}

				hourStatuses = append(hourStatuses, hourStatus)
			} else {
				// No data for this bucket
				isisDown := false
				if rollup != nil {
					isisDown = rollup.ISISDown
				}
				hourStatuses = append(hourStatuses, LinkHourStatus{
					Hour:        key,
					Status:      "no_data",
					Collecting:  isCollecting,
					DrainStatus: drainStatus,
					ISISDown:    isisDown,
				})
			}
		}

		// Check for no_data buckets
		for _, h := range hourStatuses {
			if h.Collecting {
				continue
			}
			if h.Status == "no_data" {
				issueReasons["no_data"] = true
				break
			}
			if h.Samples > 0 && (h.SideASamples == 0 || h.SideZSamples == 0) {
				issueReasons["no_data"] = true
				break
			}
		}

		// Determine if link is down (high loss in recent data or ISIS down)
		isDown := false
		// Check most recent non-collecting bucket for very high loss
		for i := len(hourStatuses) - 1; i >= 0; i-- {
			h := hourStatuses[i]
			if h.Collecting {
				continue
			}
			if h.AvgLossPct >= 95 && h.Samples > 0 {
				isDown = true
			}
			break // only check the most recent completed bucket
		}
		// ISIS down also means the link is down
		if rollupData, exists := linkRollupMap[linkBucketKey{LinkPK: pk, BucketTS: now.Truncate(bucketDuration)}]; exists && rollupData.ISISDown {
			isDown = true
		}
		if currentDrainStatus != "" || isProvisioning {
			isDown = false
		}

		var issueReasonsList []string
		for reason := range issueReasons {
			issueReasonsList = append(issueReasonsList, reason)
		}
		sort.Strings(issueReasonsList)

		responseCommittedRtt := meta.CommittedRttUs
		if meta.LinkType != "WAN" || meta.SideAMetro == meta.SideZMetro {
			responseCommittedRtt = 0
		}

		links = append(links, LinkHistory{
			PK:             pk,
			Code:           meta.Code,
			LinkType:       meta.LinkType,
			Contributor:    meta.Contributor,
			SideAMetro:     meta.SideAMetro,
			SideZMetro:     meta.SideZMetro,
			SideADevice:    meta.SideADevice,
			SideZDevice:    meta.SideZDevice,
			BandwidthBps:   meta.BandwidthBps,
			CommittedRttUs: responseCommittedRtt,
			IsDown:         isDown,
			DrainStatus:    currentDrainStatus,
			Provisioning:   isProvisioning,
			Hours:          hourStatuses,
			IssueReasons:   issueReasonsList,
		})
	}

	sort.Slice(links, func(i, j int) bool {
		return links[i].Code < links[j].Code
	})

	resp := &LinkHistoryResponse{
		Links:         links,
		TimeRange:     params.TimeRange,
		BucketMinutes: params.BucketMinutes,
		BucketCount:   params.BucketCount,
	}

	slog.Info("fetchLinkHistoryFromRollup completed", "duration", time.Since(start), "range", params.TimeRange, "buckets", params.BucketCount, "links", len(links))
	return resp, nil
}

// fetchDeviceHistoryFromRollup performs the device history data fetch using rollup tables.
func (a *API) fetchDeviceHistoryFromRollup(ctx context.Context, timeRange string, requestedBuckets int, filters ...statusFilter) (*DeviceHistoryResponse, error) {
	start := time.Now()
	db := a.envDB(ctx)

	params := parseBucketParams(timeRange, requestedBuckets)
	params.UseRaw = isRawSource(ctx)
	bucketDuration := time.Duration(params.BucketMinutes) * time.Minute
	now := time.Now().UTC()

	var (
		deviceMeta map[string]*statusDeviceMeta
		intfRows   []interfaceRollupRow
		// Track which devices have probes (appear in link rollup)
		deviceHasProbes map[string]bool
	)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(5)

	// 1. Fetch device metadata
	g.Go(func() error {
		var err error
		deviceMeta, err = queryStatusDeviceMeta(ctx, db)
		if err != nil {
			return fmt.Errorf("device metadata: %w", err)
		}
		return nil
	})

	// 2. Fetch interface rollup data grouped by device
	g.Go(func() error {
		var err error
		intfRows, err = queryInterfaceRollup(ctx, db, params, interfaceRollupOpts{
			GroupBy: groupByDevice,
		})
		if err != nil {
			return fmt.Errorf("device interface rollup: %w", err)
		}
		return nil
	})

	// 3. Check probe presence: devices connected to links with rollup data have probes
	g.Go(func() error {
		linkRollup, err := queryLinkRollup(ctx, db, params)
		if err != nil {
			return fmt.Errorf("link rollup for probes: %w", err)
		}
		linkMeta, err := queryStatusLinkMeta(ctx, db)
		if err != nil {
			return fmt.Errorf("link metadata for probes: %w", err)
		}
		// Build set of link PKs that have probe data
		linksWithData := make(map[string]bool)
		for bk := range linkRollup {
			linksWithData[bk.LinkPK] = true
		}
		// Map back to devices
		deviceHasProbes = make(map[string]bool)
		for _, meta := range linkMeta {
			if linksWithData[meta.PK] {
				deviceHasProbes[meta.SideADevicePK] = true
				deviceHasProbes[meta.SideZDevicePK] = true
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Apply search filters to metadata
	if len(filters) > 0 {
		for pk, meta := range deviceMeta {
			if !deviceMatchesFilters(meta, filters) {
				delete(deviceMeta, pk)
			}
		}
	}

	// Index interface data by (device_pk, bucket_ts)
	type deviceBucketKey struct {
		devicePK string
		bucketTS time.Time
	}
	intfIndex := make(map[deviceBucketKey]*interfaceRollupRow)
	for i := range intfRows {
		dbk := deviceBucketKey{devicePK: intfRows[i].DevicePK, bucketTS: intfRows[i].BucketTS}
		intfIndex[dbk] = &intfRows[i]
	}

	// Build per-device history
	var devices []DeviceHistory
	for pk, meta := range deviceMeta {
		issueReasons := make(map[string]bool)
		isDrained := health.IsDrainedStatus(meta.Status)
		if isDrained {
			issueReasons["drained"] = true
		}

		var hourStatuses []DeviceHourStatus
		hasAnyData := false
		for i := params.BucketCount - 1; i >= 0; i-- {
			bucketStart := now.Truncate(bucketDuration).Add(-time.Duration(i) * bucketDuration)
			key := bucketStart.Format(time.RFC3339)
			isCollecting := i == 0

			dbk := deviceBucketKey{devicePK: pk, bucketTS: bucketStart}
			row := intfIndex[dbk]

			if row != nil {
				hasAnyData = true
				totalErrors := row.InErrors + row.OutErrors + row.InFcsErrors
				totalDiscards := row.InDiscards + row.OutDiscards
				status := health.ClassifyDeviceStatus(totalErrors, totalDiscards, row.CarrierTransitions)

				// Track issue reasons
				if row.InErrors > 0 || row.OutErrors > 0 {
					issueReasons["interface_errors"] = true
				}
				if row.InFcsErrors > 0 {
					issueReasons["fcs_errors"] = true
				}
				if row.InDiscards > 0 || row.OutDiscards > 0 {
					issueReasons["discards"] = true
				}
				if row.CarrierTransitions > 0 {
					issueReasons["carrier_transitions"] = true
				}

				// Drain status from rollup
				drainStatus := ""
				if health.IsDrainedStatus(row.Status) || row.WasDrained {
					if health.IsDrainedStatus(row.Status) {
						drainStatus = row.Status
					} else {
						drainStatus = "soft-drained"
					}
				}
				if drainStatus != "" {
					status = "disabled"
				}

				// ISIS state
				if row.ISISOverload {
					issueReasons["isis_overload"] = true
				}
				if row.ISISUnreachable {
					issueReasons["isis_unreachable"] = true
				}

				// Check probes
				noProbes := !deviceHasProbes[pk] && !isCollecting
				if noProbes {
					issueReasons["no_probes"] = true
				}

				hourStatuses = append(hourStatuses, DeviceHourStatus{
					Hour:               key,
					Status:             status,
					Collecting:         isCollecting,
					InErrors:           row.InErrors,
					OutErrors:          row.OutErrors,
					InFcsErrors:        row.InFcsErrors,
					InDiscards:         row.InDiscards,
					OutDiscards:        row.OutDiscards,
					CarrierTransitions: row.CarrierTransitions,
					DrainStatus:        drainStatus,
					NoProbes:           noProbes,
					ISISOverload:       row.ISISOverload,
					ISISUnreachable:    row.ISISUnreachable,
				})
			} else {
				drainStatus := ""
				if isDrained {
					drainStatus = meta.Status
				}
				status := "no_data"
				if drainStatus != "" {
					status = "disabled"
				}
				hourStatuses = append(hourStatuses, DeviceHourStatus{
					Hour:        key,
					Status:      status,
					Collecting:  isCollecting,
					DrainStatus: drainStatus,
				})
			}
		}

		if !hasAnyData && !isDrained {
			issueReasons["no_data"] = true
		}

		var issueReasonsList []string
		for reason := range issueReasons {
			issueReasonsList = append(issueReasonsList, reason)
		}
		sort.Strings(issueReasonsList)

		devices = append(devices, DeviceHistory{
			PK:           pk,
			Code:         meta.Code,
			DeviceType:   meta.DeviceType,
			Contributor:  meta.Contributor,
			Metro:        meta.Metro,
			MaxUsers:     meta.MaxUsers,
			Hours:        hourStatuses,
			IssueReasons: issueReasonsList,
		})
	}

	sort.Slice(devices, func(i, j int) bool {
		return devices[i].Code < devices[j].Code
	})

	resp := &DeviceHistoryResponse{
		Devices:       devices,
		TimeRange:     params.TimeRange,
		BucketMinutes: params.BucketMinutes,
		BucketCount:   params.BucketCount,
	}

	slog.Info("fetchDeviceHistoryFromRollup completed", "duration", time.Since(start), "range", params.TimeRange, "buckets", params.BucketCount, "devices", len(devices))
	return resp, nil
}

// fetchInterfaceIssuesFromRollup returns interface issues using rollup tables.
func (a *API) fetchInterfaceIssuesFromRollup(ctx context.Context, duration time.Duration) ([]InterfaceIssue, error) {
	db := a.envDB(ctx)
	hours := int(duration.Hours())
	if hours < 1 {
		hours = 1
	}

	// Query rollup for interfaces with errors, grouped by device+intf
	query := fmt.Sprintf(`
		SELECT
			r.device_pk,
			d.code as device_code,
			d.device_type,
			COALESCE(contrib.code, '') as contributor,
			COALESCE(m.code, '') as metro,
			r.intf as interface_name,
			COALESCE(l.pk, '') as link_pk,
			COALESCE(l.code, '') as link_code,
			COALESCE(l.link_type, '') as link_type,
			r.link_side,
			sum(r.in_errors) as in_errors,
			sum(r.out_errors) as out_errors,
			sum(r.in_fcs_errors) as in_fcs_errors,
			sum(r.in_discards) as in_discards,
			sum(r.out_discards) as out_discards,
			sum(r.carrier_transitions) as carrier_transitions,
			formatDateTime(min(r.bucket_ts), '%%Y-%%m-%%dT%%H:%%i:%%sZ', 'UTC') as first_seen,
			formatDateTime(max(r.bucket_ts), '%%Y-%%m-%%dT%%H:%%i:%%sZ', 'UTC') as last_seen
		FROM device_interface_rollup_5m r FINAL
		JOIN dz_devices_current d ON r.device_pk = d.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dz_contributors_current contrib ON d.contributor_pk = contrib.pk
		LEFT JOIN dz_links_current l ON r.link_pk = l.pk
		WHERE r.bucket_ts > now() - INTERVAL %d HOUR
		  AND d.status = 'activated'
		  AND (r.in_errors > 0 OR r.out_errors > 0 OR r.in_fcs_errors > 0 OR r.in_discards > 0 OR r.out_discards > 0 OR r.carrier_transitions > 0)
		GROUP BY r.device_pk, d.code, d.device_type, contrib.code, m.code, r.intf, l.pk, l.code, l.link_type, r.link_side
		ORDER BY (in_errors + out_errors + in_fcs_errors + in_discards + out_discards + carrier_transitions) DESC
		LIMIT 50
	`, hours)

	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var issues []InterfaceIssue
	for rows.Next() {
		var issue InterfaceIssue
		if err := rows.Scan(
			&issue.DevicePK, &issue.DeviceCode, &issue.DeviceType,
			&issue.Contributor, &issue.Metro, &issue.InterfaceName,
			&issue.LinkPK, &issue.LinkCode, &issue.LinkType, &issue.LinkSide,
			&issue.InErrors, &issue.OutErrors, &issue.InFcsErrors,
			&issue.InDiscards, &issue.OutDiscards, &issue.CarrierTransitions,
			&issue.FirstSeen, &issue.LastSeen,
		); err != nil {
			return nil, err
		}
		issues = append(issues, issue)
	}
	return issues, rows.Err()
}

// fetchDeviceInterfaceHistoryFromRollup returns per-interface history for a device using rollup tables.
func (a *API) fetchDeviceInterfaceHistoryFromRollup(ctx context.Context, devicePK string, timeRange string, requestedBuckets int) (*DeviceInterfaceHistoryResponse, error) {
	db := a.envDB(ctx)
	params := parseBucketParams(timeRange, requestedBuckets)
	params.UseRaw = isRawSource(ctx)
	bucketDuration := time.Duration(params.BucketMinutes) * time.Minute
	now := time.Now().UTC()

	intfRows, err := queryInterfaceRollup(ctx, db, params, interfaceRollupOpts{
		GroupBy:   groupByDeviceIntf,
		DevicePKs: []string{devicePK},
	})
	if err != nil {
		return nil, fmt.Errorf("device interface rollup: %w", err)
	}

	// Index by (intf, bucket_ts) and collect interface metadata
	type intfMeta struct {
		linkPK   string
		linkSide string
	}
	type intfBK struct {
		intf     string
		bucketTS time.Time
	}
	intfMetaMap := make(map[string]intfMeta)
	intfBucketMap := make(map[intfBK]*interfaceRollupRow)
	for i := range intfRows {
		row := &intfRows[i]
		intfMetaMap[row.Intf] = intfMeta{linkPK: row.LinkPK, linkSide: row.LinkSide}
		intfBucketMap[intfBK{intf: row.Intf, bucketTS: row.BucketTS}] = row
	}

	// Look up link codes from metadata
	var linkPKs []string
	for _, m := range intfMetaMap {
		if m.linkPK != "" {
			linkPKs = append(linkPKs, m.linkPK)
		}
	}
	linkMeta := make(map[string]*statusLinkMeta)
	if len(linkPKs) > 0 {
		linkMeta, err = queryStatusLinkMeta(ctx, db, linkPKs...)
		if err != nil {
			slog.Warn("failed to fetch link metadata for interface history", "error", err)
		}
	}

	var interfaces []InterfaceHistory
	for intfName, meta := range intfMetaMap {
		var hours []InterfaceHourStatus
		for i := params.BucketCount - 1; i >= 0; i-- {
			bucketStart := now.Truncate(bucketDuration).Add(-time.Duration(i) * bucketDuration)
			key := bucketStart.Format(time.RFC3339)

			if row, ok := intfBucketMap[intfBK{intf: intfName, bucketTS: bucketStart}]; ok {
				hours = append(hours, InterfaceHourStatus{
					Hour:               key,
					InErrors:           row.InErrors,
					OutErrors:          row.OutErrors,
					InFcsErrors:        row.InFcsErrors,
					InDiscards:         row.InDiscards,
					OutDiscards:        row.OutDiscards,
					CarrierTransitions: row.CarrierTransitions,
				})
			} else {
				hours = append(hours, InterfaceHourStatus{Hour: key})
			}
		}

		ih := InterfaceHistory{
			InterfaceName: intfName,
			LinkPK:        meta.linkPK,
			LinkSide:      meta.linkSide,
			Hours:         hours,
		}
		if lm, ok := linkMeta[meta.linkPK]; ok {
			ih.LinkCode = lm.Code
			ih.LinkType = lm.LinkType
		}
		interfaces = append(interfaces, ih)
	}

	sort.Slice(interfaces, func(i, j int) bool {
		return interfaces[i].InterfaceName < interfaces[j].InterfaceName
	})

	return &DeviceInterfaceHistoryResponse{
		Interfaces:    interfaces,
		TimeRange:     params.TimeRange,
		BucketMinutes: params.BucketMinutes,
		BucketCount:   params.BucketCount,
	}, nil
}

// fetchSingleLinkHistoryFromRollup returns the history for a single link using rollup tables.
func (a *API) fetchSingleLinkHistoryFromRollup(ctx context.Context, linkPK string, timeRange string, requestedBuckets int) (*SingleLinkHistoryResponse, error) {
	db := a.envDB(ctx)
	params := parseBucketParams(timeRange, requestedBuckets)
	params.UseRaw = isRawSource(ctx)
	bucketDuration := time.Duration(params.BucketMinutes) * time.Minute
	now := time.Now().UTC()

	var (
		meta          *statusLinkMeta
		linkRollupMap map[linkBucketKey]*linkRollupRow
		intfRows      []interfaceRollupRow
	)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		metas, err := queryStatusLinkMeta(ctx, db, linkPK)
		if err != nil {
			return err
		}
		meta = metas[linkPK]
		return nil
	})

	g.Go(func() error {
		var err error
		linkRollupMap, err = queryLinkRollup(ctx, db, params, linkPK)
		return err
	})

	g.Go(func() error {
		var err error
		intfRows, err = queryInterfaceRollup(ctx, db, params, interfaceRollupOpts{
			GroupBy: groupByLinkSide,
			LinkPKs: []string{linkPK},
		})
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	if meta == nil {
		return nil, fmt.Errorf("link not found: %s", linkPK)
	}

	// Index interface data by (bucket, side)
	type sideKey struct {
		bucketTS time.Time
		side     string
	}
	intfIndex := make(map[sideKey]*interfaceRollupRow)
	for i := range intfRows {
		sk := sideKey{bucketTS: intfRows[i].BucketTS, side: intfRows[i].LinkSide}
		intfIndex[sk] = &intfRows[i]
	}

	committedRtt := meta.CommittedRttUs
	if meta.LinkType != "WAN" || meta.SideAMetro == meta.SideZMetro {
		committedRtt = 0
	}

	var hours []LinkHourStatus
	for i := params.BucketCount - 1; i >= 0; i-- {
		bucketStart := now.Truncate(bucketDuration).Add(-time.Duration(i) * bucketDuration)
		key := bucketStart.Format(time.RFC3339)
		isCollecting := i == 0

		bk := linkBucketKey{LinkPK: linkPK, BucketTS: bucketStart}
		rollup := linkRollupMap[bk]

		drainStatus := ""
		if rollup != nil && (health.IsDrainedStatus(rollup.Status) || rollup.WasDrained) {
			if health.IsDrainedStatus(rollup.Status) {
				drainStatus = rollup.Status
			} else {
				drainStatus = "soft-drained"
			}
		}

		if rollup != nil && (rollup.ASamples > 0 || rollup.ZSamples > 0) {
			totalSamples := rollup.ASamples + rollup.ZSamples
			avgLatency := float64(0)
			if totalSamples > 0 {
				avgLatency = (rollup.AAvgRttUs*float64(rollup.ASamples) + rollup.ZAvgRttUs*float64(rollup.ZSamples)) / float64(totalSamples)
			}
			lossPct := rollup.ALossPct
			if rollup.ZLossPct > lossPct {
				lossPct = rollup.ZLossPct
			}

			status := health.ClassifyLinkStatus(avgLatency, lossPct, committedRtt)

			if drainStatus != "hard-drained" && (rollup.ASamples == 0) != (rollup.ZSamples == 0) {
				if isCollecting {
					status = "no_data"
				} else if status == "healthy" || status == "degraded" {
					status = "unhealthy"
				}
			}

			hs := LinkHourStatus{
				Hour:           key,
				Status:         status,
				Collecting:     isCollecting,
				DrainStatus:    drainStatus,
				AvgLatencyUs:   avgLatency,
				AvgLossPct:     lossPct,
				Samples:        totalSamples,
				SideALatencyUs: rollup.AAvgRttUs,
				SideALossPct:   rollup.ALossPct,
				SideASamples:   rollup.ASamples,
				SideZLatencyUs: rollup.ZAvgRttUs,
				SideZLossPct:   rollup.ZLossPct,
				SideZSamples:   rollup.ZSamples,
				ISISDown:       rollup.ISISDown,
			}

			// Interface counters per side
			if a, ok := intfIndex[sideKey{bucketTS: bucketStart, side: "A"}]; ok {
				hs.SideAInErrors = a.InErrors
				hs.SideAOutErrors = a.OutErrors
				hs.SideAInFcsErrors = a.InFcsErrors
				hs.SideAInDiscards = a.InDiscards
				hs.SideAOutDiscards = a.OutDiscards
				hs.SideACarrierTransitions = a.CarrierTransitions
			}
			if z, ok := intfIndex[sideKey{bucketTS: bucketStart, side: "Z"}]; ok {
				hs.SideZInErrors = z.InErrors
				hs.SideZOutErrors = z.OutErrors
				hs.SideZInFcsErrors = z.InFcsErrors
				hs.SideZInDiscards = z.InDiscards
				hs.SideZOutDiscards = z.OutDiscards
				hs.SideZCarrierTransitions = z.CarrierTransitions
			}

			// Utilization
			if meta.BandwidthBps > 0 {
				var totalInBps, totalOutBps float64
				if a, ok := intfIndex[sideKey{bucketTS: bucketStart, side: "A"}]; ok {
					totalInBps += a.AvgInBps
					totalOutBps += a.AvgOutBps
				}
				if z, ok := intfIndex[sideKey{bucketTS: bucketStart, side: "Z"}]; ok {
					totalInBps += z.AvgInBps
					totalOutBps += z.AvgOutBps
				}
				hs.UtilizationInPct = (totalInBps / float64(meta.BandwidthBps)) * 100
				hs.UtilizationOutPct = (totalOutBps / float64(meta.BandwidthBps)) * 100
			}

			hours = append(hours, hs)
		} else {
			isisDown := false
			if rollup != nil {
				isisDown = rollup.ISISDown
			}
			hours = append(hours, LinkHourStatus{
				Hour:        key,
				Status:      "no_data",
				Collecting:  isCollecting,
				DrainStatus: drainStatus,
				ISISDown:    isisDown,
			})
		}
	}

	return &SingleLinkHistoryResponse{
		PK:             meta.PK,
		Code:           meta.Code,
		CommittedRttUs: committedRtt,
		Hours:          hours,
		TimeRange:      params.TimeRange,
		BucketMinutes:  params.BucketMinutes,
		BucketCount:    params.BucketCount,
	}, nil
}

// fetchSingleDeviceHistoryFromRollup returns the history for a single device using rollup tables.
func (a *API) fetchSingleDeviceHistoryFromRollup(ctx context.Context, devicePK string, timeRange string, requestedBuckets int) (*SingleDeviceHistoryResponse, error) {
	db := a.envDB(ctx)
	params := parseBucketParams(timeRange, requestedBuckets)
	params.UseRaw = isRawSource(ctx)
	bucketDuration := time.Duration(params.BucketMinutes) * time.Minute
	now := time.Now().UTC()

	var (
		meta     *statusDeviceMeta
		intfRows []interfaceRollupRow
	)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		metas, err := queryStatusDeviceMeta(ctx, db, devicePK)
		if err != nil {
			return err
		}
		meta = metas[devicePK]
		return nil
	})

	g.Go(func() error {
		var err error
		intfRows, err = queryInterfaceRollup(ctx, db, params, interfaceRollupOpts{
			GroupBy:   groupByDevice,
			DevicePKs: []string{devicePK},
		})
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	if meta == nil {
		return nil, fmt.Errorf("device not found: %s", devicePK)
	}

	// Index by bucket
	type devBK struct {
		bucketTS time.Time
	}
	intfIndex := make(map[devBK]*interfaceRollupRow)
	for i := range intfRows {
		intfIndex[devBK{bucketTS: intfRows[i].BucketTS}] = &intfRows[i]
	}

	var hours []DeviceHourStatus
	for i := params.BucketCount - 1; i >= 0; i-- {
		bucketStart := now.Truncate(bucketDuration).Add(-time.Duration(i) * bucketDuration)
		key := bucketStart.Format(time.RFC3339)
		isCollecting := i == 0

		row := intfIndex[devBK{bucketTS: bucketStart}]
		if row != nil {
			totalErrors := row.InErrors + row.OutErrors + row.InFcsErrors
			totalDiscards := row.InDiscards + row.OutDiscards
			status := health.ClassifyDeviceStatus(totalErrors, totalDiscards, row.CarrierTransitions)

			drainStatus := ""
			if health.IsDrainedStatus(row.Status) {
				drainStatus = row.Status
				status = "disabled"
			}

			hours = append(hours, DeviceHourStatus{
				Hour:               key,
				Status:             status,
				Collecting:         isCollecting,
				InErrors:           row.InErrors,
				OutErrors:          row.OutErrors,
				InFcsErrors:        row.InFcsErrors,
				InDiscards:         row.InDiscards,
				OutDiscards:        row.OutDiscards,
				CarrierTransitions: row.CarrierTransitions,
				DrainStatus:        drainStatus,
				ISISOverload:       row.ISISOverload,
				ISISUnreachable:    row.ISISUnreachable,
			})
		} else {
			drainStatus := ""
			if health.IsDrainedStatus(meta.Status) {
				drainStatus = meta.Status
			}
			status := "no_data"
			if drainStatus != "" {
				status = "disabled"
			}
			hours = append(hours, DeviceHourStatus{
				Hour:        key,
				Status:      status,
				Collecting:  isCollecting,
				DrainStatus: drainStatus,
			})
		}
	}

	// Build issue reasons from the hours
	issueReasons := make(map[string]bool)
	hasAnyData := false
	for _, h := range hours {
		if h.InErrors > 0 || h.OutErrors > 0 {
			issueReasons["interface_errors"] = true
		}
		if h.InFcsErrors > 0 {
			issueReasons["fcs_errors"] = true
		}
		if h.InDiscards > 0 || h.OutDiscards > 0 {
			issueReasons["discards"] = true
		}
		if h.CarrierTransitions > 0 {
			issueReasons["carrier_transitions"] = true
		}
		if h.ISISOverload {
			issueReasons["isis_overload"] = true
		}
		if h.ISISUnreachable {
			issueReasons["isis_unreachable"] = true
		}
		if h.DrainStatus != "" {
			issueReasons["drained"] = true
		}
		if h.Status != "no_data" && h.Status != "disabled" {
			hasAnyData = true
		}
	}
	if !hasAnyData && !health.IsDrainedStatus(meta.Status) {
		issueReasons["no_data"] = true
	}
	var issueReasonsList []string
	for r := range issueReasons {
		issueReasonsList = append(issueReasonsList, r)
	}
	sort.Strings(issueReasonsList)

	return &SingleDeviceHistoryResponse{
		PK:            meta.PK,
		Code:          meta.Code,
		DeviceType:    meta.DeviceType,
		Contributor:   meta.Contributor,
		Metro:         meta.Metro,
		MaxUsers:      meta.MaxUsers,
		Hours:         hours,
		IssueReasons:  issueReasonsList,
		TimeRange:     params.TimeRange,
		BucketMinutes: params.BucketMinutes,
		BucketCount:   params.BucketCount,
	}, nil
}
