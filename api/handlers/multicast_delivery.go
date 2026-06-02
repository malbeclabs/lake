package handlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

// GetMulticastDeliveryState returns observed multicast forwarding state plus semantic enrichment.
func (a *API) GetMulticastDeliveryState(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	pkOrCode := chi.URLParam(r, "pk")
	if pkOrCode == "" {
		http.Error(w, "missing multicast group pk", http.StatusBadRequest)
		return
	}

	params, err := parseMulticastDeliveryParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := a.FetchMulticastDeliveryStateData(ctx, pkOrCode, params)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "multicast group not found", http.StatusNotFound)
			return
		}
		logError("multicast delivery-state query error", "error", err, "pk", pkOrCode)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	writeJSON(w, resp)
}

func (a *API) FetchMulticastDeliveryStateData(ctx context.Context, pkOrCode string, params MulticastDeliveryParams) (*MulticastDeliveryStateResponse, error) {
	now := time.Now().UTC()
	group, err := a.queryMulticastDeliveryGroup(ctx, pkOrCode)
	if err != nil {
		return nil, err
	}

	available, err := a.queryMulticastDeliverySources(ctx)
	if err != nil {
		return nil, err
	}
	sourceTimes, err := a.queryMulticastDeliverySourceIngestTimes(ctx)
	if err != nil {
		return nil, err
	}

	resp := &MulticastDeliveryStateResponse{
		Group:              group,
		SourceAvailable:    available["dz_ip_mroute_entries_current"],
		GeneratedAt:        formatMulticastTime(now),
		Mode:               params.Mode,
		Publishers:         []MulticastDeliveryMember{},
		Subscribers:        []MulticastDeliveryMember{},
		Routes:             []MulticastDeliveryRoute{},
		OIFs:               []MulticastDeliveryOIF{},
		ObservedSegments:   []MulticastDeliverySegment{},
		ExpectedSegments:   []MulticastDeliverySegment{},
		SubscriberOutcomes: []MulticastDeliverySubscriberState{},
		Anomalies:          []MulticastDeliveryAnomaly{},
		MSDP: MulticastDeliveryMSDP{
			Peers:        []MulticastDeliveryMSDPPeer{},
			PimSACache:   []MulticastDeliveryMSDPSA{},
			SACache:      []MulticastDeliveryMSDPSA{},
			PIMNeighbors: []MulticastDeliveryPIMNeighbor{},
		},
	}

	members, err := a.queryMulticastDeliveryMembers(ctx, group.PK)
	if err != nil {
		return nil, err
	}
	resp.Publishers, resp.Subscribers = splitMulticastDeliveryMembers(members, params)

	if !available["dz_ip_mroute_entries_current"] {
		resp.Freshness = buildMulticastFreshness(now, available, sourceTimes, nil, nil, nil, nil)
		resp.Anomalies = append(resp.Anomalies, multicastAnomaly(
			"mroute-source-unavailable", "warning", "source_unavailable", "group",
			map[string]string{"group_pk": group.PK},
			"mroute forwarding state source is unavailable for this environment",
		))
		return resp, nil
	}

	routes, routeTimes, err := a.queryMulticastDeliveryRoutes(ctx, group, params)
	if err != nil {
		if multicastDeliverySourceErr(err) {
			available["dz_ip_mroute_entries_current"] = false
			resp.SourceAvailable = false
			resp.Freshness = buildMulticastFreshness(now, available, sourceTimes, nil, nil, nil, nil)
			return resp, nil
		}
		return nil, err
	}
	resp.Routes = routes

	oifs, _, err := a.queryMulticastDeliveryOIFs(ctx, group, params)
	if err != nil {
		if multicastDeliverySourceErr(err) {
			available["dz_ip_mroute_entries_current"] = false
			resp.SourceAvailable = false
			resp.Freshness = buildMulticastFreshness(now, available, sourceTimes, nil, nil, nil, nil)
			return resp, nil
		}
		return nil, err
	}
	resp.OIFs = oifs

	var msdpPeerTimes, pimSATimes, saTimes []time.Time
	if available["dz_ip_msdp_peers_current"] {
		resp.MSDP.Peers, msdpPeerTimes, err = a.queryMulticastDeliveryMSDPPeers(ctx, params, routes)
		if err != nil {
			if !multicastDeliverySourceErr(err) {
				return nil, err
			}
			available["dz_ip_msdp_peers_current"] = false
		}
	}
	if available["dz_ip_msdp_pim_sa_cache_current"] {
		resp.MSDP.PimSACache, pimSATimes, err = a.queryMulticastDeliveryPimSACache(ctx, group, params)
		if err != nil {
			if !multicastDeliverySourceErr(err) {
				return nil, err
			}
			available["dz_ip_msdp_pim_sa_cache_current"] = false
		}
	}
	if available["dz_ip_msdp_sa_cache_current"] {
		resp.MSDP.SACache, saTimes, err = a.queryMulticastDeliverySACache(ctx, group, params)
		if err != nil {
			if !multicastDeliverySourceErr(err) {
				return nil, err
			}
			available["dz_ip_msdp_sa_cache_current"] = false
		}
	}

	resp.Freshness = buildMulticastFreshness(now, available, sourceTimes, routeTimes, msdpPeerTimes, pimSATimes, saTimes)
	applyMulticastFreshnessToRows(resp)
	resp.ObservedSegments = buildObservedMulticastSegments(oifs)
	if params.Mode == "expected" || params.Mode == "diff" || params.Mode == "all" {
		resp.ExpectedSegments, _ = a.buildExpectedMulticastDeliverySegments(ctx, group.PK, params)
	}
	resp.SubscriberOutcomes = buildMulticastSubscriberOutcomes(resp.Subscribers, resp.ObservedSegments, resp.ExpectedSegments)
	resp.Anomalies = buildMulticastDeliveryAnomalies(group, routes, oifs, resp.Freshness, resp.SubscriberOutcomes)

	return resp, nil
}

func parseMulticastDeliveryParams(r *http.Request) (MulticastDeliveryParams, error) {
	q := r.URL.Query()
	mode := strings.TrimSpace(q.Get("mode"))
	if mode == "" {
		mode = "all"
	}
	if mode != "observed" && mode != "expected" && mode != "diff" && mode != "all" {
		return MulticastDeliveryParams{}, fmt.Errorf("invalid mode")
	}
	return MulticastDeliveryParams{
		Mode:       mode,
		Sources:    splitCSVParam(q.Get("source")),
		Publishers: splitCSVParam(firstNonEmpty(q.Get("publisher"), q.Get("publishers"))),
		Devices:    splitCSVParam(q.Get("device")),
		Links:      splitCSVParam(q.Get("link")),
		Includes:   csvSet(q.Get("include")),
	}, nil
}

func (a *API) queryMulticastDeliveryGroup(ctx context.Context, pkOrCode string) (MulticastDeliveryGroup, error) {
	query := `
		SELECT
			pk,
			COALESCE(code, '') AS code,
			COALESCE(multicast_ip, '') AS multicast_ip,
			COALESCE(max_bandwidth, 0) AS max_bandwidth,
			COALESCE(status, '') AS status,
			COALESCE(publisher_count, 0) AS publisher_count,
			COALESCE(subscriber_count, 0) AS subscriber_count
		FROM dz_multicast_groups_current
		WHERE pk = ? OR code = ?
		LIMIT 1
		SETTINGS max_execution_time = 10, timeout_before_checking_execution_speed = 0
	`
	var group MulticastDeliveryGroup
	start := time.Now()
	err := a.envDB(ctx).QueryRow(ctx, query, pkOrCode, pkOrCode).Scan(
		&group.PK,
		&group.Code,
		&group.MulticastIP,
		&group.MaxBandwidth,
		&group.Status,
		&group.PublisherCount,
		&group.SubscriberCount,
	)
	metrics.RecordClickHouseQuery("multicast_delivery_group", time.Since(start), err)
	return group, err
}

func (a *API) queryMulticastDeliverySources(ctx context.Context) (map[string]bool, error) {
	names := []string{
		"dz_ip_mroute_entries_current",
		"dz_ip_msdp_peers_current",
		"dz_ip_msdp_pim_sa_cache_current",
		"dz_ip_msdp_sa_cache_current",
	}
	available := make(map[string]bool, len(names))
	query := `
		SELECT name
		FROM system.tables
		WHERE database = currentDatabase()
			AND name IN (?)
		SETTINGS max_execution_time = 5, timeout_before_checking_execution_speed = 0
	`
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, names)
	metrics.RecordClickHouseQuery("multicast_delivery_sources", time.Since(start), err)
	if err != nil {
		return available, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return available, err
		}
		available[name] = true
	}
	return available, rows.Err()
}

func (a *API) queryMulticastDeliverySourceIngestTimes(ctx context.Context) (map[string]time.Time, error) {
	sourceTimes := map[string]time.Time{}
	query := `
		SELECT activity, max(finished_at) AS latest_finished_at
		FROM log_ingestion_runs
		WHERE network = ?
			AND status = 'success'
			AND activity IN (?)
		GROUP BY activity
		SETTINGS max_execution_time = 5, timeout_before_checking_execution_speed = 0
	`
	activities := []string{"SyncIPMroute", "SyncMSDP"}
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, string(EnvFromContext(ctx)), activities)
	metrics.RecordClickHouseQuery("multicast_delivery_source_ingest_times", time.Since(start), err)
	if err != nil {
		if multicastDeliverySourceErr(err) {
			return sourceTimes, nil
		}
		return sourceTimes, err
	}
	defer rows.Close()
	for rows.Next() {
		var activity string
		var latest time.Time
		if err := rows.Scan(&activity, &latest); err != nil {
			return sourceTimes, err
		}
		switch activity {
		case "SyncIPMroute":
			sourceTimes["dz_ip_mroute_entries_current"] = latest
		case "SyncMSDP":
			sourceTimes["dz_ip_msdp_peers_current"] = latest
			sourceTimes["dz_ip_msdp_pim_sa_cache_current"] = latest
			sourceTimes["dz_ip_msdp_sa_cache_current"] = latest
		}
	}
	return sourceTimes, rows.Err()
}

func (a *API) queryMulticastDeliveryMembers(ctx context.Context, groupPK string) ([]MulticastDeliveryMember, error) {
	query := `
		SELECT
			u.pk AS user_pk,
			CASE
				WHEN has(JSONExtract(u.publishers, 'Array(String)'), ?) AND has(JSONExtract(u.subscribers, 'Array(String)'), ?) THEN 'P+S'
				WHEN has(JSONExtract(u.publishers, 'Array(String)'), ?) THEN 'P'
				ELSE 'S'
			END AS mode,
			u.device_pk AS device_pk,
			d.code AS device_code,
			d.metro_pk AS metro_pk,
			m.code AS metro_code,
			u.client_ip AS client_ip,
			u.dz_ip AS dz_ip,
			u.owner_pubkey AS owner_pubkey,
			u.tunnel_id AS tunnel_id
		FROM dz_users_current u
		LEFT ANY JOIN dz_devices_current d ON u.device_pk = d.pk
		LEFT ANY JOIN dz_metros_current m ON d.metro_pk = m.pk
		WHERE u.status = 'activated'
			AND u.kind = 'multicast'
			AND (
				has(JSONExtract(u.publishers, 'Array(String)'), ?)
				OR has(JSONExtract(u.subscribers, 'Array(String)'), ?)
			)
		ORDER BY mode, user_pk
		LIMIT ?
		SETTINGS max_execution_time = 15,
			max_result_rows = 10000,
			result_overflow_mode = 'break',
			timeout_before_checking_execution_speed = 0
	`
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, groupPK, groupPK, groupPK, groupPK, groupPK, multicastDeliveryMaxMemberRows)
	metrics.RecordClickHouseQuery("multicast_delivery_members", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	members := []MulticastDeliveryMember{}
	for rows.Next() {
		var m MulticastDeliveryMember
		if err := rows.Scan(
			&m.UserPK,
			&m.Mode,
			&m.DevicePK,
			&m.DeviceCode,
			&m.MetroPK,
			&m.MetroCode,
			&m.ClientIP,
			&m.DZIP,
			&m.OwnerPubkey,
			&m.TunnelID,
		); err != nil {
			return nil, err
		}
		members = append(members, m)
	}
	return members, rows.Err()
}
