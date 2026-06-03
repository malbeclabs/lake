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

const (
	multicastDeliveryMrouteSource    = "dz_ip_mroute_entries_current"
	multicastDeliveryMSDPPeersSource = "dz_ip_msdp_peers_current"
	multicastDeliveryMSDPPimSASource = "dz_ip_msdp_pim_sa_cache_current"
	multicastDeliveryMSDPSASource    = "dz_ip_msdp_sa_cache_current"
	multicastDeliveryMrouteView      = "enriched_ip_mroute"
	multicastDeliveryOIFView         = "enriched_ip_mroute_oifs"
	multicastDeliveryMSDPPeersView   = "enriched_ip_msdp_peers"
	multicastDeliveryMSDPPimSAView   = "enriched_ip_msdp_pim_sa_cache"
	multicastDeliveryMSDPSAView      = "enriched_ip_msdp_sa_cache"
)

type multicastDeliveryRequestContext struct {
	Now         time.Time
	Group       MulticastDeliveryGroup
	Available   map[string]bool
	SourceTimes map[string]time.Time
}

// GetMulticastGroupMroutes returns paginated enriched PIM mroute rows for a multicast group.
func (a *API) GetMulticastGroupMroutes(w http.ResponseWriter, r *http.Request) {
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

	data, err := a.loadMulticastDeliveryRequestContext(ctx, pkOrCode)
	if err != nil {
		writeMulticastDeliveryError(w, err, "multicast mroutes query error", pkOrCode)
		return
	}

	mroutes := []MulticastDeliveryMroute{}
	var mrouteTimes []time.Time
	if data.Available[multicastDeliveryMrouteView] {
		mroutes, mrouteTimes, err = a.queryMulticastDeliveryMroutes(ctx, data.Group, params)
		if err != nil {
			if !multicastDeliverySourceErr(err) {
				writeMulticastDeliveryError(w, err, "multicast mroutes query error", pkOrCode)
				return
			}
			data.Available[multicastDeliveryMrouteView] = false
			mroutes = []MulticastDeliveryMroute{}
			mrouteTimes = nil
		}
	}

	freshness := buildMulticastFreshness(data.Now, data.Available, data.SourceTimes, mrouteTimes, nil, nil, nil)
	applyMrouteFreshness(freshness.Mroute, mroutes)
	items, total := paginateMulticastDeliveryItems(mroutes, params)

	writeJSON(w, MulticastDeliveryMroutesResponse{
		Group:           data.Group,
		SourceAvailable: data.Available[multicastDeliveryMrouteView],
		GeneratedAt:     formatMulticastTime(data.Now),
		Freshness:       freshness,
		Items:           items,
		Total:           total,
		Limit:           params.Limit,
		Offset:          params.Offset,
	})
}

// GetMulticastGroupOIFs returns paginated enriched mroute OIF rows for a multicast group.
func (a *API) GetMulticastGroupOIFs(w http.ResponseWriter, r *http.Request) {
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

	data, err := a.loadMulticastDeliveryRequestContext(ctx, pkOrCode)
	if err != nil {
		writeMulticastDeliveryError(w, err, "multicast oifs query error", pkOrCode)
		return
	}

	oifs := []MulticastDeliveryOIF{}
	var oifTimes []time.Time
	if data.Available[multicastDeliveryOIFView] {
		oifs, oifTimes, err = a.queryMulticastDeliveryOIFs(ctx, data.Group, params)
		if err != nil {
			if !multicastDeliverySourceErr(err) {
				writeMulticastDeliveryError(w, err, "multicast oifs query error", pkOrCode)
				return
			}
			data.Available[multicastDeliveryOIFView] = false
			oifs = []MulticastDeliveryOIF{}
			oifTimes = nil
		}
	}

	freshnessAvailable := data.Available
	if !data.Available[multicastDeliveryOIFView] {
		freshnessAvailable = copyMulticastDeliveryAvailability(data.Available)
		freshnessAvailable[multicastDeliveryMrouteView] = false
	}
	freshness := buildMulticastFreshness(data.Now, freshnessAvailable, data.SourceTimes, oifTimes, nil, nil, nil)
	applyOIFFreshness(freshness.Mroute, oifs)
	items, total := paginateMulticastDeliveryItems(oifs, params)

	writeJSON(w, MulticastDeliveryOIFsResponse{
		Group:           data.Group,
		SourceAvailable: data.Available[multicastDeliveryOIFView],
		GeneratedAt:     formatMulticastTime(data.Now),
		Freshness:       freshness,
		Items:           items,
		Total:           total,
		Limit:           params.Limit,
		Offset:          params.Offset,
	})
}

// GetMulticastGroupMSDP returns paginated MSDP rows related to a multicast group.
func (a *API) GetMulticastGroupMSDP(w http.ResponseWriter, r *http.Request) {
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

	data, err := a.loadMulticastDeliveryRequestContext(ctx, pkOrCode)
	if err != nil {
		writeMulticastDeliveryError(w, err, "multicast msdp query error", pkOrCode)
		return
	}

	var mroutes []MulticastDeliveryMroute
	var mrouteTimes, peerTimes, pimSATimes, saTimes []time.Time
	if shouldQueryMSDPPeers(params.MSDPKind) && data.Available[multicastDeliveryMrouteView] {
		mroutes, mrouteTimes, err = a.queryMulticastDeliveryMroutes(ctx, data.Group, params)
		if err != nil {
			if !multicastDeliverySourceErr(err) {
				writeMulticastDeliveryError(w, err, "multicast msdp mroute query error", pkOrCode)
				return
			}
			data.Available[multicastDeliveryMrouteView] = false
			mroutes = nil
			mrouteTimes = nil
		}
	}

	peers := []MulticastDeliveryMSDPPeer{}
	if shouldQueryMSDPPeers(params.MSDPKind) && data.Available[multicastDeliveryMSDPPeersView] {
		peers, peerTimes, err = a.queryMulticastDeliveryMSDPPeers(ctx, params, mroutes)
		if err != nil {
			if !multicastDeliverySourceErr(err) {
				writeMulticastDeliveryError(w, err, "multicast msdp peers query error", pkOrCode)
				return
			}
			data.Available[multicastDeliveryMSDPPeersView] = false
			peers = []MulticastDeliveryMSDPPeer{}
			peerTimes = nil
		}
	}

	pimSAs := []MulticastDeliveryMSDPSA{}
	if shouldQueryMSDPPimSACache(params.MSDPKind) && data.Available[multicastDeliveryMSDPPimSAView] {
		pimSAs, pimSATimes, err = a.queryMulticastDeliveryPimSACache(ctx, data.Group, params)
		if err != nil {
			if !multicastDeliverySourceErr(err) {
				writeMulticastDeliveryError(w, err, "multicast msdp pim-sa query error", pkOrCode)
				return
			}
			data.Available[multicastDeliveryMSDPPimSAView] = false
			pimSAs = []MulticastDeliveryMSDPSA{}
			pimSATimes = nil
		}
	}

	saCache := []MulticastDeliveryMSDPSA{}
	if shouldQueryMSDPSACache(params.MSDPKind) && data.Available[multicastDeliveryMSDPSAView] {
		saCache, saTimes, err = a.queryMulticastDeliverySACache(ctx, data.Group, params)
		if err != nil {
			if !multicastDeliverySourceErr(err) {
				writeMulticastDeliveryError(w, err, "multicast msdp sa query error", pkOrCode)
				return
			}
			data.Available[multicastDeliveryMSDPSAView] = false
			saCache = []MulticastDeliveryMSDPSA{}
			saTimes = nil
		}
	}

	freshness := buildMulticastFreshness(data.Now, data.Available, data.SourceTimes, mrouteTimes, peerTimes, pimSATimes, saTimes)
	applyMrouteFreshness(freshness.Mroute, mroutes)
	applyMSDPPeerFreshness(freshness.MSDPPeers, peers)
	applyMSDPSAFreshness(freshness.MSDPPimSACache, pimSAs)
	applyMSDPSAFreshness(freshness.MSDPSACache, saCache)

	msdpItems := buildMulticastMSDPItems(params.MSDPKind, peers, pimSAs, saCache)
	items, total := paginateMulticastDeliveryItems(msdpItems, params)

	writeJSON(w, MulticastDeliveryMSDPResponse{
		Group:       data.Group,
		GeneratedAt: formatMulticastTime(data.Now),
		Kind:        params.MSDPKind,
		Freshness:   freshness,
		Items:       items,
		Total:       total,
		Limit:       params.Limit,
		Offset:      params.Offset,
	})
}

// GetMulticastGroupDeliveryTree returns semantic observed/expected delivery-tree state without raw rows.
func (a *API) GetMulticastGroupDeliveryTree(w http.ResponseWriter, r *http.Request) {
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

	data, err := a.loadMulticastDeliveryRequestContext(ctx, pkOrCode)
	if err != nil {
		writeMulticastDeliveryError(w, err, "multicast delivery-tree query error", pkOrCode)
		return
	}

	members, err := a.queryMulticastDeliveryMembers(ctx, data.Group.PK)
	if err != nil {
		writeMulticastDeliveryError(w, err, "multicast delivery-tree members query error", pkOrCode)
		return
	}
	_, subscribers := splitMulticastDeliveryMembers(members, params)

	mroutes := []MulticastDeliveryMroute{}
	oifs := []MulticastDeliveryOIF{}
	var mrouteTimes []time.Time
	anomalies := []MulticastDeliveryAnomaly{}

	if !data.Available[multicastDeliveryMrouteView] || !data.Available[multicastDeliveryOIFView] {
		anomalies = append(anomalies, multicastAnomaly(
			"mroute-source-unavailable", "warning", "source_unavailable", "group",
			map[string]string{"group_pk": data.Group.PK},
			"mroute forwarding state source is unavailable for this environment",
		))
	} else {
		mroutes, mrouteTimes, err = a.queryMulticastDeliveryMroutes(ctx, data.Group, params)
		if err != nil {
			if !multicastDeliverySourceErr(err) {
				writeMulticastDeliveryError(w, err, "multicast delivery-tree mroutes query error", pkOrCode)
				return
			}
			data.Available[multicastDeliveryMrouteView] = false
			anomalies = append(anomalies, multicastAnomaly(
				"mroute-source-unavailable", "warning", "source_unavailable", "group",
				map[string]string{"group_pk": data.Group.PK},
				"mroute forwarding state source is unavailable for this environment",
			))
		} else {
			oifs, _, err = a.queryMulticastDeliveryOIFs(ctx, data.Group, params)
			if err != nil {
				if !multicastDeliverySourceErr(err) {
					writeMulticastDeliveryError(w, err, "multicast delivery-tree oifs query error", pkOrCode)
					return
				}
				data.Available[multicastDeliveryOIFView] = false
				mroutes = []MulticastDeliveryMroute{}
				oifs = []MulticastDeliveryOIF{}
				mrouteTimes = nil
				anomalies = append(anomalies, multicastAnomaly(
					"mroute-source-unavailable", "warning", "source_unavailable", "group",
					map[string]string{"group_pk": data.Group.PK},
					"mroute forwarding state source is unavailable for this environment",
				))
			}
		}
	}

	freshnessAvailable := data.Available
	if !data.Available[multicastDeliveryMrouteView] || !data.Available[multicastDeliveryOIFView] {
		freshnessAvailable = copyMulticastDeliveryAvailability(data.Available)
		freshnessAvailable[multicastDeliveryMrouteView] = false
	}
	freshness := buildMulticastFreshness(data.Now, freshnessAvailable, data.SourceTimes, mrouteTimes, nil, nil, nil)
	applyMrouteFreshness(freshness.Mroute, mroutes)
	applyOIFFreshness(freshness.Mroute, oifs)

	observed := buildObservedMulticastSegments(oifs)
	expected := []MulticastDeliverySegment{}
	if params.Mode == "expected" || params.Mode == "diff" || params.Mode == "all" {
		expected, _ = a.buildExpectedMulticastDeliverySegments(ctx, data.Group.PK, params)
	}
	outcomes := buildMulticastSubscriberOutcomes(subscribers, observed, expected)
	anomalies = append(anomalies, buildMulticastDeliveryAnomalies(data.Group, mroutes, oifs, freshness, outcomes)...)

	writeJSON(w, MulticastDeliveryTreeResponse{
		Group:              data.Group,
		SourceAvailable:    data.Available[multicastDeliveryMrouteView] && data.Available[multicastDeliveryOIFView],
		GeneratedAt:        formatMulticastTime(data.Now),
		Mode:               params.Mode,
		Freshness:          freshness,
		ObservedSegments:   observed,
		ExpectedSegments:   expected,
		SubscriberOutcomes: outcomes,
		Anomalies:          anomalies,
	})
}

func (a *API) loadMulticastDeliveryRequestContext(ctx context.Context, pkOrCode string) (multicastDeliveryRequestContext, error) {
	now := time.Now().UTC()
	group, err := a.queryMulticastDeliveryGroup(ctx, pkOrCode)
	if err != nil {
		return multicastDeliveryRequestContext{}, err
	}

	available, err := a.queryMulticastDeliverySources(ctx)
	if err != nil {
		return multicastDeliveryRequestContext{}, err
	}
	sourceTimes, err := a.queryMulticastDeliverySourceIngestTimes(ctx)
	if err != nil {
		return multicastDeliveryRequestContext{}, err
	}

	return multicastDeliveryRequestContext{
		Now:         now,
		Group:       group,
		Available:   available,
		SourceTimes: sourceTimes,
	}, nil
}

func writeMulticastDeliveryError(w http.ResponseWriter, err error, msg, pkOrCode string) {
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "multicast group not found", http.StatusNotFound)
		return
	}
	logError(msg, "error", err, "pk", pkOrCode)
	http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
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

	msdpKind := strings.TrimSpace(q.Get("kind"))
	if msdpKind == "" {
		msdpKind = "all"
	}
	if msdpKind != "all" && msdpKind != "peers" && msdpKind != "pim_sa_cache" && msdpKind != "sa_cache" {
		return MulticastDeliveryParams{}, fmt.Errorf("invalid kind")
	}

	pagination := ParsePagination(r, 100)
	return MulticastDeliveryParams{
		Mode:        mode,
		Sources:     splitCSVParam(q.Get("source")),
		Publishers:  splitCSVParam(firstNonEmpty(q.Get("publisher"), q.Get("publishers"))),
		Subscribers: splitCSVParam(firstNonEmpty(q.Get("subscriber"), q.Get("subscribers"))),
		Devices:     splitCSVParam(q.Get("device")),
		Links:       splitCSVParam(q.Get("link")),
		OIFKinds:    splitCSVParam(q.Get("oif_kind")),
		MSDPKind:    msdpKind,
		Includes:    csvSet(q.Get("include")),
		Limit:       pagination.Limit,
		Offset:      pagination.Offset,
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
		multicastDeliveryMrouteView,
		multicastDeliveryOIFView,
		multicastDeliveryMSDPPeersView,
		multicastDeliveryMSDPPimSAView,
		multicastDeliveryMSDPSAView,
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
			sourceTimes[multicastDeliveryMrouteSource] = latest
		case "SyncMSDP":
			sourceTimes[multicastDeliveryMSDPPeersSource] = latest
			sourceTimes[multicastDeliveryMSDPPimSASource] = latest
			sourceTimes[multicastDeliveryMSDPSASource] = latest
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
