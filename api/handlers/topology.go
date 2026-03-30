package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
	"golang.org/x/sync/errgroup"
)

type Metro struct {
	PK        string  `json:"pk"`
	Code      string  `json:"code"`
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type DeviceInterface struct {
	Name   string `json:"name"`
	IP     string `json:"ip"`
	Status string `json:"status"`
}

type Device struct {
	PK              string            `json:"pk"`
	Code            string            `json:"code"`
	Status          string            `json:"status"`
	DeviceType      string            `json:"device_type"`
	MetroPK         string            `json:"metro_pk"`
	ContributorPK   string            `json:"contributor_pk"`
	ContributorCode string            `json:"contributor_code"`
	UserCount       uint64            `json:"user_count"`
	ValidatorCount  uint64            `json:"validator_count"`
	StakeSol        float64           `json:"stake_sol"`
	StakeShare      float64           `json:"stake_share"`
	Interfaces      []DeviceInterface `json:"interfaces"`
}

type Link struct {
	PK                  string  `json:"pk"`
	Code                string  `json:"code"`
	Status              string  `json:"status"`
	LinkType            string  `json:"link_type"`
	BandwidthBps        int64   `json:"bandwidth_bps"`
	SideAPK             string  `json:"side_a_pk"`
	SideACode           string  `json:"side_a_code"`
	SideAIfaceName      string  `json:"side_a_iface_name"`
	SideAIP             string  `json:"side_a_ip"`
	SideZPK             string  `json:"side_z_pk"`
	SideZCode           string  `json:"side_z_code"`
	SideZIfaceName      string  `json:"side_z_iface_name"`
	SideZIP             string  `json:"side_z_ip"`
	ContributorPK       string  `json:"contributor_pk"`
	ContributorCode     string  `json:"contributor_code"`
	LatencyUs           float64 `json:"latency_us"`
	JitterUs            float64 `json:"jitter_us"`
	LatencyAtoZUs       float64 `json:"latency_a_to_z_us"`
	JitterAtoZUs        float64 `json:"jitter_a_to_z_us"`
	LatencyZtoAUs       float64 `json:"latency_z_to_a_us"`
	JitterZtoAUs        float64 `json:"jitter_z_to_a_us"`
	LossPercent         float64 `json:"loss_percent"`
	SampleCount         uint64  `json:"sample_count"`
	InBps               float64 `json:"in_bps"`
	OutBps              float64 `json:"out_bps"`
	CommittedRttNs      int64   `json:"committed_rtt_ns"`
	ISISDelayOverrideNs int64   `json:"isis_delay_override_ns"`
}

type Validator struct {
	VotePubkey  string  `json:"vote_pubkey"`
	NodePubkey  string  `json:"node_pubkey"`
	DevicePK    string  `json:"device_pk"`
	TunnelID    int32   `json:"tunnel_id"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
	City        string  `json:"city"`
	Country     string  `json:"country"`
	StakeSol    float64 `json:"stake_sol"`
	StakeShare  float64 `json:"stake_share"`
	Commission  int64   `json:"commission"`
	Version     string  `json:"version"`
	GossipIP    string  `json:"gossip_ip"`
	GossipPort  int32   `json:"gossip_port"`
	TPUQuicIP   string  `json:"tpu_quic_ip"`
	TPUQuicPort int32   `json:"tpu_quic_port"`
	InBps       float64 `json:"in_bps"`
	OutBps      float64 `json:"out_bps"`
}

type TopologyResponse struct {
	Metros     []Metro     `json:"metros"`
	Devices    []Device    `json:"devices"`
	Links      []Link      `json:"links"`
	Validators []Validator `json:"validators"`
	Error      string      `json:"error,omitempty"`
}

func (a *API) GetTopology(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	response, err := a.fetchTopologyData(ctx)
	if err != nil && dberror.IsTransient(err) {
		cancel()
		var retryCancel context.CancelFunc
		ctx, retryCancel = context.WithTimeout(r.Context(), 10*time.Second)
		defer retryCancel()
		response, err = a.fetchTopologyData(ctx)
	}

	if err != nil {
		slog.Warn("topology query failed", "error", err)
		response.Error = dberror.UserMessage(err)
	}

	// Ensure non-nil slices for JSON serialization
	if response.Metros == nil {
		response.Metros = []Metro{}
	}
	if response.Devices == nil {
		response.Devices = []Device{}
	}
	if response.Links == nil {
		response.Links = []Link{}
	}
	if response.Validators == nil {
		response.Validators = []Validator{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logError("failed to encode response", "error", err)
	}
}

// fetchTopologyData performs the actual topology data fetch from the database.
func (a *API) fetchTopologyData(ctx context.Context) (TopologyResponse, error) {
	start := time.Now()

	var metros []Metro
	var devices []Device
	var links []Link
	var validators []Validator

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(10)

	// Fetch metros with coordinates
	g.Go(func() error {
		query := `
			SELECT pk, code, name, latitude, longitude
			FROM dz_metros_current
			WHERE latitude IS NOT NULL AND longitude IS NOT NULL
		`
		rows, err := a.envDB(ctx).Query(ctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var m Metro
			if err := rows.Scan(&m.PK, &m.Code, &m.Name, &m.Latitude, &m.Longitude); err != nil {
				return err
			}
			metros = append(metros, m)
		}
		return rows.Err()
	})

	// Fetch activated devices with user/validator/stake stats
	g.Go(func() error {
		query := `
			WITH total_stake AS (
				SELECT COALESCE(SUM(activated_stake_lamports), 0) as total_lamports
				FROM solana_vote_accounts_current
				WHERE epoch_vote_account = 'true' AND activated_stake_lamports > 0
			),
			device_stats AS (
				SELECT
					u.device_pk,
					COUNT(DISTINCT u.pk) as user_count,
					COUNT(DISTINCT va.vote_pubkey) as validator_count,
					COALESCE(SUM(va.activated_stake_lamports), 0) / 1e9 as stake_sol
				FROM dz_users_current u
				LEFT JOIN solana_gossip_nodes_current gn ON u.client_ip = gn.gossip_ip
				LEFT JOIN solana_vote_accounts_current va ON gn.pubkey = va.node_pubkey
					AND va.epoch_vote_account = 'true'
					AND va.activated_stake_lamports > 0
				WHERE u.status = 'activated'
					AND u.client_ip != ''
				GROUP BY u.device_pk
			)
			SELECT
				d.pk, d.code, d.status, d.device_type, d.metro_pk,
				d.contributor_pk, c.code as contributor_code,
				COALESCE(ds.user_count, 0) as user_count,
				COALESCE(ds.validator_count, 0) as validator_count,
				COALESCE(ds.stake_sol, 0) as stake_sol,
				CASE
					WHEN ts.total_lamports > 0 THEN COALESCE(ds.stake_sol, 0) * 1e9 / ts.total_lamports * 100
					ELSE 0
				END as stake_share,
				COALESCE(d.interfaces, '[]') as interfaces
			FROM dz_devices_current d
			CROSS JOIN total_stake ts
			LEFT JOIN device_stats ds ON d.pk = ds.device_pk
			LEFT JOIN dz_contributors_current c ON d.contributor_pk = c.pk
			WHERE d.status = 'activated'
		`
		rows, err := a.envDB(ctx).Query(ctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var d Device
			var interfacesJSON string
			if err := rows.Scan(&d.PK, &d.Code, &d.Status, &d.DeviceType, &d.MetroPK, &d.ContributorPK, &d.ContributorCode, &d.UserCount, &d.ValidatorCount, &d.StakeSol, &d.StakeShare, &interfacesJSON); err != nil {
				return err
			}
			if err := json.Unmarshal([]byte(interfacesJSON), &d.Interfaces); err != nil {
				logError("failed to parse interfaces JSON", "device_pk", d.PK, "error", err)
				d.Interfaces = []DeviceInterface{}
			}
			devices = append(devices, d)
		}
		return rows.Err()
	})

	// Fetch activated links with measured latency, jitter, loss, and traffic rates
	g.Go(func() error {
		query := `
			SELECT
				l.pk, l.code, l.status, l.link_type, l.bandwidth_bps,
				l.side_a_pk, COALESCE(da.code, '') as side_a_code, COALESCE(l.side_a_iface_name, '') as side_a_iface_name, COALESCE(l.side_a_ip, '') as side_a_ip,
				l.side_z_pk, COALESCE(dz.code, '') as side_z_code, COALESCE(l.side_z_iface_name, '') as side_z_iface_name, COALESCE(l.side_z_ip, '') as side_z_ip,
				l.contributor_pk, COALESCE(c.code, '') as contributor_code,
				COALESCE(lat.avg_rtt_us, 0) as latency_us,
				COALESCE(lat.avg_ipdv_us, 0) as jitter_us,
				COALESCE(lat_a.avg_rtt_us, 0) as latency_a_to_z_us,
				COALESCE(lat_a.avg_ipdv_us, 0) as jitter_a_to_z_us,
				COALESCE(lat_z.avg_rtt_us, 0) as latency_z_to_a_us,
				COALESCE(lat_z.avg_ipdv_us, 0) as jitter_z_to_a_us,
				COALESCE(lat.loss_percent, 0) as loss_percent,
				COALESCE(lat.sample_count, 0) as sample_count,
				COALESCE(traffic.in_bps, 0) as in_bps,
				COALESCE(traffic.out_bps, 0) as out_bps,
				COALESCE(l.committed_rtt_ns, 0) as committed_rtt_ns,
				COALESCE(l.isis_delay_override_ns, 0) as isis_delay_override_ns
			FROM dz_links_current l
			LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
			LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
			LEFT JOIN dz_contributors_current c ON l.contributor_pk = c.pk
			LEFT JOIN (
				SELECT link_pk,
					sum(a_avg_rtt_us * a_samples + z_avg_rtt_us * z_samples) / greatest(sum(a_samples + z_samples), 1) as avg_rtt_us,
					sum(a_avg_jitter_us * a_samples + z_avg_jitter_us * z_samples) / greatest(sum(a_samples + z_samples), 1) as avg_ipdv_us,
					sum(a_loss_pct * a_samples + z_loss_pct * z_samples) / greatest(sum(a_samples + z_samples), 1) as loss_percent,
					sum(a_samples + z_samples) as sample_count
				FROM link_rollup_5m FINAL
				WHERE bucket_ts >= now() - INTERVAL 3 HOUR
				GROUP BY link_pk
			) lat ON l.pk = lat.link_pk
			LEFT JOIN (
				SELECT link_pk,
					sum(a_avg_rtt_us * a_samples) / greatest(sum(a_samples), 1) as avg_rtt_us,
					sum(a_avg_jitter_us * a_samples) / greatest(sum(a_samples), 1) as avg_ipdv_us
				FROM link_rollup_5m FINAL
				WHERE bucket_ts >= now() - INTERVAL 3 HOUR
				GROUP BY link_pk
			) lat_a ON l.pk = lat_a.link_pk
			LEFT JOIN (
				SELECT link_pk,
					sum(z_avg_rtt_us * z_samples) / greatest(sum(z_samples), 1) as avg_rtt_us,
					sum(z_avg_jitter_us * z_samples) / greatest(sum(z_samples), 1) as avg_ipdv_us
				FROM link_rollup_5m FINAL
				WHERE bucket_ts >= now() - INTERVAL 3 HOUR
				GROUP BY link_pk
			) lat_z ON l.pk = lat_z.link_pk
			LEFT JOIN (
				SELECT link_pk,
					avg(avg_in_bps) as in_bps,
					avg(avg_out_bps) as out_bps
				FROM device_interface_rollup_5m
				WHERE bucket_ts >= now() - INTERVAL 5 MINUTE
					AND link_pk != ''
				GROUP BY link_pk
			) traffic ON l.pk = traffic.link_pk
			WHERE l.status = 'activated'
		`
		rows, err := a.envDB(ctx).Query(ctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var l Link
			if err := rows.Scan(&l.PK, &l.Code, &l.Status, &l.LinkType, &l.BandwidthBps, &l.SideAPK, &l.SideACode, &l.SideAIfaceName, &l.SideAIP, &l.SideZPK, &l.SideZCode, &l.SideZIfaceName, &l.SideZIP, &l.ContributorPK, &l.ContributorCode, &l.LatencyUs, &l.JitterUs, &l.LatencyAtoZUs, &l.JitterAtoZUs, &l.LatencyZtoAUs, &l.JitterZtoAUs, &l.LossPercent, &l.SampleCount, &l.InBps, &l.OutBps, &l.CommittedRttNs, &l.ISISDelayOverrideNs); err != nil {
				return err
			}
			links = append(links, l)
		}
		return rows.Err()
	})

	// Fetch validators on DZ with their GeoIP locations and traffic rates
	g.Go(func() error {
		query := `
			WITH dz_user_ips AS (
				SELECT
					client_ip,
					any(device_pk) as device_pk,
					any(tunnel_id) as tunnel_id
				FROM dz_users_current
				WHERE status = 'activated'
					AND client_ip != ''
				GROUP BY client_ip
			),
			total_dz_stake AS (
				SELECT COALESCE(SUM(va.activated_stake_lamports), 0) as total_lamports
				FROM dz_user_ips u
				JOIN solana_gossip_nodes_current gn ON u.client_ip = gn.gossip_ip
				JOIN solana_vote_accounts_current va ON gn.pubkey = va.node_pubkey
					AND va.epoch_vote_account = 'true'
					AND va.activated_stake_lamports > 0
			),
			user_traffic AS (
				SELECT
					user_tunnel_id,
					CASE WHEN SUM(delta_duration) > 0 THEN SUM(in_octets_delta) * 8 / SUM(delta_duration) ELSE 0 END as in_bps,
					CASE WHEN SUM(delta_duration) > 0 THEN SUM(out_octets_delta) * 8 / SUM(delta_duration) ELSE 0 END as out_bps
				FROM fact_dz_device_interface_counters
				WHERE event_ts > now() - INTERVAL 5 MINUTE
					AND user_tunnel_id IS NOT NULL
					AND delta_duration > 0
					AND in_octets_delta >= 0
					AND out_octets_delta >= 0
				GROUP BY user_tunnel_id
			)
			SELECT
				va.vote_pubkey,
				gn.pubkey as node_pubkey,
				u.device_pk,
				u.tunnel_id,
				geo.latitude,
				geo.longitude,
				COALESCE(geo.city, '') as city,
				COALESCE(geo.country, '') as country,
				va.activated_stake_lamports / 1e9 as stake_sol,
				CASE
					WHEN ts.total_lamports > 0 THEN va.activated_stake_lamports / ts.total_lamports * 100
					ELSE 0
				END as stake_share,
				COALESCE(va.commission_percentage, 0) as commission,
				COALESCE(gn.version, '') as version,
				COALESCE(gn.gossip_ip, '') as gossip_ip,
				COALESCE(gn.gossip_port, 0) as gossip_port,
				COALESCE(gn.tpuquic_ip, '') as tpu_quic_ip,
				COALESCE(gn.tpuquic_port, 0) as tpu_quic_port,
				COALESCE(traffic.in_bps, 0) as in_bps,
				COALESCE(traffic.out_bps, 0) as out_bps
			FROM dz_user_ips u
			JOIN solana_gossip_nodes_current gn ON u.client_ip = gn.gossip_ip
			JOIN solana_vote_accounts_current va ON gn.pubkey = va.node_pubkey
				AND va.epoch_vote_account = 'true'
				AND va.activated_stake_lamports > 0
			LEFT JOIN geoip_records_current geo ON gn.gossip_ip = geo.ip
			LEFT JOIN user_traffic traffic ON u.tunnel_id = traffic.user_tunnel_id
			CROSS JOIN total_dz_stake ts
			WHERE geo.latitude IS NOT NULL
				AND geo.longitude IS NOT NULL
		`
		rows, err := a.envDB(ctx).Query(ctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var v Validator
			if err := rows.Scan(&v.VotePubkey, &v.NodePubkey, &v.DevicePK, &v.TunnelID, &v.Latitude, &v.Longitude, &v.City, &v.Country, &v.StakeSol, &v.StakeShare, &v.Commission, &v.Version, &v.GossipIP, &v.GossipPort, &v.TPUQuicIP, &v.TPUQuicPort, &v.InBps, &v.OutBps); err != nil {
				return err
			}
			validators = append(validators, v)
		}
		return rows.Err()
	})

	err := g.Wait()
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	return TopologyResponse{
		Metros:     metros,
		Devices:    devices,
		Links:      links,
		Validators: validators,
	}, err
}

// Traffic data point for charts
type TrafficDataPoint struct {
	Time    string  `json:"time"`
	AvgIn   float64 `json:"avgIn"`
	AvgOut  float64 `json:"avgOut"`
	PeakIn  float64 `json:"peakIn"`
	PeakOut float64 `json:"peakOut"`
}

// InterfaceTrafficDataPoint is a per-interface traffic data point
type InterfaceTrafficDataPoint struct {
	Time    string  `json:"time"`
	Intf    string  `json:"intf"`
	AvgIn   float64 `json:"avgIn"`
	AvgOut  float64 `json:"avgOut"`
	PeakIn  float64 `json:"peakIn"`
	PeakOut float64 `json:"peakOut"`
}

type TrafficResponse struct {
	Points     []TrafficDataPoint          `json:"points"`
	Interfaces []InterfaceTrafficDataPoint `json:"interfaces,omitempty"`
	Error      string                      `json:"error,omitempty"`
}

func (a *API) GetEntityTraffic(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	itemType := r.URL.Query().Get("type")
	pk := r.URL.Query().Get("pk")

	if pk == "" || (itemType != "link" && itemType != "device" && itemType != "validator") {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(TrafficResponse{Points: []TrafficDataPoint{}})
		return
	}

	// Normalize topology params to the shared time filter system.
	// The topology API uses "range" (not "time_range") and "from"/"to" (not "start_time"/"end_time").
	q := r.URL.Query()
	if q.Get("range") != "" && q.Get("time_range") == "" {
		q.Set("time_range", q.Get("range"))
	}
	if q.Get("from") != "" && q.Get("to") != "" && q.Get("start_time") == "" {
		fromTime, err1 := time.Parse("2006-01-02-15:04:05", q.Get("from"))
		toTime, err2 := time.Parse("2006-01-02-15:04:05", q.Get("to"))
		if err1 == nil && err2 == nil {
			q.Set("start_time", fmt.Sprintf("%d", fromTime.Unix()))
			q.Set("end_time", fmt.Sprintf("%d", toTime.Unix()))
		}
	}
	r.URL.RawQuery = q.Encode()

	// Use shared time filter with raw/rollup routing
	timeFilter, bucketInterval, useRaw := trafficTimeFilter(r)

	metricParam := r.URL.Query().Get("metric")
	breakdown := r.URL.Query().Get("breakdown")

	// Parse aggregation mode
	aggParam := r.URL.Query().Get("agg")
	if aggParam == "" {
		aggParam = "max"
	}

	var whereColumn string
	switch itemType {
	case "link":
		whereColumn = "link_pk"
	case "validator":
		whereColumn = "user_tunnel_id"
	default:
		whereColumn = "device_pk"
	}

	start := time.Now()
	var points []TrafficDataPoint
	response := TrafficResponse{Points: points}

	if useRaw {
		// Raw fact table path
		var rawAggFunc string
		switch aggParam {
		case "avg":
			rawAggFunc = "avg"
		case "min":
			rawAggFunc = "min"
		case "p50":
			rawAggFunc = "quantile(0.5)"
		case "p90":
			rawAggFunc = "quantile(0.9)"
		case "p95":
			rawAggFunc = "quantile(0.95)"
		case "p99":
			rawAggFunc = "quantile(0.99)"
		default:
			rawAggFunc = "max"
		}

		var avgInExpr, avgOutExpr, peakInExpr, peakOutExpr string
		if metricParam == "packets" {
			avgInExpr = "avg(in_pkts_delta / nullIf(delta_duration, 0))"
			avgOutExpr = "avg(out_pkts_delta / nullIf(delta_duration, 0))"
			peakInExpr = fmt.Sprintf("%s(in_pkts_delta / nullIf(delta_duration, 0))", rawAggFunc)
			peakOutExpr = fmt.Sprintf("%s(out_pkts_delta / nullIf(delta_duration, 0))", rawAggFunc)
		} else {
			avgInExpr = "avg(in_octets_delta * 8 / nullIf(delta_duration, 0))"
			avgOutExpr = "avg(out_octets_delta * 8 / nullIf(delta_duration, 0))"
			peakInExpr = fmt.Sprintf("%s(in_octets_delta * 8 / nullIf(delta_duration, 0))", rawAggFunc)
			peakOutExpr = fmt.Sprintf("%s(out_octets_delta * 8 / nullIf(delta_duration, 0))", rawAggFunc)
		}

		bucketExpr := fmt.Sprintf("toStartOfInterval(event_ts, INTERVAL %s)", bucketInterval)

		// Aggregated query (skip when only interface breakdown needed)
		if breakdown != "interface" || (itemType != "device" && itemType != "link") {
			query := fmt.Sprintf(`
				SELECT
					formatDateTime(%s, '%%Y-%%m-%%dT%%H:%%i:%%SZ') as time_bucket,
					%s as avg_in, %s as avg_out,
					%s as peak_in, %s as peak_out
				FROM fact_dz_device_interface_counters
				WHERE %s AND %s = $1
					AND delta_duration > 0 AND in_octets_delta >= 0 AND out_octets_delta >= 0
				GROUP BY time_bucket
				ORDER BY min(event_ts)
			`, bucketExpr, avgInExpr, avgOutExpr, peakInExpr, peakOutExpr, timeFilter, whereColumn)

			points = a.scanTrafficPoints(ctx, query, pk)
		}

		// Per-interface breakdown
		if breakdown == "interface" && (itemType == "device" || itemType == "link") {
			intfExpr := "intf"
			if itemType == "link" {
				intfExpr = "concat(link_side, ':', intf)"
			}
			intfQuery := fmt.Sprintf(`
				SELECT
					formatDateTime(%s, '%%Y-%%m-%%dT%%H:%%i:%%SZ') as time_bucket,
					%s as intf_key,
					%s as avg_in, %s as avg_out,
					%s as peak_in, %s as peak_out
				FROM fact_dz_device_interface_counters
				WHERE %s AND %s = $1
					AND delta_duration > 0 AND in_octets_delta >= 0 AND out_octets_delta >= 0
				GROUP BY time_bucket, intf_key
				ORDER BY min(event_ts), intf_key
			`, bucketExpr, intfExpr, avgInExpr, avgOutExpr, peakInExpr, peakOutExpr, timeFilter, whereColumn)

			response.Interfaces = a.scanInterfaceTrafficPoints(ctx, intfQuery, pk)
		}
	} else {
		// Rollup path
		aggPrefix := "max"
		switch aggParam {
		case "avg":
			aggPrefix = "avg"
		case "min":
			aggPrefix = "min"
		case "p50":
			aggPrefix = "p50"
		case "p90":
			aggPrefix = "p90"
		case "p95":
			aggPrefix = "p95"
		case "p99":
			aggPrefix = "p99"
		}

		rollupAggFunc := "MAX"
		switch aggParam {
		case "avg":
			rollupAggFunc = "AVG"
		case "min":
			rollupAggFunc = "MIN"
		}

		var avgInCol, avgOutCol, peakInCol, peakOutCol string
		if metricParam == "packets" {
			avgInCol = "avg_in_pps"
			avgOutCol = "avg_out_pps"
			peakInCol = fmt.Sprintf("%s_in_pps", aggPrefix)
			peakOutCol = fmt.Sprintf("%s_out_pps", aggPrefix)
		} else {
			avgInCol = "avg_in_bps"
			avgOutCol = "avg_out_bps"
			peakInCol = fmt.Sprintf("%s_in_bps", aggPrefix)
			peakOutCol = fmt.Sprintf("%s_out_bps", aggPrefix)
		}

		bucketExpr := fmt.Sprintf("toStartOfInterval(bucket_ts, INTERVAL %s)", bucketInterval)

		// Aggregated query
		if breakdown != "interface" || (itemType != "device" && itemType != "link") {
			query := fmt.Sprintf(`
				SELECT
					formatDateTime(%s, '%%Y-%%m-%%dT%%H:%%i:%%SZ') as time_bucket,
					AVG(%s) as avg_in, AVG(%s) as avg_out,
					%s(%s) as peak_in, %s(%s) as peak_out
				FROM device_interface_rollup_5m
				WHERE %s AND %s = $1
				GROUP BY time_bucket
				ORDER BY time_bucket
			`, bucketExpr, avgInCol, avgOutCol, rollupAggFunc, peakInCol, rollupAggFunc, peakOutCol, timeFilter, whereColumn)

			points = a.scanTrafficPoints(ctx, query, pk)
		}

		// Per-interface breakdown
		if breakdown == "interface" && (itemType == "device" || itemType == "link") {
			intfExpr := "intf"
			if itemType == "link" {
				intfExpr = "concat(link_side, ':', intf)"
			}
			intfQuery := fmt.Sprintf(`
				SELECT
					formatDateTime(%s, '%%Y-%%m-%%dT%%H:%%i:%%SZ') as time_bucket,
					%s as intf_key,
					AVG(%s) as avg_in, AVG(%s) as avg_out,
					%s(%s) as peak_in, %s(%s) as peak_out
				FROM device_interface_rollup_5m
				WHERE %s AND %s = $1
				GROUP BY time_bucket, intf_key
				ORDER BY time_bucket, intf_key
			`, bucketExpr, intfExpr, avgInCol, avgOutCol, rollupAggFunc, peakInCol, rollupAggFunc, peakOutCol, timeFilter, whereColumn)

			response.Interfaces = a.scanInterfaceTrafficPoints(ctx, intfQuery, pk)
		}
	}

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil)

	if points == nil {
		points = []TrafficDataPoint{}
	}
	response.Points = points

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// scanTrafficPoints executes a query and scans aggregated traffic data points.
func (a *API) scanTrafficPoints(ctx context.Context, query, pk string) []TrafficDataPoint {
	rows, err := a.envDB(ctx).Query(ctx, query, pk)
	if err != nil {
		logError("traffic query error", "error", err)
		return nil
	}
	defer rows.Close()

	var points []TrafficDataPoint
	for rows.Next() {
		var p TrafficDataPoint
		var avgIn, avgOut, peakIn, peakOut *float64
		if err := rows.Scan(&p.Time, &avgIn, &avgOut, &peakIn, &peakOut); err != nil {
			logError("traffic scan error", "error", err)
			return points
		}
		if avgIn != nil {
			p.AvgIn = *avgIn
		}
		if avgOut != nil {
			p.AvgOut = *avgOut
		}
		if peakIn != nil {
			p.PeakIn = *peakIn
		}
		if peakOut != nil {
			p.PeakOut = *peakOut
		}
		points = append(points, p)
	}
	return points
}

// scanInterfaceTrafficPoints executes a query and scans per-interface traffic data points.
func (a *API) scanInterfaceTrafficPoints(ctx context.Context, query, pk string) []InterfaceTrafficDataPoint {
	rows, err := a.envDB(ctx).Query(ctx, query, pk)
	if err != nil {
		logError("interface traffic query error", "error", err)
		return nil
	}
	defer rows.Close()

	var points []InterfaceTrafficDataPoint
	for rows.Next() {
		var p InterfaceTrafficDataPoint
		var avgIn, avgOut, peakIn, peakOut *float64
		if err := rows.Scan(&p.Time, &p.Intf, &avgIn, &avgOut, &peakIn, &peakOut); err != nil {
			logError("interface traffic scan error", "error", err)
			return points
		}
		if avgIn != nil {
			p.AvgIn = *avgIn
		}
		if avgOut != nil {
			p.AvgOut = *avgOut
		}
		if peakIn != nil {
			p.PeakIn = *peakIn
		}
		if peakOut != nil {
			p.PeakOut = *peakOut
		}
		points = append(points, p)
	}
	return points
}

// scanLatencyPoints executes a query and scans latency data points.
func (a *API) scanLatencyPoints(ctx context.Context, query string, args ...any) []LinkLatencyDataPoint {
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	if err != nil {
		logError("latency query error", "error", err)
		return nil
	}
	defer rows.Close()

	var points []LinkLatencyDataPoint
	for rows.Next() {
		var p LinkLatencyDataPoint
		var avgRtt, p95Rtt, avgJitter, lossPct, avgRttAtoZ, p95RttAtoZ, avgRttZtoA, p95RttZtoA, jitterAtoZ, jitterZtoA *float64
		if err := rows.Scan(&p.Time, &avgRtt, &p95Rtt, &avgJitter, &lossPct, &avgRttAtoZ, &p95RttAtoZ, &avgRttZtoA, &p95RttZtoA, &jitterAtoZ, &jitterZtoA); err != nil {
			logError("latency scan error", "error", err)
			return points
		}
		if avgRtt != nil && !math.IsNaN(*avgRtt) {
			p.AvgRttMs = *avgRtt
		}
		if p95Rtt != nil && !math.IsNaN(*p95Rtt) {
			p.P95RttMs = *p95Rtt
		}
		if avgJitter != nil && !math.IsNaN(*avgJitter) {
			p.AvgJitter = *avgJitter
		}
		if lossPct != nil && !math.IsNaN(*lossPct) {
			p.LossPct = *lossPct
		}
		if avgRttAtoZ != nil && !math.IsNaN(*avgRttAtoZ) {
			p.AvgRttAtoZMs = *avgRttAtoZ
		}
		if p95RttAtoZ != nil && !math.IsNaN(*p95RttAtoZ) {
			p.P95RttAtoZMs = *p95RttAtoZ
		}
		if avgRttZtoA != nil && !math.IsNaN(*avgRttZtoA) {
			p.AvgRttZtoAMs = *avgRttZtoA
		}
		if p95RttZtoA != nil && !math.IsNaN(*p95RttZtoA) {
			p.P95RttZtoAMs = *p95RttZtoA
		}
		if jitterAtoZ != nil && !math.IsNaN(*jitterAtoZ) {
			p.JitterAtoZMs = *jitterAtoZ
		}
		if jitterZtoA != nil && !math.IsNaN(*jitterZtoA) {
			p.JitterZtoAMs = *jitterZtoA
		}
		points = append(points, p)
	}
	return points
}

// Link latency data point for charts
type LinkLatencyDataPoint struct {
	Time         string  `json:"time"`
	AvgRttMs     float64 `json:"avgRttMs"`
	P95RttMs     float64 `json:"p95RttMs"`
	AvgJitter    float64 `json:"avgJitter"`
	LossPct      float64 `json:"lossPct"`
	AvgRttAtoZMs float64 `json:"avgRttAtoZMs"`
	P95RttAtoZMs float64 `json:"p95RttAtoZMs"`
	AvgRttZtoAMs float64 `json:"avgRttZtoAMs"`
	P95RttZtoAMs float64 `json:"p95RttZtoAMs"`
	JitterAtoZMs float64 `json:"jitterAtoZMs"`
	JitterZtoAMs float64 `json:"jitterZtoAMs"`
}

type LinkLatencyResponse struct {
	Points []LinkLatencyDataPoint `json:"points"`
	Error  string                 `json:"error,omitempty"`
}

func (a *API) GetLinkLatencyHistory(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	pk := r.URL.Query().Get("pk")

	if pk == "" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(LinkLatencyResponse{Points: []LinkLatencyDataPoint{}})
		return
	}

	// Normalize topology params to shared time filter system
	q := r.URL.Query()
	if q.Get("range") != "" && q.Get("time_range") == "" {
		q.Set("time_range", q.Get("range"))
	}
	if q.Get("from") != "" && q.Get("to") != "" && q.Get("start_time") == "" {
		fromTime, err1 := time.Parse("2006-01-02-15:04:05", q.Get("from"))
		toTime, err2 := time.Parse("2006-01-02-15:04:05", q.Get("to"))
		if err1 == nil && err2 == nil {
			q.Set("start_time", fmt.Sprintf("%d", fromTime.Unix()))
			q.Set("end_time", fmt.Sprintf("%d", toTime.Unix()))
		}
	}
	r.URL.RawQuery = q.Encode()

	// Use shared time filter with raw/rollup routing
	timeFilter, bucketInterval, useRaw := trafficTimeFilter(r)

	// Parse aggregation mode for RTT and jitter
	aggParam := r.URL.Query().Get("agg")
	if aggParam == "" {
		aggParam = "avg"
	}

	start := time.Now()
	var points []LinkLatencyDataPoint

	if useRaw {
		// Raw fact table path — complex query with display timestamps and sub-bucket loss
		topoDisplayTs := "if(h.sampling_interval_us > 0 AND f.sample_index >= h.latest_sample_index - 1000, f.ingested_at, f.event_ts)"
		topoHeaderJoin := `LEFT JOIN (
				SELECT origin_device_pk, target_device_pk, link_pk AS _hdr_link_pk, epoch,
					   max(latest_sample_index) AS latest_sample_index,
					   any(sampling_interval_us) AS sampling_interval_us
				FROM fact_dz_device_link_latency_sample_header
				GROUP BY origin_device_pk, target_device_pk, link_pk, epoch
			) h ON f.origin_device_pk = h.origin_device_pk
				AND f.target_device_pk = h.target_device_pk
				AND f.link_pk = h._hdr_link_pk
				AND f.epoch = h.epoch`
		displayBucketExpr := fmt.Sprintf("toStartOfInterval(%s, INTERVAL %s)", topoDisplayTs, bucketInterval)
		lossBucketExpr := fmt.Sprintf("toStartOfInterval(%s, INTERVAL 5 MINUTE)", topoDisplayTs)

		// Map agg to SQL function for RTT and jitter
		var rttAggFunc, jitterAggFunc string
		switch aggParam {
		case "min":
			rttAggFunc = "min"
			jitterAggFunc = "min"
		case "p50":
			rttAggFunc = "quantile(0.5)"
			jitterAggFunc = "quantile(0.5)"
		case "p90":
			rttAggFunc = "quantile(0.9)"
			jitterAggFunc = "quantile(0.9)"
		case "p95":
			rttAggFunc = "quantile(0.95)"
			jitterAggFunc = "quantile(0.95)"
		case "p99":
			rttAggFunc = "quantile(0.99)"
			jitterAggFunc = "quantile(0.99)"
		case "max":
			rttAggFunc = "max"
			jitterAggFunc = "max"
		default: // avg
			rttAggFunc = "avg"
			jitterAggFunc = "avg"
		}

		query := fmt.Sprintf(`
			WITH loss_sub AS (
				SELECT
					%s as display_bucket,
					countIf(f.loss) * 100.0 / count(*) as loss_pct
				FROM fact_dz_device_link_latency f
				JOIN dz_links_current l ON f.link_pk = l.pk
				%s
				WHERE %s AND f.link_pk = $1
				GROUP BY display_bucket, %s
			),
			loss_max AS (
				SELECT display_bucket, max(loss_pct) as loss_pct
				FROM loss_sub
				GROUP BY display_bucket
			)
			SELECT
				formatDateTime(%s, '%%Y-%%m-%%dT%%H:%%i:%%SZ') as time_bucket,
				avg(f.rtt_us) / 1000.0 as avg_rtt_ms,
				%s(f.rtt_us) / 1000.0 as p95_rtt_ms,
				avg(abs(f.ipdv_us)) / 1000.0 as avg_jitter_ms,
				COALESCE(max(lm.loss_pct), 0) as loss_pct,
				avgIf(f.rtt_us, f.origin_device_pk = l.side_a_pk) / 1000.0 as avg_rtt_a_to_z_ms,
				%sIf(f.rtt_us, f.origin_device_pk = l.side_a_pk) / 1000.0 as p95_rtt_a_to_z_ms,
				avgIf(f.rtt_us, f.origin_device_pk = l.side_z_pk) / 1000.0 as avg_rtt_z_to_a_ms,
				%sIf(f.rtt_us, f.origin_device_pk = l.side_z_pk) / 1000.0 as p95_rtt_z_to_a_ms,
				%sIf(abs(f.ipdv_us), f.origin_device_pk = l.side_a_pk) / 1000.0 as jitter_a_to_z_ms,
				%sIf(abs(f.ipdv_us), f.origin_device_pk = l.side_z_pk) / 1000.0 as jitter_z_to_a_ms
			FROM fact_dz_device_link_latency f
			JOIN dz_links_current l ON f.link_pk = l.pk
			%s
			LEFT JOIN loss_max lm ON lm.display_bucket = %s
			WHERE %s AND f.link_pk = $2
			GROUP BY %s
			ORDER BY %s`,
			displayBucketExpr, topoHeaderJoin, timeFilter, lossBucketExpr,
			displayBucketExpr,
			rttAggFunc,
			rttAggFunc, rttAggFunc,
			jitterAggFunc, jitterAggFunc,
			topoHeaderJoin, displayBucketExpr,
			timeFilter, displayBucketExpr, displayBucketExpr)

		points = a.scanLatencyPoints(ctx, query, pk, pk)
	} else {
		// Rollup path — read from link_rollup_5m
		var aggPrefix string
		switch aggParam {
		case "min":
			aggPrefix = "min"
		case "p50":
			aggPrefix = "p50"
		case "p90":
			aggPrefix = "p90"
		case "p95":
			aggPrefix = "p95"
		case "p99":
			aggPrefix = "p99"
		case "max":
			aggPrefix = "max"
		default:
			aggPrefix = "avg"
		}

		rollupAggFunc := "AVG"
		switch aggParam {
		case "max":
			rollupAggFunc = "MAX"
		case "min":
			rollupAggFunc = "MIN"
		}

		query := fmt.Sprintf(`
			SELECT
				formatDateTime(toStartOfInterval(bucket_ts, INTERVAL %s), '%%Y-%%m-%%dT%%H:%%i:%%SZ') as time_bucket,
				AVG(a_avg_rtt_us + z_avg_rtt_us) / 2.0 / 1000.0 as avg_rtt_ms,
				%s(greatest(a_%s_rtt_us, z_%s_rtt_us)) / 1000.0 as p95_rtt_ms,
				AVG(a_avg_jitter_us + z_avg_jitter_us) / 2.0 / 1000.0 as avg_jitter_ms,
				MAX(greatest(a_loss_pct, z_loss_pct)) as loss_pct,
				%s(a_%s_rtt_us) / 1000.0 as avg_rtt_a_to_z_ms,
				%s(a_%s_rtt_us) / 1000.0 as p95_rtt_a_to_z_ms,
				%s(z_%s_rtt_us) / 1000.0 as avg_rtt_z_to_a_ms,
				%s(z_%s_rtt_us) / 1000.0 as p95_rtt_z_to_a_ms,
				%s(a_%s_jitter_us) / 1000.0 as jitter_a_to_z_ms,
				%s(z_%s_jitter_us) / 1000.0 as jitter_z_to_a_ms
			FROM link_rollup_5m
			WHERE %s AND link_pk = $1
			GROUP BY time_bucket
			ORDER BY time_bucket`,
			bucketInterval,
			rollupAggFunc, aggPrefix, aggPrefix,
			"AVG", "avg", // avg_rtt A→Z always avg for the "avg" column
			rollupAggFunc, aggPrefix, // p95_rtt A→Z uses selected agg
			"AVG", "avg", // avg_rtt Z→A always avg
			rollupAggFunc, aggPrefix, // p95_rtt Z→A uses selected agg
			rollupAggFunc, aggPrefix, // jitter A→Z
			rollupAggFunc, aggPrefix, // jitter Z→A
			timeFilter)

		points = a.scanLatencyPoints(ctx, query, pk)
	}

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil)

	if points == nil {
		points = []LinkLatencyDataPoint{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(LinkLatencyResponse{Points: points}); err != nil {
		logError("failed to encode response", "pk", pk, "error", err)
	}
}

// DZ vs Internet latency comparison types
type LatencyComparison struct {
	OriginMetroPK        string   `json:"origin_metro_pk"`
	OriginMetroCode      string   `json:"origin_metro_code"`
	OriginMetroName      string   `json:"origin_metro_name"`
	TargetMetroPK        string   `json:"target_metro_pk"`
	TargetMetroCode      string   `json:"target_metro_code"`
	TargetMetroName      string   `json:"target_metro_name"`
	DzAvgRttMs           float64  `json:"dz_avg_rtt_ms"`
	DzP95RttMs           float64  `json:"dz_p95_rtt_ms"`
	DzAvgJitterMs        *float64 `json:"dz_avg_jitter_ms"`
	DzLossPct            float64  `json:"dz_loss_pct"`
	DzSampleCount        uint64   `json:"dz_sample_count"`
	InternetAvgRttMs     float64  `json:"internet_avg_rtt_ms"`
	InternetP95RttMs     float64  `json:"internet_p95_rtt_ms"`
	InternetAvgJitterMs  *float64 `json:"internet_avg_jitter_ms"`
	InternetSampleCount  uint64   `json:"internet_sample_count"`
	RttImprovementPct    *float64 `json:"rtt_improvement_pct"`
	JitterImprovementPct *float64 `json:"jitter_improvement_pct"`
}

type LatencyComparisonResponse struct {
	Comparisons []LatencyComparison `json:"comparisons"`
	Summary     struct {
		TotalPairs        int     `json:"total_pairs"`
		AvgImprovementPct float64 `json:"avg_improvement_pct"`
		MaxImprovementPct float64 `json:"max_improvement_pct"`
		PairsWithData     int     `json:"pairs_with_data"`
	} `json:"summary"`
}

func (a *API) GetLatencyComparison(w http.ResponseWriter, r *http.Request) {
	// Try cache first (cache only holds mainnet data)
	if isMainnet(r.Context()) {
		if data, err := a.readPageCache(r.Context(), "latency_comparison"); err == nil {
			w.Header().Set("X-Cache", "HIT")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(data)
			return
		}
	}

	// Cache miss - fetch fresh data
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	start := time.Now()

	// Query the pre-built comparison view
	query := `
		SELECT
			m1.pk AS origin_metro_pk,
			c.origin_metro AS origin_metro_code,
			c.origin_metro_name,
			m2.pk AS target_metro_pk,
			c.target_metro AS target_metro_code,
			c.target_metro_name,
			c.dz_avg_rtt_ms,
			c.dz_p95_rtt_ms,
			c.dz_avg_jitter_ms,
			c.dz_loss_pct,
			c.dz_sample_count,
			c.internet_avg_rtt_ms,
			c.internet_p95_rtt_ms,
			c.internet_avg_jitter_ms,
			c.internet_sample_count,
			c.rtt_improvement_pct,
			c.jitter_improvement_pct
		FROM dz_vs_internet_latency_comparison c
		JOIN dz_metros_current m1 ON c.origin_metro = m1.code
		JOIN dz_metros_current m2 ON c.target_metro = m2.code
		WHERE c.dz_sample_count > 0
		  AND isFinite(c.dz_avg_rtt_ms)
		  AND isFinite(c.internet_avg_rtt_ms)
		ORDER BY c.origin_metro, c.target_metro
	`

	rows, err := a.envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("latency comparison query error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var comparisons []LatencyComparison
	var totalImprovement float64
	var maxImprovement float64
	var pairsWithData int

	for rows.Next() {
		var lc LatencyComparison
		if err := rows.Scan(
			&lc.OriginMetroPK,
			&lc.OriginMetroCode,
			&lc.OriginMetroName,
			&lc.TargetMetroPK,
			&lc.TargetMetroCode,
			&lc.TargetMetroName,
			&lc.DzAvgRttMs,
			&lc.DzP95RttMs,
			&lc.DzAvgJitterMs,
			&lc.DzLossPct,
			&lc.DzSampleCount,
			&lc.InternetAvgRttMs,
			&lc.InternetP95RttMs,
			&lc.InternetAvgJitterMs,
			&lc.InternetSampleCount,
			&lc.RttImprovementPct,
			&lc.JitterImprovementPct,
		); err != nil {
			logError("latency comparison scan error", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}

		if lc.RttImprovementPct != nil {
			pairsWithData++
			totalImprovement += *lc.RttImprovementPct
			if *lc.RttImprovementPct > maxImprovement {
				maxImprovement = *lc.RttImprovementPct
			}
		}

		comparisons = append(comparisons, lc)
	}

	if err := rows.Err(); err != nil {
		logError("latency comparison rows error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	if comparisons == nil {
		comparisons = []LatencyComparison{}
	}

	avgImprovement := 0.0
	if pairsWithData > 0 {
		avgImprovement = totalImprovement / float64(pairsWithData)
	}

	response := LatencyComparisonResponse{
		Comparisons: comparisons,
	}
	response.Summary.TotalPairs = len(comparisons)
	response.Summary.AvgImprovementPct = avgImprovement
	response.Summary.MaxImprovementPct = maxImprovement
	response.Summary.PairsWithData = pairsWithData

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// FetchLatencyComparisonData fetches DZ vs Internet latency comparison data.
// Used by both the handler and the cache.
func (a *API) FetchLatencyComparisonData(ctx context.Context) (*LatencyComparisonResponse, error) {
	start := time.Now()

	query := `
		SELECT
			m1.pk AS origin_metro_pk,
			c.origin_metro AS origin_metro_code,
			c.origin_metro_name,
			m2.pk AS target_metro_pk,
			c.target_metro AS target_metro_code,
			c.target_metro_name,
			c.dz_avg_rtt_ms,
			c.dz_p95_rtt_ms,
			c.dz_avg_jitter_ms,
			c.dz_loss_pct,
			c.dz_sample_count,
			c.internet_avg_rtt_ms,
			c.internet_p95_rtt_ms,
			c.internet_avg_jitter_ms,
			c.internet_sample_count,
			c.rtt_improvement_pct,
			c.jitter_improvement_pct
		FROM dz_vs_internet_latency_comparison c
		JOIN dz_metros_current m1 ON c.origin_metro = m1.code
		JOIN dz_metros_current m2 ON c.target_metro = m2.code
		WHERE c.dz_sample_count > 0
		  AND isFinite(c.dz_avg_rtt_ms)
		  AND isFinite(c.internet_avg_rtt_ms)
		ORDER BY c.origin_metro, c.target_metro
	`

	rows, err := a.envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var comparisons []LatencyComparison
	var totalImprovement float64
	var maxImprovement float64
	var pairsWithData int

	for rows.Next() {
		var lc LatencyComparison
		if err := rows.Scan(
			&lc.OriginMetroPK,
			&lc.OriginMetroCode,
			&lc.OriginMetroName,
			&lc.TargetMetroPK,
			&lc.TargetMetroCode,
			&lc.TargetMetroName,
			&lc.DzAvgRttMs,
			&lc.DzP95RttMs,
			&lc.DzAvgJitterMs,
			&lc.DzLossPct,
			&lc.DzSampleCount,
			&lc.InternetAvgRttMs,
			&lc.InternetP95RttMs,
			&lc.InternetAvgJitterMs,
			&lc.InternetSampleCount,
			&lc.RttImprovementPct,
			&lc.JitterImprovementPct,
		); err != nil {
			return nil, err
		}

		if lc.RttImprovementPct != nil {
			pairsWithData++
			totalImprovement += *lc.RttImprovementPct
			if *lc.RttImprovementPct > maxImprovement {
				maxImprovement = *lc.RttImprovementPct
			}
		}

		comparisons = append(comparisons, lc)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if comparisons == nil {
		comparisons = []LatencyComparison{}
	}

	avgImprovement := 0.0
	if pairsWithData > 0 {
		avgImprovement = totalImprovement / float64(pairsWithData)
	}

	response := &LatencyComparisonResponse{
		Comparisons: comparisons,
	}
	response.Summary.TotalPairs = len(comparisons)
	response.Summary.AvgImprovementPct = avgImprovement
	response.Summary.MaxImprovementPct = maxImprovement
	response.Summary.PairsWithData = pairsWithData

	return response, nil
}

// Latency history time series point
type LatencyHistoryPoint struct {
	Timestamp       time.Time `json:"timestamp"`
	DzAvgRttMs      *float64  `json:"dz_avg_rtt_ms"`
	DzAvgJitterMs   *float64  `json:"dz_avg_jitter_ms"`
	DzSampleCount   uint64    `json:"dz_sample_count"`
	InetAvgRttMs    *float64  `json:"inet_avg_rtt_ms"`
	InetAvgJitterMs *float64  `json:"inet_avg_jitter_ms"`
	InetSampleCount uint64    `json:"inet_sample_count"`
}

type LatencyHistoryResponse struct {
	OriginMetroCode string                `json:"origin_metro_code"`
	TargetMetroCode string                `json:"target_metro_code"`
	Points          []LatencyHistoryPoint `json:"points"`
}

// GetLatencyHistory returns time-series latency data for a specific metro pair
func (a *API) GetLatencyHistory(w http.ResponseWriter, r *http.Request) {
	originCode := chi.URLParam(r, "origin")
	targetCode := chi.URLParam(r, "target")

	if originCode == "" || targetCode == "" {
		http.Error(w, "origin and target metro codes required", http.StatusBadRequest)
		return
	}

	// Normalize the metro pair (alphabetical order to match the view)
	metro1, metro2 := originCode, targetCode
	if metro2 < metro1 {
		metro1, metro2 = metro2, metro1
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	start := time.Now()

	// Fixed 24h window with 30-minute buckets (48 points).
	// DZ side uses link_rollup_5m (re-aggregated to 30m); internet side uses raw fact table.
	query := `
		WITH
		time_buckets AS (
			SELECT toStartOfInterval(
				now() - INTERVAL number * 30 MINUTE, INTERVAL 30 MINUTE
			) AS bucket
			FROM numbers(48)
		),
		dz_data AS (
			SELECT
				toStartOfInterval(r.bucket_ts, INTERVAL 30 MINUTE) AS bucket,
				round(AVG((r.a_avg_rtt_us + r.z_avg_rtt_us) / 2.0) / 1000.0, 2) AS avg_rtt_ms,
				round(AVG((r.a_avg_jitter_us + r.z_avg_jitter_us) / 2.0) / 1000.0, 2) AS avg_jitter_ms,
				SUM(r.a_samples + r.z_samples) AS sample_count
			FROM link_rollup_5m r
			JOIN dz_links_current l ON r.link_pk = l.pk
			JOIN dz_devices_current da ON l.side_a_pk = da.pk
			JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
			JOIN dz_metros_current ma ON da.metro_pk = ma.pk
			JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
			WHERE r.bucket_ts >= now() - INTERVAL 24 HOUR
				AND r.link_pk != ''
				AND least(ma.code, mz.code) = $1
				AND greatest(ma.code, mz.code) = $2
			GROUP BY bucket
		),
		inet_data AS (
			SELECT
				toStartOfInterval(if(ih.sampling_interval_us > 0 AND f.sample_index >= ih.latest_sample_index - 1000, f.ingested_at, f.event_ts), INTERVAL 30 MINUTE) AS bucket,
				round(avg(f.rtt_us) / 1000.0, 2) AS avg_rtt_ms,
				round(avg(f.ipdv_us) / 1000.0, 2) AS avg_jitter_ms,
				count() AS sample_count
			FROM fact_dz_internet_metro_latency f
			LEFT JOIN (
				SELECT origin_metro_pk, target_metro_pk, data_provider AS _hdr_data_provider, epoch,
					   latest_sample_index, sampling_interval_us
				FROM fact_dz_internet_metro_latency_sample_header
			) ih ON f.origin_metro_pk = ih.origin_metro_pk
				AND f.target_metro_pk = ih.target_metro_pk
				AND f.data_provider = ih._hdr_data_provider
				AND f.epoch = ih.epoch
			JOIN dz_metros_current ma ON f.origin_metro_pk = ma.pk
			JOIN dz_metros_current mz ON f.target_metro_pk = mz.pk
			WHERE f.ingested_at >= now() - INTERVAL 24 HOUR
				AND least(ma.code, mz.code) = $1
				AND greatest(ma.code, mz.code) = $2
			GROUP BY bucket
		)
		SELECT
			tb.bucket AS timestamp,
			dz.avg_rtt_ms AS dz_avg_rtt_ms,
			dz.avg_jitter_ms AS dz_avg_jitter_ms,
			COALESCE(dz.sample_count, 0) AS dz_sample_count,
			inet.avg_rtt_ms AS inet_avg_rtt_ms,
			inet.avg_jitter_ms AS inet_avg_jitter_ms,
			COALESCE(inet.sample_count, 0) AS inet_sample_count
		FROM time_buckets tb
		LEFT JOIN dz_data dz ON tb.bucket = dz.bucket
		LEFT JOIN inet_data inet ON tb.bucket = inet.bucket
		WHERE tb.bucket >= now() - INTERVAL 24 HOUR
		  AND tb.bucket < toStartOfInterval(now(), INTERVAL 30 MINUTE)
		ORDER BY tb.bucket ASC
	`

	rows, err := a.envDB(ctx).Query(ctx, query, metro1, metro2)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("latency history query error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var points []LatencyHistoryPoint
	for rows.Next() {
		var p LatencyHistoryPoint
		if err := rows.Scan(
			&p.Timestamp,
			&p.DzAvgRttMs,
			&p.DzAvgJitterMs,
			&p.DzSampleCount,
			&p.InetAvgRttMs,
			&p.InetAvgJitterMs,
			&p.InetSampleCount,
		); err != nil {
			logError("latency history scan error", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		points = append(points, p)
	}

	if err := rows.Err(); err != nil {
		logError("latency history rows error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	if points == nil {
		points = []LatencyHistoryPoint{}
	}

	response := LatencyHistoryResponse{
		OriginMetroCode: originCode,
		TargetMetroCode: targetCode,
		Points:          points,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}
