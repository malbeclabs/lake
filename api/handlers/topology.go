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

func GetTopology(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	response, err := fetchTopologyData(ctx)
	if err != nil && dberror.IsTransient(err) {
		cancel()
		var retryCancel context.CancelFunc
		ctx, retryCancel = context.WithTimeout(r.Context(), 10*time.Second)
		defer retryCancel()
		response, err = fetchTopologyData(ctx)
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
		slog.Error("failed to encode response", "error", err)
	}
}

// fetchTopologyData performs the actual topology data fetch from the database.
func fetchTopologyData(ctx context.Context) (TopologyResponse, error) {
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
		rows, err := envDB(ctx).Query(ctx, query)
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
		rows, err := envDB(ctx).Query(ctx, query)
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
				slog.Error("failed to parse interfaces JSON", "device_pk", d.PK, "error", err)
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
					avg(rtt_us) as avg_rtt_us,
					avg(abs(ipdv_us)) as avg_ipdv_us,
					countIf(loss) * 100.0 / count(*) as loss_percent,
					count(*) as sample_count
				FROM fact_dz_device_link_latency
				WHERE event_ts > now() - INTERVAL 3 HOUR
				GROUP BY link_pk
			) lat ON l.pk = lat.link_pk
			LEFT JOIN (
				SELECT link_pk, origin_device_pk,
					avg(rtt_us) as avg_rtt_us,
					avg(abs(ipdv_us)) as avg_ipdv_us
				FROM fact_dz_device_link_latency
				WHERE event_ts > now() - INTERVAL 3 HOUR
				GROUP BY link_pk, origin_device_pk
			) lat_a ON l.pk = lat_a.link_pk AND l.side_a_pk = lat_a.origin_device_pk
			LEFT JOIN (
				SELECT link_pk, origin_device_pk,
					avg(rtt_us) as avg_rtt_us,
					avg(abs(ipdv_us)) as avg_ipdv_us
				FROM fact_dz_device_link_latency
				WHERE event_ts > now() - INTERVAL 3 HOUR
				GROUP BY link_pk, origin_device_pk
			) lat_z ON l.pk = lat_z.link_pk AND l.side_z_pk = lat_z.origin_device_pk
			LEFT JOIN (
				SELECT link_pk,
					CASE WHEN SUM(delta_duration) > 0 THEN SUM(in_octets_delta) * 8 / SUM(delta_duration) ELSE 0 END as in_bps,
					CASE WHEN SUM(delta_duration) > 0 THEN SUM(out_octets_delta) * 8 / SUM(delta_duration) ELSE 0 END as out_bps
				FROM fact_dz_device_interface_counters
				WHERE event_ts > now() - INTERVAL 5 MINUTE
					AND link_pk != ''
					AND delta_duration > 0
					AND in_octets_delta >= 0
					AND out_octets_delta >= 0
				GROUP BY link_pk
			) traffic ON l.pk = traffic.link_pk
			WHERE l.status = 'activated'
		`
		rows, err := envDB(ctx).Query(ctx, query)
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
		rows, err := envDB(ctx).Query(ctx, query)
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

func GetTopologyTraffic(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	itemType := r.URL.Query().Get("type")
	pk := r.URL.Query().Get("pk")

	if pk == "" || (itemType != "link" && itemType != "device" && itemType != "validator") {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(TrafficResponse{Points: []TrafficDataPoint{}})
		return
	}

	// Parse time range parameters
	rangeParam := r.URL.Query().Get("range")
	fromParam := r.URL.Query().Get("from")
	toParam := r.URL.Query().Get("to")
	bucketParam := r.URL.Query().Get("bucket")
	metricParam := r.URL.Query().Get("metric") // "packets" for pps, default is bps

	var timeFilter string
	var bucketSeconds int
	var timeFormat string
	isPresetRange := false

	if fromParam != "" && toParam != "" {
		fromTime, err := time.Parse("2006-01-02-15:04:05", fromParam)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(TrafficResponse{Error: "invalid 'from' format, use yyyy-mm-dd-hh:mm:ss"})
			return
		}
		toTime, err := time.Parse("2006-01-02-15:04:05", toParam)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(TrafficResponse{Error: "invalid 'to' format, use yyyy-mm-dd-hh:mm:ss"})
			return
		}
		duration := toTime.Sub(fromTime)
		bucketSeconds = calculateBucketSize(duration)
		timeFormat = timeFormatForBucket(bucketSeconds)
		timeFilter = fmt.Sprintf("event_ts >= '%s' AND event_ts <= '%s'",
			fromTime.UTC().Format("2006-01-02 15:04:05"),
			toTime.UTC().Format("2006-01-02 15:04:05"))
	} else {
		isPresetRange = true
		var intervalMinutes int
		switch rangeParam {
		case "15m":
			intervalMinutes = 15
		case "30m":
			intervalMinutes = 30
		case "1h":
			intervalMinutes = 60
		case "3h":
			intervalMinutes = 180
		case "6h":
			intervalMinutes = 360
		case "12h":
			intervalMinutes = 720
		case "2d":
			intervalMinutes = 2880
		case "7d":
			intervalMinutes = 10080
		default: // 24h
			intervalMinutes = 1440
		}
		bucketSeconds = calculateBucketSize(time.Duration(intervalMinutes) * time.Minute)
		timeFormat = timeFormatForBucket(bucketSeconds)
		timeFilter = fmt.Sprintf("event_ts > now() - INTERVAL %d MINUTE", intervalMinutes)
	}

	// Allow client to override auto bucket size
	if bucketParam != "" && bucketParam != "auto" {
		override := parseBucketParam(bucketParam)
		if override > 0 {
			bucketSeconds = override
			timeFormat = timeFormatForBucket(bucketSeconds)
		}
	}

	// Exclude the current incomplete bucket for preset ranges so the chart
	// line doesn't drop to zero at the trailing edge.
	if isPresetRange {
		timeFilter += fmt.Sprintf(" AND event_ts < toStartOfInterval(now(), INTERVAL %d SECOND)", bucketSeconds)
	}

	bucketExpr := fmt.Sprintf("toStartOfInterval(event_ts, INTERVAL %d SECOND)", bucketSeconds)

	start := time.Now()

	breakdown := r.URL.Query().Get("breakdown")
	var points []TrafficDataPoint
	var whereColumn string

	switch itemType {
	case "link":
		whereColumn = "link_pk"
	case "validator":
		whereColumn = "user_tunnel_id"
	default:
		whereColumn = "device_pk"
	}

	// Select metric expressions based on metric param
	var avgInExpr, avgOutExpr, peakInExpr, peakOutExpr string
	if metricParam == "packets" {
		avgInExpr = "avg(in_pkts_delta / nullIf(delta_duration, 0))"
		avgOutExpr = "avg(out_pkts_delta / nullIf(delta_duration, 0))"
		peakInExpr = "max(in_pkts_delta / nullIf(delta_duration, 0))"
		peakOutExpr = "max(out_pkts_delta / nullIf(delta_duration, 0))"
	} else {
		avgInExpr = "avg(in_octets_delta * 8 / nullIf(delta_duration, 0))"
		avgOutExpr = "avg(out_octets_delta * 8 / nullIf(delta_duration, 0))"
		peakInExpr = "max(in_octets_delta * 8 / nullIf(delta_duration, 0))"
		peakOutExpr = "max(out_octets_delta * 8 / nullIf(delta_duration, 0))"
	}

	// Skip aggregated query when only interface breakdown is needed — the caller
	// doesn't use the points array and running both queries sequentially under
	// the same timeout budget causes unnecessary timeouts.
	if breakdown != "interface" || (itemType != "device" && itemType != "link") {
		query := fmt.Sprintf(`
			SELECT
				formatDateTime(%s, '%s') as time_bucket,
				%s as avg_in,
				%s as avg_out,
				%s as peak_in,
				%s as peak_out
			FROM fact_dz_device_interface_counters
			WHERE %s
				AND %s = $1
				AND delta_duration > 0
				AND in_octets_delta >= 0
				AND out_octets_delta >= 0
			GROUP BY time_bucket
			ORDER BY min(event_ts)
		`, bucketExpr, timeFormat, avgInExpr, avgOutExpr, peakInExpr, peakOutExpr, timeFilter, whereColumn)

		rows, err := envDB(ctx).Query(ctx, query, pk)
		if err != nil {
			slog.Error("traffic query error", "error", err)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(TrafficResponse{Error: dberror.UserMessage(err)})
			return
		}
		defer rows.Close()

		for rows.Next() {
			var p TrafficDataPoint
			var avgIn, avgOut, peakIn, peakOut *float64
			if err := rows.Scan(&p.Time, &avgIn, &avgOut, &peakIn, &peakOut); err != nil {
				slog.Error("traffic scan error", "error", err)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(TrafficResponse{Error: dberror.UserMessage(err)})
				return
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

		duration := time.Since(start)
		metrics.RecordClickHouseQuery(duration, rows.Err())
	}

	if points == nil {
		points = []TrafficDataPoint{}
	}

	response := TrafficResponse{Points: points}

	// If breakdown=interface requested and type is device or link, run per-interface query
	if breakdown == "interface" && (itemType == "device" || itemType == "link") {
		// For links, prefix intf with link_side ('A:'/'Z:') to distinguish sides
		// that may share the same interface name (e.g. both sides have Ethernet1/1)
		intfExpr := "intf"
		if itemType == "link" {
			intfExpr = "concat(link_side, ':', intf)"
		}
		intfQuery := fmt.Sprintf(`
			SELECT
				formatDateTime(%s, '%s') as time_bucket,
				%s as intf_key,
				%s as avg_in,
				%s as avg_out,
				%s as peak_in,
				%s as peak_out
			FROM fact_dz_device_interface_counters
			WHERE %s
				AND %s = $1
				AND delta_duration > 0
				AND in_octets_delta >= 0
				AND out_octets_delta >= 0
			GROUP BY time_bucket, intf_key
			ORDER BY min(event_ts), intf_key
		`, bucketExpr, timeFormat, intfExpr, avgInExpr, avgOutExpr, peakInExpr, peakOutExpr, timeFilter, whereColumn)

		intfRows, intfErr := envDB(ctx).Query(ctx, intfQuery, pk)
		if intfErr != nil {
			slog.Error("interface traffic query error", "error", intfErr)
		} else {
			defer intfRows.Close()
			var intfPoints []InterfaceTrafficDataPoint
			for intfRows.Next() {
				var p InterfaceTrafficDataPoint
				var avgIn, avgOut, peakIn, peakOut *float64
				if err := intfRows.Scan(&p.Time, &p.Intf, &avgIn, &avgOut, &peakIn, &peakOut); err != nil {
					slog.Error("interface traffic scan error", "error", err)
					break
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
				intfPoints = append(intfPoints, p)
			}
			response.Interfaces = intfPoints
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
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

// parseBucketParam converts a bucket size string like "10 SECOND", "1 MINUTE", "1 HOUR" to seconds.
func parseBucketParam(bucket string) int {
	switch bucket {
	case "10 SECOND":
		return 10
	case "30 SECOND":
		return 30
	case "1 MINUTE":
		return 60
	case "5 MINUTE":
		return 300
	case "10 MINUTE":
		return 600
	case "30 MINUTE":
		return 1800
	case "1 HOUR":
		return 3600
	default:
		return 0
	}
}

// calculateBucketSize returns an appropriate bucket size in seconds for the given duration,
// matching the traffic dashboard's granularity.
func calculateBucketSize(d time.Duration) int {
	switch {
	case d <= 1*time.Hour:
		return 10 // 10 second buckets
	case d <= 3*time.Hour:
		return 30 // 30 second buckets
	case d <= 12*time.Hour:
		return 60 // 1 minute buckets
	case d <= 24*time.Hour:
		return 300 // 5 minute buckets
	case d <= 3*24*time.Hour:
		return 600 // 10 minute buckets
	case d <= 7*24*time.Hour:
		return 1800 // 30 minute buckets
	default:
		return 3600 // 1 hour buckets
	}
}

// timeFormatForBucket returns the ClickHouse time format string for API responses.
// Always returns ISO 8601 format so the frontend can parse timestamps and format them locally.
func timeFormatForBucket(_ int) string {
	return "%Y-%m-%dT%H:%i:%S"
}

func GetLinkLatencyHistory(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	pk := r.URL.Query().Get("pk")

	if pk == "" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(LinkLatencyResponse{Points: []LinkLatencyDataPoint{}})
		return
	}

	// Parse time range parameters
	rangeParam := r.URL.Query().Get("range")
	fromParam := r.URL.Query().Get("from")
	toParam := r.URL.Query().Get("to")

	// Determine time filter and bucket size
	var timeFilter string
	var bucketSeconds int
	var timeFormat string

	if fromParam != "" && toParam != "" {
		// Custom date range: from=2024-01-20-14:30:00 to=2024-01-21-14:30:00
		fromTime, err := time.Parse("2006-01-02-15:04:05", fromParam)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(LinkLatencyResponse{Error: "invalid 'from' format, use yyyy-mm-dd-hh:mm:ss"})
			return
		}
		toTime, err := time.Parse("2006-01-02-15:04:05", toParam)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(LinkLatencyResponse{Error: "invalid 'to' format, use yyyy-mm-dd-hh:mm:ss"})
			return
		}
		duration := toTime.Sub(fromTime)
		bucketSeconds = calculateBucketSize(duration)
		timeFormat = timeFormatForBucket(bucketSeconds)
		timeFilter = fmt.Sprintf("f.event_ts >= '%s' AND f.event_ts <= '%s'",
			fromTime.UTC().Format("2006-01-02 15:04:05"),
			toTime.UTC().Format("2006-01-02 15:04:05"))
	} else {
		// Preset range
		var intervalMinutes int
		switch rangeParam {
		case "15m":
			intervalMinutes = 15
		case "30m":
			intervalMinutes = 30
		case "1h":
			intervalMinutes = 60
		case "3h":
			intervalMinutes = 180
		case "6h":
			intervalMinutes = 360
		case "12h":
			intervalMinutes = 720
		case "2d":
			intervalMinutes = 2880
		case "7d":
			intervalMinutes = 10080
		default: // 24h
			intervalMinutes = 1440
		}
		bucketSeconds = calculateBucketSize(time.Duration(intervalMinutes) * time.Minute)
		timeFormat = timeFormatForBucket(bucketSeconds)
		// Exclude the current incomplete bucket so the chart line doesn't drop to zero.
		timeFilter = fmt.Sprintf("f.event_ts > now() - INTERVAL %d MINUTE AND f.event_ts < toStartOfInterval(now(), INTERVAL %d SECOND)", intervalMinutes, bucketSeconds)
	}

	start := time.Now()

	// Get latency stats for a link with per-direction breakdown.
	// Loss is computed as the max of 5-minute sub-bucket loss percentages within each
	// display bucket, matching Grafana's [5m] window for sharper spike visibility.
	displayBucketExpr := fmt.Sprintf("toStartOfInterval(f.event_ts, INTERVAL %d SECOND)", bucketSeconds)
	lossBucketExpr := fmt.Sprintf("toStartOfInterval(f.event_ts, INTERVAL %d SECOND)", min(bucketSeconds, 300))
	query := `
		WITH loss_sub AS (
			SELECT
				` + displayBucketExpr + ` as display_bucket,
				countIf(f.loss) * 100.0 / count(*) as loss_pct
			FROM fact_dz_device_link_latency f
			JOIN dz_links_current l ON f.link_pk = l.pk
			WHERE ` + timeFilter + `
				AND f.link_pk = $1
			GROUP BY display_bucket, ` + lossBucketExpr + `
		),
		loss_max AS (
			SELECT display_bucket, max(loss_pct) as loss_pct
			FROM loss_sub
			GROUP BY display_bucket
		)
		SELECT
			formatDateTime(` + displayBucketExpr + `, '` + timeFormat + `') as time_bucket,
			avg(f.rtt_us) / 1000.0 as avg_rtt_ms,
			quantile(0.95)(f.rtt_us) / 1000.0 as p95_rtt_ms,
			avg(abs(f.ipdv_us)) / 1000.0 as avg_jitter_ms,
			COALESCE(max(lm.loss_pct), 0) as loss_pct,
			avgIf(f.rtt_us, f.origin_device_pk = l.side_a_pk) / 1000.0 as avg_rtt_a_to_z_ms,
			quantileIf(0.95)(f.rtt_us, f.origin_device_pk = l.side_a_pk) / 1000.0 as p95_rtt_a_to_z_ms,
			avgIf(f.rtt_us, f.origin_device_pk = l.side_z_pk) / 1000.0 as avg_rtt_z_to_a_ms,
			quantileIf(0.95)(f.rtt_us, f.origin_device_pk = l.side_z_pk) / 1000.0 as p95_rtt_z_to_a_ms,
			avgIf(abs(f.ipdv_us), f.origin_device_pk = l.side_a_pk) / 1000.0 as jitter_a_to_z_ms,
			avgIf(abs(f.ipdv_us), f.origin_device_pk = l.side_z_pk) / 1000.0 as jitter_z_to_a_ms
		FROM fact_dz_device_link_latency f
		JOIN dz_links_current l ON f.link_pk = l.pk
		LEFT JOIN loss_max lm ON lm.display_bucket = ` + displayBucketExpr + `
		WHERE ` + timeFilter + `
			AND f.link_pk = $2
		GROUP BY ` + displayBucketExpr + `
		ORDER BY ` + displayBucketExpr

	rows, err := envDB(ctx).Query(ctx, query, pk, pk)
	if err != nil {
		slog.Error("latency query error", "pk", pk, "error", err)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(LinkLatencyResponse{Error: dberror.UserMessage(err)})
		return
	}
	defer rows.Close()

	var points []LinkLatencyDataPoint
	for rows.Next() {
		var p LinkLatencyDataPoint
		var avgRtt, p95Rtt, avgJitter, lossPct, avgRttAtoZ, p95RttAtoZ, avgRttZtoA, p95RttZtoA, jitterAtoZ, jitterZtoA *float64
		if err := rows.Scan(&p.Time, &avgRtt, &p95Rtt, &avgJitter, &lossPct, &avgRttAtoZ, &p95RttAtoZ, &avgRttZtoA, &p95RttZtoA, &jitterAtoZ, &jitterZtoA); err != nil {
			slog.Error("latency scan error", "error", err)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(LinkLatencyResponse{Error: dberror.UserMessage(err)})
			return
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

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, rows.Err())

	if err := rows.Err(); err != nil {
		slog.Error("latency rows iteration error", "pk", pk, "error", err)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(LinkLatencyResponse{Error: dberror.UserMessage(err)})
		return
	}

	if points == nil {
		points = []LinkLatencyDataPoint{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(LinkLatencyResponse{Points: points}); err != nil {
		slog.Error("failed to encode response", "pk", pk, "error", err)
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

func GetLatencyComparison(w http.ResponseWriter, r *http.Request) {
	// Try cache first (cache only holds mainnet data)
	if isMainnet(r.Context()) && pageCache != nil {
		if cached := pageCache.GetLatencyComparison(); cached != nil {
			w.Header().Set("X-Cache", "HIT")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(cached)
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
		ORDER BY c.origin_metro, c.target_metro
	`

	rows, err := envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		slog.Error("latency comparison query error", "error", err)
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
			slog.Error("latency comparison scan error", "error", err)
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
		slog.Error("latency comparison rows error", "error", err)
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

// fetchLatencyComparisonData fetches DZ vs Internet latency comparison data.
// Used by both the handler and the cache.
func fetchLatencyComparisonData(ctx context.Context) (*LatencyComparisonResponse, error) {
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
		ORDER BY c.origin_metro, c.target_metro
	`

	rows, err := envDB(ctx).Query(ctx, query)
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
func GetLatencyHistory(w http.ResponseWriter, r *http.Request) {
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

	// Parse time range (default 24h)
	timeRange := r.URL.Query().Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}

	var intervalHours int
	switch timeRange {
	case "1h":
		intervalHours = 1
	case "6h":
		intervalHours = 6
	case "12h":
		intervalHours = 12
	case "3d":
		intervalHours = 72
	case "7d":
		intervalHours = 168
	default:
		intervalHours = 24
	}

	// Calculate bucket size (aim for ~48 points)
	bucketMinutes := (intervalHours * 60) / 48
	if bucketMinutes < 5 {
		bucketMinutes = 5
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	start := time.Now()

	// Query to get time-bucketed latency data for both DZ and Internet
	query := fmt.Sprintf(`
		WITH
		lookback AS (
			SELECT now() - INTERVAL %d HOUR AS min_ts
		),
		time_buckets AS (
			SELECT
				toStartOfInterval(event_ts, INTERVAL %d MINUTE) AS bucket
			FROM (
				SELECT arrayJoin(
					arrayMap(
						x -> now() - INTERVAL x * %d MINUTE,
						range(0, %d)
					)
				) AS event_ts
			)
		),
		dz_data AS (
			SELECT
				toStartOfInterval(f.event_ts, INTERVAL %d MINUTE) AS bucket,
				round(avg(f.rtt_us) / 1000.0, 2) AS avg_rtt_ms,
				round(avg(f.ipdv_us) / 1000.0, 2) AS avg_jitter_ms,
				count() AS sample_count
			FROM fact_dz_device_link_latency f
			CROSS JOIN lookback
			JOIN dz_links_current l ON f.link_pk = l.pk
			JOIN dz_devices_current da ON l.side_a_pk = da.pk
			JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
			JOIN dz_metros_current ma ON da.metro_pk = ma.pk
			JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
			WHERE f.event_ts >= lookback.min_ts
				AND f.link_pk != ''
				AND f.loss = false
				AND least(ma.code, mz.code) = $1
				AND greatest(ma.code, mz.code) = $2
			GROUP BY bucket
		),
		inet_data AS (
			SELECT
				toStartOfInterval(f.event_ts, INTERVAL %d MINUTE) AS bucket,
				round(avg(f.rtt_us) / 1000.0, 2) AS avg_rtt_ms,
				round(avg(f.ipdv_us) / 1000.0, 2) AS avg_jitter_ms,
				count() AS sample_count
			FROM fact_dz_internet_metro_latency f
			CROSS JOIN lookback
			JOIN dz_metros_current ma ON f.origin_metro_pk = ma.pk
			JOIN dz_metros_current mz ON f.target_metro_pk = mz.pk
			WHERE f.event_ts >= lookback.min_ts
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
		WHERE tb.bucket >= now() - INTERVAL %d HOUR
		  AND tb.bucket < toStartOfInterval(now(), INTERVAL %d MINUTE)
		ORDER BY tb.bucket ASC
	`, intervalHours, bucketMinutes, bucketMinutes, 48, bucketMinutes, bucketMinutes, intervalHours, bucketMinutes)

	rows, err := envDB(ctx).Query(ctx, query, metro1, metro2)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		slog.Error("latency history query error", "error", err)
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
			slog.Error("latency history scan error", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		points = append(points, p)
	}

	if err := rows.Err(); err != nil {
		slog.Error("latency history rows error", "error", err)
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
