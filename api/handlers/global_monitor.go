package handlers

import (
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"golang.org/x/sync/errgroup"
)

// gmDB returns the backtick-quoted global monitor database name.
func gmDB(a *API) string {
	return fmt.Sprintf("`%s`", a.GlobalMonitorDB)
}

// gmValidatorTable returns the table name for the given probe type.
func gmValidatorTable(probeType string) string {
	switch strings.ToLower(probeType) {
	case "tpuquic":
		return "solana_validator_tpuquic_probe"
	default:
		return "solana_validator_icmp_probe"
	}
}

// gmTimeRange returns the ClickHouse INTERVAL expression for the given range param.
func gmTimeRange(r *http.Request) string {
	switch r.URL.Query().Get("range") {
	case "1h":
		return "1 HOUR"
	case "3h":
		return "3 HOUR"
	case "6h":
		return "6 HOUR"
	case "12h":
		return "12 HOUR"
	case "3d":
		return "3 DAY"
	case "7d":
		return "7 DAY"
	default:
		return "24 HOUR"
	}
}

// gmBucketInterval returns a suitable bucket interval for the given range.
func gmBucketInterval(r *http.Request) string {
	switch r.URL.Query().Get("range") {
	case "1h":
		return "1 MINUTE"
	case "3h":
		return "5 MINUTE"
	case "6h":
		return "10 MINUTE"
	case "12h":
		return "15 MINUTE"
	case "3d":
		return "1 HOUR"
	case "7d":
		return "4 HOUR"
	default:
		return "30 MINUTE"
	}
}

// --- Response types ---

type GMTimePoint struct {
	Time    string  `json:"time"`
	DZValue float64 `json:"dz_value"`
	PIValue float64 `json:"pi_value"`
}

type GMValidatorItem struct {
	ValidatorPubkey   string  `json:"validator_pubkey"`
	StakeLamports     uint64  `json:"stake_lamports"`
	LeaderRatio       float64 `json:"leader_ratio"`
	TargetIP          string  `json:"target_ip"`
	Metro             string  `json:"metro"`
	Country           string  `json:"country"`
	City              string  `json:"city"`
	ASNOrg            string  `json:"asn_org"`
	DZDMetro          string  `json:"dzd_metro"`
	DZAvailabilityPct float64 `json:"dz_availability_pct"`
	PIAvailabilityPct float64 `json:"pi_availability_pct"`
	DZRttMs           float64 `json:"dz_rtt_ms"`
	PIRttMs           float64 `json:"pi_rtt_ms"`
	RttDeltaMs        float64 `json:"rtt_delta_ms"`
}

type GMValidatorsResponse struct {
	Items []GMValidatorItem `json:"items"`
	Total int               `json:"total"`
}

type GMValidatorSummary struct {
	TotalValidators  uint64  `json:"total_validators"`
	DZAvailablePct   float64 `json:"dz_available_pct"`
	DZBetterRttPct   float64 `json:"dz_better_rtt_pct"`
	MedianRttDeltaMs float64 `json:"median_rtt_delta_ms"`

	AvailabilityTS []GMTimePoint `json:"availability_ts"`
	RttTS          []GMTimePoint `json:"rtt_ts"`
}

type GMMetroBreakdown struct {
	SourceMetro string  `json:"source_metro"`
	DZRttMs     float64 `json:"dz_rtt_ms"`
	PIRttMs     float64 `json:"pi_rtt_ms"`
	DZAvailPct  float64 `json:"dz_avail_pct"`
	PIAvailPct  float64 `json:"pi_avail_pct"`
	ProbeCount  uint64  `json:"probe_count"`
}

type GMValidatorDetail struct {
	GMValidatorItem
	Region string `json:"region"`

	RttTS          []GMTimePoint    `json:"rtt_ts"`
	AvailabilityTS []GMTimePoint    `json:"availability_ts"`
	MetroBreakdown []GMMetroBreakdown `json:"metro_breakdown"`
}

// --- Validator handlers ---

func (a *API) GetGMValidators(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	db := gmDB(a)
	table := gmValidatorTable(r.URL.Query().Get("probe_type"))
	interval := gmTimeRange(r)

	query := fmt.Sprintf(`
		SELECT
			validator_pubkey,
			any(validator_stake_lamports) AS stake,
			any(validator_leader_ratio) AS leader_ratio,
			any(target_ip) AS target_ip,
			any(target_geoip_metro) AS metro,
			any(target_geoip_country) AS country,
			any(target_geoip_city) AS city,
			any(target_geoip_asn_org) AS asn_org,
			any(target_dzd_metro_name) AS dzd_metro,
			countIf(probe_ok = true AND probe_path = 'doublezero') * 100.0
				/ nullIf(countIf(probe_path = 'doublezero'), 0) AS dz_avail,
			countIf(probe_ok = true AND probe_path = 'public_internet') * 100.0
				/ nullIf(countIf(probe_path = 'public_internet'), 0) AS pi_avail,
			avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'doublezero') AS dz_rtt,
			avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'public_internet') AS pi_rtt
		FROM %s.%s
		WHERE timestamp >= now() - INTERVAL %s
		GROUP BY validator_pubkey
		ORDER BY stake DESC
	`, db, table, interval)

	rows, err := a.envDB(ctx).Query(ctx, query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var items []GMValidatorItem
	for rows.Next() {
		var v GMValidatorItem
		var dzAvail, piAvail, dzRtt, piRtt *float64
		if err := rows.Scan(
			&v.ValidatorPubkey, &v.StakeLamports, &v.LeaderRatio,
			&v.TargetIP, &v.Metro, &v.Country, &v.City, &v.ASNOrg, &v.DZDMetro,
			&dzAvail, &piAvail, &dzRtt, &piRtt,
		); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		v.DZAvailabilityPct = derefFloat(dzAvail)
		v.PIAvailabilityPct = derefFloat(piAvail)
		v.DZRttMs = derefFloat(dzRtt)
		v.PIRttMs = derefFloat(piRtt)
		v.RttDeltaMs = v.PIRttMs - v.DZRttMs
		items = append(items, v)
	}
	if items == nil {
		items = []GMValidatorItem{}
	}
	writeJSON(w, GMValidatorsResponse{Items: items, Total: len(items)})
}

func (a *API) GetGMValidatorsSummary(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	db := gmDB(a)
	table := gmValidatorTable(r.URL.Query().Get("probe_type"))
	interval := gmTimeRange(r)
	bucket := gmBucketInterval(r)

	g, gctx := errgroup.WithContext(ctx)

	var summary GMValidatorSummary

	// Aggregate stats
	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT
				count() AS total,
				avg(dz_avail) AS avg_dz_avail,
				sumIf(1, dz_rtt < pi_rtt) * 100.0 / nullIf(count(), 0) AS dz_better_pct,
				medianIf(pi_rtt - dz_rtt, isFinite(pi_rtt - dz_rtt)) AS median_delta
			FROM (
				SELECT
					validator_pubkey,
					countIf(probe_ok = true AND probe_path = 'doublezero') * 100.0
						/ nullIf(countIf(probe_path = 'doublezero'), 0) AS dz_avail,
					avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'doublezero') AS dz_rtt,
					avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'public_internet') AS pi_rtt
				FROM %s.%s
				WHERE timestamp >= now() - INTERVAL %s
				GROUP BY validator_pubkey
			)
		`, db, table, interval)

		rows, err := a.envDB(gctx).Query(gctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()

		if rows.Next() {
			var dzAvail, dzBetter, medianDelta *float64
			if err := rows.Scan(&summary.TotalValidators, &dzAvail, &dzBetter, &medianDelta); err != nil {
				return err
			}
			summary.DZAvailablePct = derefFloat(dzAvail)
			summary.DZBetterRttPct = derefFloat(dzBetter)
			summary.MedianRttDeltaMs = derefFloat(medianDelta)
		}
		return nil
	})

	// Time-bucketed availability
	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT
				toStartOfInterval(timestamp, INTERVAL %s) AS ts,
				countIf(probe_ok = true AND probe_path = 'doublezero') * 100.0
					/ nullIf(countIf(probe_path = 'doublezero'), 0) AS dz_avail,
				countIf(probe_ok = true AND probe_path = 'public_internet') * 100.0
					/ nullIf(countIf(probe_path = 'public_internet'), 0) AS pi_avail
			FROM %s.%s
			WHERE timestamp >= now() - INTERVAL %s
			GROUP BY ts
			ORDER BY ts
		`, bucket, db, table, interval)

		rows, err := a.envDB(gctx).Query(gctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var tp GMTimePoint
			var ts time.Time
			var dzVal, piVal *float64
			if err := rows.Scan(&ts, &dzVal, &piVal); err != nil {
				return err
			}
			tp.Time = ts.UTC().Format(time.RFC3339)
			tp.DZValue = derefFloat(dzVal)
			tp.PIValue = derefFloat(piVal)
			summary.AvailabilityTS = append(summary.AvailabilityTS, tp)
		}
		return nil
	})

	// Time-bucketed RTT
	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT
				toStartOfInterval(timestamp, INTERVAL %s) AS ts,
				avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'doublezero') AS dz_rtt,
				avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'public_internet') AS pi_rtt
			FROM %s.%s
			WHERE timestamp >= now() - INTERVAL %s
			GROUP BY ts
			ORDER BY ts
		`, bucket, db, table, interval)

		rows, err := a.envDB(gctx).Query(gctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var tp GMTimePoint
			var ts time.Time
			var dzVal, piVal *float64
			if err := rows.Scan(&ts, &dzVal, &piVal); err != nil {
				return err
			}
			tp.Time = ts.UTC().Format(time.RFC3339)
			tp.DZValue = derefFloat(dzVal)
			tp.PIValue = derefFloat(piVal)
			summary.RttTS = append(summary.RttTS, tp)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if summary.AvailabilityTS == nil {
		summary.AvailabilityTS = []GMTimePoint{}
	}
	if summary.RttTS == nil {
		summary.RttTS = []GMTimePoint{}
	}

	writeJSON(w, summary)
}

func (a *API) GetGMValidator(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pubkey := chi.URLParam(r, "pubkey")
	db := gmDB(a)
	table := gmValidatorTable(r.URL.Query().Get("probe_type"))
	interval := gmTimeRange(r)
	bucket := gmBucketInterval(r)

	g, gctx := errgroup.WithContext(ctx)

	var detail GMValidatorDetail

	// Validator info
	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT
				any(validator_stake_lamports),
				any(validator_leader_ratio),
				any(target_ip),
				any(target_geoip_metro),
				any(target_geoip_country),
				any(target_geoip_city),
				any(target_geoip_region),
				any(target_geoip_asn_org),
				any(target_dzd_metro_name),
				countIf(probe_ok = true AND probe_path = 'doublezero') * 100.0
					/ nullIf(countIf(probe_path = 'doublezero'), 0),
				countIf(probe_ok = true AND probe_path = 'public_internet') * 100.0
					/ nullIf(countIf(probe_path = 'public_internet'), 0),
				avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'doublezero'),
				avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'public_internet')
			FROM %s.%s
			WHERE timestamp >= now() - INTERVAL %s AND validator_pubkey = ?
		`, db, table, interval)

		rows, err := a.envDB(gctx).Query(gctx, query, pubkey)
		if err != nil {
			return err
		}
		defer rows.Close()

		if rows.Next() {
			var dzAvail, piAvail, dzRtt, piRtt *float64
			if err := rows.Scan(
				&detail.StakeLamports, &detail.LeaderRatio,
				&detail.TargetIP, &detail.Metro, &detail.Country,
				&detail.City, &detail.Region, &detail.ASNOrg, &detail.DZDMetro,
				&dzAvail, &piAvail, &dzRtt, &piRtt,
			); err != nil {
				return err
			}
			detail.ValidatorPubkey = pubkey
			detail.DZAvailabilityPct = derefFloat(dzAvail)
			detail.PIAvailabilityPct = derefFloat(piAvail)
			detail.DZRttMs = derefFloat(dzRtt)
			detail.PIRttMs = derefFloat(piRtt)
			detail.RttDeltaMs = detail.PIRttMs - detail.DZRttMs
		}
		return nil
	})

	// Time-bucketed RTT
	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT
				toStartOfInterval(timestamp, INTERVAL %s) AS ts,
				avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'doublezero') AS dz_rtt,
				avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'public_internet') AS pi_rtt
			FROM %s.%s
			WHERE timestamp >= now() - INTERVAL %s AND validator_pubkey = ?
			GROUP BY ts
			ORDER BY ts
		`, bucket, db, table, interval)

		rows, err := a.envDB(gctx).Query(gctx, query, pubkey)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var tp GMTimePoint
			var ts time.Time
			var dzVal, piVal *float64
			if err := rows.Scan(&ts, &dzVal, &piVal); err != nil {
				return err
			}
			tp.Time = ts.UTC().Format(time.RFC3339)
			tp.DZValue = derefFloat(dzVal)
			tp.PIValue = derefFloat(piVal)
			detail.RttTS = append(detail.RttTS, tp)
		}
		return nil
	})

	// Time-bucketed availability
	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT
				toStartOfInterval(timestamp, INTERVAL %s) AS ts,
				countIf(probe_ok = true AND probe_path = 'doublezero') * 100.0
					/ nullIf(countIf(probe_path = 'doublezero'), 0),
				countIf(probe_ok = true AND probe_path = 'public_internet') * 100.0
					/ nullIf(countIf(probe_path = 'public_internet'), 0)
			FROM %s.%s
			WHERE timestamp >= now() - INTERVAL %s AND validator_pubkey = ?
			GROUP BY ts
			ORDER BY ts
		`, bucket, db, table, interval)

		rows, err := a.envDB(gctx).Query(gctx, query, pubkey)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var tp GMTimePoint
			var ts time.Time
			var dzVal, piVal *float64
			if err := rows.Scan(&ts, &dzVal, &piVal); err != nil {
				return err
			}
			tp.Time = ts.UTC().Format(time.RFC3339)
			tp.DZValue = derefFloat(dzVal)
			tp.PIValue = derefFloat(piVal)
			detail.AvailabilityTS = append(detail.AvailabilityTS, tp)
		}
		return nil
	})

	// Per-source-metro breakdown
	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT
				source_metro_name,
				avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'doublezero') AS dz_rtt,
				avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'public_internet') AS pi_rtt,
				countIf(probe_ok = true AND probe_path = 'doublezero') * 100.0
					/ nullIf(countIf(probe_path = 'doublezero'), 0) AS dz_avail,
				countIf(probe_ok = true AND probe_path = 'public_internet') * 100.0
					/ nullIf(countIf(probe_path = 'public_internet'), 0) AS pi_avail,
				count() AS probe_count
			FROM %s.%s
			WHERE timestamp >= now() - INTERVAL %s AND validator_pubkey = ?
			GROUP BY source_metro_name
			ORDER BY source_metro_name
		`, db, table, interval)

		rows, err := a.envDB(gctx).Query(gctx, query, pubkey)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var m GMMetroBreakdown
			var dzRtt, piRtt, dzAvail, piAvail *float64
			if err := rows.Scan(&m.SourceMetro, &dzRtt, &piRtt, &dzAvail, &piAvail, &m.ProbeCount); err != nil {
				return err
			}
			m.DZRttMs = derefFloat(dzRtt)
			m.PIRttMs = derefFloat(piRtt)
			m.DZAvailPct = derefFloat(dzAvail)
			m.PIAvailPct = derefFloat(piAvail)
			detail.MetroBreakdown = append(detail.MetroBreakdown, m)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if detail.RttTS == nil {
		detail.RttTS = []GMTimePoint{}
	}
	if detail.AvailabilityTS == nil {
		detail.AvailabilityTS = []GMTimePoint{}
	}
	if detail.MetroBreakdown == nil {
		detail.MetroBreakdown = []GMMetroBreakdown{}
	}

	writeJSON(w, detail)
}

// --- User types ---

type GMUserItem struct {
	UserPubkey        string  `json:"user_pubkey"`
	TargetIP          string  `json:"target_ip"`
	Metro             string  `json:"metro"`
	Country           string  `json:"country"`
	City              string  `json:"city"`
	ASNOrg            string  `json:"asn_org"`
	DZDMetro          string  `json:"dzd_metro"`
	DZAvailabilityPct float64 `json:"dz_availability_pct"`
	PIAvailabilityPct float64 `json:"pi_availability_pct"`
	DZRttMs           float64 `json:"dz_rtt_ms"`
	PIRttMs           float64 `json:"pi_rtt_ms"`
	RttDeltaMs        float64 `json:"rtt_delta_ms"`
	PacketLossPct     float64 `json:"packet_loss_pct"`
}

type GMUsersResponse struct {
	Items []GMUserItem `json:"items"`
	Total int          `json:"total"`
}

type GMUserSummary struct {
	TotalUsers       uint64  `json:"total_users"`
	DZAvailablePct   float64 `json:"dz_available_pct"`
	DZBetterRttPct   float64 `json:"dz_better_rtt_pct"`
	MedianRttDeltaMs float64 `json:"median_rtt_delta_ms"`

	AvailabilityTS []GMTimePoint `json:"availability_ts"`
	RttTS          []GMTimePoint `json:"rtt_ts"`
}

type GMUserDetail struct {
	GMUserItem
	Region string `json:"region"`

	RttTS          []GMTimePoint    `json:"rtt_ts"`
	AvailabilityTS []GMTimePoint    `json:"availability_ts"`
	MetroBreakdown []GMMetroBreakdown `json:"metro_breakdown"`
}

// --- User handlers ---

const gmUserTable = "doublezero_user_icmp_probe"

func (a *API) GetGMUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	db := gmDB(a)
	interval := gmTimeRange(r)

	query := fmt.Sprintf(`
		SELECT
			user_pubkey,
			any(target_ip) AS target_ip,
			any(target_geoip_metro) AS metro,
			any(target_geoip_country) AS country,
			any(target_geoip_city) AS city,
			any(target_geoip_asn_org) AS asn_org,
			any(target_dzd_metro_name) AS dzd_metro,
			countIf(probe_ok = true AND probe_path = 'doublezero') * 100.0
				/ nullIf(countIf(probe_path = 'doublezero'), 0) AS dz_avail,
			countIf(probe_ok = true AND probe_path = 'public_internet') * 100.0
				/ nullIf(countIf(probe_path = 'public_internet'), 0) AS pi_avail,
			avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'doublezero') AS dz_rtt,
			avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'public_internet') AS pi_rtt,
			1.0 - (sum(probe_packets_recv) / nullIf(sum(probe_packets_sent), 0)) AS pkt_loss
		FROM %s.%s
		WHERE timestamp >= now() - INTERVAL %s
		GROUP BY user_pubkey
		ORDER BY dz_avail DESC
	`, db, gmUserTable, interval)

	rows, err := a.envDB(ctx).Query(ctx, query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var items []GMUserItem
	for rows.Next() {
		var u GMUserItem
		var dzAvail, piAvail, dzRtt, piRtt, pktLoss *float64
		if err := rows.Scan(
			&u.UserPubkey, &u.TargetIP, &u.Metro, &u.Country, &u.City, &u.ASNOrg, &u.DZDMetro,
			&dzAvail, &piAvail, &dzRtt, &piRtt, &pktLoss,
		); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		u.DZAvailabilityPct = derefFloat(dzAvail)
		u.PIAvailabilityPct = derefFloat(piAvail)
		u.DZRttMs = derefFloat(dzRtt)
		u.PIRttMs = derefFloat(piRtt)
		u.RttDeltaMs = u.PIRttMs - u.DZRttMs
		u.PacketLossPct = derefFloat(pktLoss) * 100
		items = append(items, u)
	}
	if items == nil {
		items = []GMUserItem{}
	}
	writeJSON(w, GMUsersResponse{Items: items, Total: len(items)})
}

func (a *API) GetGMUsersSummary(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	db := gmDB(a)
	interval := gmTimeRange(r)
	bucket := gmBucketInterval(r)

	g, gctx := errgroup.WithContext(ctx)

	var summary GMUserSummary

	// Aggregate stats
	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT
				count() AS total,
				avg(dz_avail) AS avg_dz_avail,
				sumIf(1, dz_rtt < pi_rtt) * 100.0 / nullIf(count(), 0) AS dz_better_pct,
				medianIf(pi_rtt - dz_rtt, isFinite(pi_rtt - dz_rtt)) AS median_delta
			FROM (
				SELECT
					user_pubkey,
					countIf(probe_ok = true AND probe_path = 'doublezero') * 100.0
						/ nullIf(countIf(probe_path = 'doublezero'), 0) AS dz_avail,
					avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'doublezero') AS dz_rtt,
					avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'public_internet') AS pi_rtt
				FROM %s.%s
				WHERE timestamp >= now() - INTERVAL %s
				GROUP BY user_pubkey
			)
		`, db, gmUserTable, interval)

		rows, err := a.envDB(gctx).Query(gctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()

		if rows.Next() {
			var dzAvail, dzBetter, medianDelta *float64
			if err := rows.Scan(&summary.TotalUsers, &dzAvail, &dzBetter, &medianDelta); err != nil {
				return err
			}
			summary.DZAvailablePct = derefFloat(dzAvail)
			summary.DZBetterRttPct = derefFloat(dzBetter)
			summary.MedianRttDeltaMs = derefFloat(medianDelta)
		}
		return nil
	})

	// Time-bucketed availability
	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT
				toStartOfInterval(timestamp, INTERVAL %s) AS ts,
				countIf(probe_ok = true AND probe_path = 'doublezero') * 100.0
					/ nullIf(countIf(probe_path = 'doublezero'), 0) AS dz_avail,
				countIf(probe_ok = true AND probe_path = 'public_internet') * 100.0
					/ nullIf(countIf(probe_path = 'public_internet'), 0) AS pi_avail
			FROM %s.%s
			WHERE timestamp >= now() - INTERVAL %s
			GROUP BY ts
			ORDER BY ts
		`, bucket, db, gmUserTable, interval)

		rows, err := a.envDB(gctx).Query(gctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var tp GMTimePoint
			var ts time.Time
			var dzVal, piVal *float64
			if err := rows.Scan(&ts, &dzVal, &piVal); err != nil {
				return err
			}
			tp.Time = ts.UTC().Format(time.RFC3339)
			tp.DZValue = derefFloat(dzVal)
			tp.PIValue = derefFloat(piVal)
			summary.AvailabilityTS = append(summary.AvailabilityTS, tp)
		}
		return nil
	})

	// Time-bucketed RTT
	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT
				toStartOfInterval(timestamp, INTERVAL %s) AS ts,
				avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'doublezero') AS dz_rtt,
				avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'public_internet') AS pi_rtt
			FROM %s.%s
			WHERE timestamp >= now() - INTERVAL %s
			GROUP BY ts
			ORDER BY ts
		`, bucket, db, gmUserTable, interval)

		rows, err := a.envDB(gctx).Query(gctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var tp GMTimePoint
			var ts time.Time
			var dzVal, piVal *float64
			if err := rows.Scan(&ts, &dzVal, &piVal); err != nil {
				return err
			}
			tp.Time = ts.UTC().Format(time.RFC3339)
			tp.DZValue = derefFloat(dzVal)
			tp.PIValue = derefFloat(piVal)
			summary.RttTS = append(summary.RttTS, tp)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if summary.AvailabilityTS == nil {
		summary.AvailabilityTS = []GMTimePoint{}
	}
	if summary.RttTS == nil {
		summary.RttTS = []GMTimePoint{}
	}

	writeJSON(w, summary)
}

func (a *API) GetGMUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID := chi.URLParam(r, "id")
	db := gmDB(a)
	interval := gmTimeRange(r)
	bucket := gmBucketInterval(r)

	g, gctx := errgroup.WithContext(ctx)

	var detail GMUserDetail

	// User info
	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT
				any(target_ip),
				any(target_geoip_metro),
				any(target_geoip_country),
				any(target_geoip_city),
				any(target_geoip_region),
				any(target_geoip_asn_org),
				any(target_dzd_metro_name),
				countIf(probe_ok = true AND probe_path = 'doublezero') * 100.0
					/ nullIf(countIf(probe_path = 'doublezero'), 0),
				countIf(probe_ok = true AND probe_path = 'public_internet') * 100.0
					/ nullIf(countIf(probe_path = 'public_internet'), 0),
				avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'doublezero'),
				avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'public_internet'),
				1.0 - (sum(probe_packets_recv) / nullIf(sum(probe_packets_sent), 0))
			FROM %s.%s
			WHERE timestamp >= now() - INTERVAL %s AND user_pubkey = ?
		`, db, gmUserTable, interval)

		rows, err := a.envDB(gctx).Query(gctx, query, userID)
		if err != nil {
			return err
		}
		defer rows.Close()

		if rows.Next() {
			var dzAvail, piAvail, dzRtt, piRtt, pktLoss *float64
			if err := rows.Scan(
				&detail.TargetIP, &detail.Metro, &detail.Country,
				&detail.City, &detail.Region, &detail.ASNOrg, &detail.DZDMetro,
				&dzAvail, &piAvail, &dzRtt, &piRtt, &pktLoss,
			); err != nil {
				return err
			}
			detail.UserPubkey = userID
			detail.DZAvailabilityPct = derefFloat(dzAvail)
			detail.PIAvailabilityPct = derefFloat(piAvail)
			detail.DZRttMs = derefFloat(dzRtt)
			detail.PIRttMs = derefFloat(piRtt)
			detail.RttDeltaMs = detail.PIRttMs - detail.DZRttMs
			detail.PacketLossPct = derefFloat(pktLoss) * 100
		}
		return nil
	})

	// Time-bucketed RTT
	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT
				toStartOfInterval(timestamp, INTERVAL %s) AS ts,
				avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'doublezero') AS dz_rtt,
				avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'public_internet') AS pi_rtt
			FROM %s.%s
			WHERE timestamp >= now() - INTERVAL %s AND user_pubkey = ?
			GROUP BY ts
			ORDER BY ts
		`, bucket, db, gmUserTable, interval)

		rows, err := a.envDB(gctx).Query(gctx, query, userID)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var tp GMTimePoint
			var ts time.Time
			var dzVal, piVal *float64
			if err := rows.Scan(&ts, &dzVal, &piVal); err != nil {
				return err
			}
			tp.Time = ts.UTC().Format(time.RFC3339)
			tp.DZValue = derefFloat(dzVal)
			tp.PIValue = derefFloat(piVal)
			detail.RttTS = append(detail.RttTS, tp)
		}
		return nil
	})

	// Time-bucketed availability
	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT
				toStartOfInterval(timestamp, INTERVAL %s) AS ts,
				countIf(probe_ok = true AND probe_path = 'doublezero') * 100.0
					/ nullIf(countIf(probe_path = 'doublezero'), 0),
				countIf(probe_ok = true AND probe_path = 'public_internet') * 100.0
					/ nullIf(countIf(probe_path = 'public_internet'), 0)
			FROM %s.%s
			WHERE timestamp >= now() - INTERVAL %s AND user_pubkey = ?
			GROUP BY ts
			ORDER BY ts
		`, bucket, db, gmUserTable, interval)

		rows, err := a.envDB(gctx).Query(gctx, query, userID)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var tp GMTimePoint
			var ts time.Time
			var dzVal, piVal *float64
			if err := rows.Scan(&ts, &dzVal, &piVal); err != nil {
				return err
			}
			tp.Time = ts.UTC().Format(time.RFC3339)
			tp.DZValue = derefFloat(dzVal)
			tp.PIValue = derefFloat(piVal)
			detail.AvailabilityTS = append(detail.AvailabilityTS, tp)
		}
		return nil
	})

	// Per-source-metro breakdown
	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT
				source_metro_name,
				avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'doublezero') AS dz_rtt,
				avgIf(probe_rtt_avg_ms, probe_ok = true AND probe_path = 'public_internet') AS pi_rtt,
				countIf(probe_ok = true AND probe_path = 'doublezero') * 100.0
					/ nullIf(countIf(probe_path = 'doublezero'), 0) AS dz_avail,
				countIf(probe_ok = true AND probe_path = 'public_internet') * 100.0
					/ nullIf(countIf(probe_path = 'public_internet'), 0) AS pi_avail,
				count() AS probe_count
			FROM %s.%s
			WHERE timestamp >= now() - INTERVAL %s AND user_pubkey = ?
			GROUP BY source_metro_name
			ORDER BY source_metro_name
		`, db, gmUserTable, interval)

		rows, err := a.envDB(gctx).Query(gctx, query, userID)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var m GMMetroBreakdown
			var dzRtt, piRtt, dzAvail, piAvail *float64
			if err := rows.Scan(&m.SourceMetro, &dzRtt, &piRtt, &dzAvail, &piAvail, &m.ProbeCount); err != nil {
				return err
			}
			m.DZRttMs = derefFloat(dzRtt)
			m.PIRttMs = derefFloat(piRtt)
			m.DZAvailPct = derefFloat(dzAvail)
			m.PIAvailPct = derefFloat(piAvail)
			detail.MetroBreakdown = append(detail.MetroBreakdown, m)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if detail.RttTS == nil {
		detail.RttTS = []GMTimePoint{}
	}
	if detail.AvailabilityTS == nil {
		detail.AvailabilityTS = []GMTimePoint{}
	}
	if detail.MetroBreakdown == nil {
		detail.MetroBreakdown = []GMMetroBreakdown{}
	}

	writeJSON(w, detail)
}

// derefFloat safely dereferences a *float64, returning 0 for nil and NaN.
func derefFloat(f *float64) float64 {
	if f == nil || math.IsNaN(*f) || math.IsInf(*f, 0) {
		return 0
	}
	return *f
}
