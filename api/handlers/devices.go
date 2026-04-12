package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/metrics"
)

type DeviceListItem struct {
	PK              string  `json:"pk"`
	Code            string  `json:"code"`
	Status          string  `json:"status"`
	DeviceType      string  `json:"device_type"`
	ContributorPK   string  `json:"contributor_pk"`
	ContributorCode string  `json:"contributor_code"`
	MetroPK         string  `json:"metro_pk"`
	MetroCode       string  `json:"metro_code"`
	PublicIP        string  `json:"public_ip"`
	MaxUsers        int32   `json:"max_users"`
	CurrentUsers    uint64  `json:"current_users"`
	InBps           float64 `json:"in_bps"`
	OutBps          float64 `json:"out_bps"`
	PeakInBps       float64 `json:"peak_in_bps"`
	PeakOutBps      float64 `json:"peak_out_bps"`
}

var deviceListSortFields = map[string]string{
	"code":        "code",
	"type":        "device_type",
	"contributor": "contributor_code",
	"metro":       "metro_code",
	"status":      "status",
	"users":       "current_users",
	"in":          "in_bps",
	"out":         "out_bps",
	"peakin":      "peak_in_bps",
	"peakout":     "peak_out_bps",
}

var deviceListFilterFields = map[string]FilterFieldConfig{
	"code":        {Column: "code", Type: FieldTypeText},
	"type":        {Column: "device_type", Type: FieldTypeText},
	"contributor": {Column: "contributor_code", Type: FieldTypeText},
	"metro":       {Column: "metro_code", Type: FieldTypeText},
	"status":      {Column: "status", Type: FieldTypeText},
	"users":       {Column: "current_users", Type: FieldTypeNumeric},
	"in":          {Column: "in_bps", Type: FieldTypeBandwidth},
	"out":         {Column: "out_bps", Type: FieldTypeBandwidth},
	"peakin":      {Column: "peak_in_bps", Type: FieldTypeBandwidth},
	"peakout":     {Column: "peak_out_bps", Type: FieldTypeBandwidth},
}

func (a *API) GetDevices(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "code", deviceListSortFields)
	filters := ParseFilters(r)
	start := time.Now()

	filterClause, filterArgs := filters.BuildFilterClause(deviceListFilterFields)
	whereFilter := ""
	if filterClause != "" {
		whereFilter = " AND " + filterClause
	}
	orderBy := sort.OrderByClause(deviceListSortFields)

	query := `
		WITH user_counts AS (
			SELECT device_pk, count(*) as user_count
			FROM dz_users_current
			WHERE status = 'activated'
			GROUP BY device_pk
		),
		traffic_rates AS (
			SELECT
				device_pk,
				avg(avg_in_bps) as in_bps,
				avg(avg_out_bps) as out_bps
			FROM device_interface_rollup_5m
			WHERE bucket_ts >= now() - INTERVAL 15 MINUTE
				AND user_tunnel_id IS NULL
				AND link_pk = ''
			GROUP BY device_pk
		),
		peak_rates AS (
			SELECT
				device_pk,
				max(max_in_bps) as peak_in_bps,
				max(max_out_bps) as peak_out_bps
			FROM device_interface_rollup_5m
			WHERE bucket_ts >= now() - INTERVAL 1 HOUR
				AND user_tunnel_id IS NULL
				AND link_pk = ''
			GROUP BY device_pk
		),
		devices_data AS (
			SELECT
				d.pk as pk,
				d.code as code,
				d.status as status,
				d.device_type as device_type,
				COALESCE(d.contributor_pk, '') as contributor_pk,
				COALESCE(c.code, '') as contributor_code,
				COALESCE(d.metro_pk, '') as metro_pk,
				COALESCE(m.code, '') as metro_code,
				COALESCE(d.public_ip, '') as public_ip,
				COALESCE(d.max_users, 0) as max_users,
				COALESCE(uc.user_count, 0) as current_users,
				COALESCE(tr.in_bps, 0) as in_bps,
				COALESCE(tr.out_bps, 0) as out_bps,
				COALESCE(pr.peak_in_bps, 0) as peak_in_bps,
				COALESCE(pr.peak_out_bps, 0) as peak_out_bps
			FROM dz_devices_current d
			LEFT JOIN dz_contributors_current c ON d.contributor_pk = c.pk
			LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
			LEFT JOIN user_counts uc ON d.pk = uc.device_pk
			LEFT JOIN traffic_rates tr ON d.pk = tr.device_pk
			LEFT JOIN peak_rates pr ON d.pk = pr.device_pk
		)
		SELECT
			pk, code, status, device_type, contributor_pk, contributor_code, metro_pk, metro_code, public_ip, max_users, current_users, in_bps, out_bps, peak_in_bps, peak_out_bps,
			count() OVER () as _total
		FROM devices_data
		WHERE 1=1` + whereFilter + " " + orderBy + `
		LIMIT ? OFFSET ?
	`

	var args []any
	args = append(args, filterArgs...)
	args = append(args, pagination.Limit, pagination.Offset)

	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("devices query failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var devices []DeviceListItem
	var total uint64
	for rows.Next() {
		var d DeviceListItem
		if err := rows.Scan(
			&d.PK,
			&d.Code,
			&d.Status,
			&d.DeviceType,
			&d.ContributorPK,
			&d.ContributorCode,
			&d.MetroPK,
			&d.MetroCode,
			&d.PublicIP,
			&d.MaxUsers,
			&d.CurrentUsers,
			&d.InBps,
			&d.OutBps,
			&d.PeakInBps,
			&d.PeakOutBps,
			&total,
		); err != nil {
			logError("devices row scan failed", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		devices = append(devices, d)
	}

	if err := rows.Err(); err != nil {
		logError("devices rows iteration failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return empty array instead of null
	if devices == nil {
		devices = []DeviceListItem{}
	}

	response := PaginatedResponse[DeviceListItem]{
		Items:  devices,
		Total:  int(total),
		Limit:  pagination.Limit,
		Offset: pagination.Offset,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logError("failed to encode response", "error", err)
	}
}

type DeviceDetailInterface struct {
	Name               string `json:"name"`
	IP                 string `json:"ip"`
	Status             string `json:"status"`
	InterfaceType      string `json:"interface_type"`
	CYOAType           string `json:"cyoa_type"`
	DIAType            string `json:"dia_type"`
	LoopbackType       string `json:"loopback_type"`
	RoutingMode        string `json:"routing_mode"`
	Bandwidth          uint64 `json:"bandwidth"`
	Cir                uint64 `json:"cir"`
	Mtu                uint16 `json:"mtu"`
	VlanID             uint16 `json:"vlan_id"`
	NodeSegmentIdx     uint16 `json:"node_segment_idx"`
	UserTunnelEndpoint bool   `json:"user_tunnel_endpoint"`
}

type DeviceDetail struct {
	PK              string                  `json:"pk"`
	Code            string                  `json:"code"`
	Status          string                  `json:"status"`
	DeviceType      string                  `json:"device_type"`
	ContributorPK   string                  `json:"contributor_pk"`
	ContributorCode string                  `json:"contributor_code"`
	MetroPK         string                  `json:"metro_pk"`
	MetroCode       string                  `json:"metro_code"`
	MetroName       string                  `json:"metro_name"`
	PublicIP        string                  `json:"public_ip"`
	MaxUsers        int32                   `json:"max_users"`
	CurrentUsers    uint64                  `json:"current_users"`
	InBps           float64                 `json:"in_bps"`
	OutBps          float64                 `json:"out_bps"`
	PeakInBps       float64                 `json:"peak_in_bps"`
	PeakOutBps      float64                 `json:"peak_out_bps"`
	ValidatorCount  uint64                  `json:"validator_count"`
	StakeSol        float64                 `json:"stake_sol"`
	StakeShare      float64                 `json:"stake_share"`
	Interfaces      []DeviceDetailInterface `json:"interfaces"`
}

func (a *API) GetDevice(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pk := chi.URLParam(r, "pk")
	if pk == "" {
		http.Error(w, "missing device pk", http.StatusBadRequest)
		return
	}

	start := time.Now()
	query := `
		WITH user_counts AS (
			SELECT device_pk, count(*) as user_count
			FROM dz_users_current
			WHERE status = 'activated'
			GROUP BY device_pk
		),
		traffic_rates AS (
			SELECT
				device_pk,
				avg(avg_in_bps) as in_bps,
				avg(avg_out_bps) as out_bps
			FROM device_interface_rollup_5m
			WHERE bucket_ts >= now() - INTERVAL 15 MINUTE
				AND user_tunnel_id IS NULL
				AND link_pk = ''
			GROUP BY device_pk
		),
		peak_rates AS (
			SELECT
				device_pk,
				max(max_in_bps) as peak_in_bps,
				max(max_out_bps) as peak_out_bps
			FROM device_interface_rollup_5m
			WHERE bucket_ts >= now() - INTERVAL 1 HOUR
				AND user_tunnel_id IS NULL
				AND link_pk = ''
			GROUP BY device_pk
		),
		validator_stats AS (
			SELECT
				u.device_pk,
				count(DISTINCT v.vote_pubkey) as validator_count,
				sum(v.activated_stake_lamports) / 1e9 as stake_sol
			FROM dz_users_current u
			JOIN solana_gossip_nodes_current g ON u.client_ip = g.gossip_ip
			JOIN solana_vote_accounts_current v ON g.pubkey = v.node_pubkey
			WHERE u.status = 'activated' AND u.client_ip != '' AND v.epoch_vote_account = 'true'
			GROUP BY u.device_pk
		),
		total_stake AS (
			SELECT COALESCE(SUM(activated_stake_lamports), 0) as total_lamports
			FROM solana_vote_accounts_current
			WHERE epoch_vote_account = 'true' AND activated_stake_lamports > 0
		)
		SELECT
			d.pk,
			d.code,
			d.status,
			d.device_type,
			COALESCE(d.contributor_pk, '') as contributor_pk,
			COALESCE(c.code, '') as contributor_code,
			COALESCE(d.metro_pk, '') as metro_pk,
			COALESCE(m.code, '') as metro_code,
			COALESCE(m.name, '') as metro_name,
			COALESCE(d.public_ip, '') as public_ip,
			COALESCE(d.max_users, 0) as max_users,
			COALESCE(uc.user_count, 0) as current_users,
			COALESCE(tr.in_bps, 0) as in_bps,
			COALESCE(tr.out_bps, 0) as out_bps,
			COALESCE(pr.peak_in_bps, 0) as peak_in_bps,
			COALESCE(pr.peak_out_bps, 0) as peak_out_bps,
			COALESCE(vs.validator_count, 0) as validator_count,
			COALESCE(vs.stake_sol, 0) as stake_sol,
			CASE
				WHEN ts.total_lamports > 0 THEN COALESCE(vs.stake_sol, 0) * 1e9 / ts.total_lamports * 100
				ELSE 0
			END as stake_share,
			COALESCE(d.interfaces, '[]') as interfaces
		FROM dz_devices_current d
		CROSS JOIN total_stake ts
		LEFT JOIN dz_contributors_current c ON d.contributor_pk = c.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN user_counts uc ON d.pk = uc.device_pk
		LEFT JOIN traffic_rates tr ON d.pk = tr.device_pk
		LEFT JOIN peak_rates pr ON d.pk = pr.device_pk
		LEFT JOIN validator_stats vs ON d.pk = vs.device_pk
		WHERE d.pk = ?
	`

	var device DeviceDetail
	var interfacesJSON string
	err := a.envDB(ctx).QueryRow(ctx, query, pk).Scan(
		&device.PK,
		&device.Code,
		&device.Status,
		&device.DeviceType,
		&device.ContributorPK,
		&device.ContributorCode,
		&device.MetroPK,
		&device.MetroCode,
		&device.MetroName,
		&device.PublicIP,
		&device.MaxUsers,
		&device.CurrentUsers,
		&device.InBps,
		&device.OutBps,
		&device.PeakInBps,
		&device.PeakOutBps,
		&device.ValidatorCount,
		&device.StakeSol,
		&device.StakeShare,
		&interfacesJSON,
	)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "device not found", http.StatusNotFound)
			return
		}
		logError("device query failed", "error", err, "pk", pk)
		http.Error(w, "device not found", http.StatusNotFound)
		return
	}

	// Parse base interfaces JSON for IP lookup and fallback
	var baseInterfaces []DeviceInterface
	if err := json.Unmarshal([]byte(interfacesJSON), &baseInterfaces); err != nil {
		baseInterfaces = nil
	}

	// Fetch enriched interfaces from the device interfaces dimension table.
	// Fall back to the basic JSON interfaces if the dimension table has no data.
	ifaceQuery := `
		SELECT
			di.intf, COALESCE(di.status, ''), COALESCE(di.interface_type, ''),
			COALESCE(di.cyoa_type, ''), COALESCE(di.dia_type, ''),
			COALESCE(di.loopback_type, ''), COALESCE(di.routing_mode, ''),
			di.bandwidth, di.cir, di.mtu, di.vlan_id, di.node_segment_idx, di.user_tunnel_endpoint
		FROM dz_device_interfaces_current di
		WHERE di.device_pk = ?
		ORDER BY di.status DESC, di.intf
	`
	ifaceRows, ifaceErr := a.envDB(ctx).Query(ctx, ifaceQuery, pk)
	if ifaceErr == nil {
		defer ifaceRows.Close()
		for ifaceRows.Next() {
			var di DeviceDetailInterface
			var ute uint8
			if err := ifaceRows.Scan(
				&di.Name, &di.Status, &di.InterfaceType,
				&di.CYOAType, &di.DIAType,
				&di.LoopbackType, &di.RoutingMode,
				&di.Bandwidth, &di.Cir, &di.Mtu, &di.VlanID, &di.NodeSegmentIdx, &ute,
			); err != nil {
				continue
			}
			di.UserTunnelEndpoint = ute == 1
			// Populate IP from the base interfaces JSON
			for _, base := range baseInterfaces {
				if base.Name == di.Name {
					di.IP = base.IP
					break
				}
			}
			device.Interfaces = append(device.Interfaces, di)
		}
	}
	// Fall back to basic interfaces if dimension table had no data
	if len(device.Interfaces) == 0 {
		for _, base := range baseInterfaces {
			device.Interfaces = append(device.Interfaces, DeviceDetailInterface{
				Name:   base.Name,
				IP:     base.IP,
				Status: base.Status,
			})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(device); err != nil {
		logError("failed to encode response", "error", err)
	}
}
