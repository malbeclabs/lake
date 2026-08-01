package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var opsMgmtBaseURL = "https://doublezero.xyz/api/network_incidents/v1"

// activeStatuses are the ticket statuses that the upstream API considers "active".
var activeStatuses = []string{
	"open", "acknowledged", "investigating", "mitigating",
	"monitoring", "planned", "in-progress",
}

// OpsTicketType is either "incident" or "maintenance".
type OpsTicketType string

const (
	OpsTicketTypeIncident    OpsTicketType = "incident"
	OpsTicketTypeMaintenance OpsTicketType = "maintenance"
)

// OpsTicketEntity is an enriched link or device reference returned by the Ops API.
type OpsTicketEntity struct {
	Code   string `json:"code"`
	Pubkey string `json:"pubkey"`
}

// OpsTicket is a ticket from the Ops Management API.
type OpsTicket struct {
	ID                  string            `json:"id"`                // UUID
	HumanReadableID     string            `json:"human_readable_id"` // e.g. "I20250413-a7b2"
	Type                OpsTicketType     `json:"type"`
	Title               string            `json:"title"`
	Description         string            `json:"description"`
	Severity            *string           `json:"severity,omitempty"` // "sev1"|"sev2"|"sev3"|nil
	Status              string            `json:"status"`             // "open"|"acknowledged"|"investigating"|...
	AffectedLinkPubkeys []string          `json:"affected_link_pubkey"`
	DevicePubkeys       []string          `json:"device_pubkey"`
	AffectedLinks       []OpsTicketEntity `json:"affected_links,omitempty"`
	AffectedDevices     []OpsTicketEntity `json:"affected_devices,omitempty"`
	ReporterName        string            `json:"reporter_name"`
	ReporterEmail       string            `json:"reporter_email,omitempty"`
	ContributorName     *string           `json:"contributor_name,omitempty"`
	StartAt             *string           `json:"start_at,omitempty"`
	EndAt               *string           `json:"end_at,omitempty"`
	SlackMessageURL     *string           `json:"slack_message_url,omitempty"`
	CreatedAt           string            `json:"created_at"`
	UpdatedAt           string            `json:"updated_at"`
	MetroCodes          []string          `json:"metro_codes,omitempty"`
	EnrichedDeviceCodes []string          `json:"enriched_device_codes,omitempty"`
}

// OpsTicketsListResponse wraps a paginated list of tickets.
type OpsTicketsListResponse struct {
	Tickets []OpsTicket `json:"tickets"`
	Total   int         `json:"total"`
}

// CreateOpsTicketRequest is the body for POST /api/ops-tickets.
type CreateOpsTicketRequest struct {
	Type                OpsTicketType `json:"type"`
	Title               string        `json:"title"`
	Description         string        `json:"description"`
	Severity            *string       `json:"severity,omitempty"`
	Status              string        `json:"status"`
	AffectedLinkPubkeys []string      `json:"affected_link_pubkey,omitempty"`
	DevicePubkeys       []string      `json:"device_pubkey,omitempty"`
	StartAt             *string       `json:"start_at,omitempty"`
	EndAt               *string       `json:"end_at,omitempty"`
	ContributorPubkey   *string       `json:"contributor_pubkey"`
	ReporterName        string        `json:"reporter_name"`
	ReporterEmail       string        `json:"reporter_email"`
}

// opsClient performs authenticated requests to the Ops Management API.
type opsClient struct {
	httpClient *http.Client
	apiKey     string
	baseURL    string
}

func newOpsClient() *opsClient {
	return &opsClient{
		httpClient: &http.Client{Timeout: 10 * time.Second},
		apiKey:     os.Getenv("OPS_MANAGEMENT_API_KEY"),
		baseURL:    opsMgmtBaseURL,
	}
}

func (c *opsClient) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ops management request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ops management returned %d: %s", resp.StatusCode, body)
	}
	return body, nil
}

func (c *opsClient) post(ctx context.Context, path string, payload any) ([]byte, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ops management request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("ops management returned %d: %s", resp.StatusCode, body)
	}
	return body, nil
}

// GetOpsTickets proxies GET /api/ops-tickets
// Query params: status (default "active"), type (optional), limit (optional).
// The upstream API understands "active" and "not_active" as status presets.
// No auth required.
func (a *API) GetOpsTickets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	client := newOpsClient()

	q := r.URL.Query()
	status := q.Get("status")
	if status == "" {
		status = "active"
	}

	upstream := url.Values{}
	upstream.Set("status", status)
	if t := q.Get("type"); t != "" {
		upstream.Set("type", t)
	}
	if lim := q.Get("limit"); lim != "" {
		upstream.Set("limit", lim)
	}

	body, err := client.get(ctx, "/tickets?"+upstream.Encode())
	if err != nil {
		http.Error(w, "failed to fetch ops tickets", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// GetMaintenanceCalendar serves GET /api/ops-tickets/maintenance.
// Public endpoint — returns only maintenance tickets with reporter_email stripped
// and metro_codes enriched from ClickHouse.
func (a *API) GetMaintenanceCalendar(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	client := newOpsClient()

	status := r.URL.Query().Get("status")
	if status == "" {
		status = "active"
	}

	tickets, err := fetchAllMaintenanceTickets(ctx, client, status)
	if err != nil {
		http.Error(w, "failed to fetch maintenance calendar", http.StatusBadGateway)
		return
	}

	if a.DB != nil {
		tickets = enrichTicketsWithMetros(ctx, a.DB, tickets)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"data": OpsTicketsListResponse{Tickets: tickets, Total: len(tickets)},
	})
}

// fetchAllMaintenanceTickets fetches all maintenance tickets for the given status,
// paginating through the upstream API using offset until all pages are collected.
func fetchAllMaintenanceTickets(ctx context.Context, client *opsClient, status string) ([]OpsTicket, error) {
	const (
		maxPages = 50
		pageSize = 50
	)
	seen := make(map[string]struct{})
	var all []OpsTicket

	for page := 0; page < maxPages; page++ {
		offset := page * pageSize
		// limit= is sent explicitly so the offset step always matches the server page size.
		path := fmt.Sprintf("/tickets?status=%s&type=maintenance&limit=%d&offset=%d",
			url.QueryEscape(status), pageSize, offset)
		body, err := client.get(ctx, path)
		if err != nil {
			return nil, err
		}

		tickets, gotSize, upstreamTotal, err := parseMaintenanceTickets(body)
		if err != nil {
			return nil, err
		}

		for _, t := range tickets {
			// Skip tickets with empty IDs (upstream data quality issue).
			if t.ID == "" {
				continue
			}
			if _, dup := seen[t.ID]; !dup {
				seen[t.ID] = struct{}{}
				all = append(all, t)
			}
		}
		log.Printf("maintenance calendar: status=%s offset=%d upstream_total=%d page_size=%d collected=%d",
			status, offset, upstreamTotal, gotSize, len(all))

		// Stop when the page was empty or we've received all upstream items.
		// upstreamTotal=0 with gotSize=0 also exits via the first guard.
		if gotSize == 0 || offset+gotSize >= upstreamTotal {
			break
		}
	}

	return all, nil
}

// parseMaintenanceTickets extracts maintenance tickets from the upstream response
// envelope { "data": { "tickets": [...], "total": N } }, stripping reporter_email.
// Returns the filtered tickets, count of all upstream tickets in the page, and the upstream total.
func parseMaintenanceTickets(body []byte) (tickets []OpsTicket, pageSize int, upstreamTotal int, err error) {
	var envelope struct {
		Data struct {
			Tickets []OpsTicket `json:"tickets"`
			Total   int         `json:"total"`
		} `json:"data"`
	}
	if err = json.Unmarshal(body, &envelope); err != nil {
		return
	}
	pageSize = len(envelope.Data.Tickets)
	upstreamTotal = envelope.Data.Total
	tickets = make([]OpsTicket, 0)
	for _, t := range envelope.Data.Tickets {
		if t.Type != OpsTicketTypeMaintenance {
			continue
		}
		t.ReporterEmail = ""
		tickets = append(tickets, t)
	}
	return
}

// enrichTicketsWithMetros queries ClickHouse to resolve actual metro codes for the
// link and device pubkeys referenced by each ticket, populating MetroCodes.
func enrichTicketsWithMetros(ctx context.Context, db driver.Conn, tickets []OpsTicket) []OpsTicket {
	linkPKSet := make(map[string]struct{})
	devicePKSet := make(map[string]struct{})
	for _, t := range tickets {
		for _, pk := range t.AffectedLinkPubkeys {
			if pk != "" {
				linkPKSet[pk] = struct{}{}
			}
		}
		for _, pk := range t.DevicePubkeys {
			if pk != "" {
				devicePKSet[pk] = struct{}{}
			}
		}
	}

	linkMetros := make(map[string][]string)
	linkDevices := make(map[string][]string) // link pk → side device codes
	deviceMetros := make(map[string][]string)

	if len(devicePKSet) > 0 {
		pks := make([]string, 0, len(devicePKSet))
		for pk := range devicePKSet {
			pks = append(pks, pk)
		}
		devRows, err := db.Query(ctx, `
			SELECT d.pk, m.code
			FROM dz_devices_current d
			LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
			WHERE d.pk IN ($1) AND m.code != ''`, pks)
		if err == nil {
			for devRows.Next() {
				var pk, code string
				if devRows.Scan(&pk, &code) == nil {
					deviceMetros[pk] = append(deviceMetros[pk], code)
				}
			}
			if err := devRows.Err(); err != nil {
				log.Printf("enrichTicketsWithMetros: device rows error: %v", err)
			}
			devRows.Close()
		}
	}

	if len(linkPKSet) > 0 {
		pks := make([]string, 0, len(linkPKSet))
		for pk := range linkPKSet {
			pks = append(pks, pk)
		}
		linkRows, err := db.Query(ctx, `
			SELECT l.pk, COALESCE(ma.code, ''), COALESCE(mz.code, ''), COALESCE(da.code, ''), COALESCE(dz.code, '')
			FROM dz_links_current l
			LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
			LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
			LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
			LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
			WHERE l.pk IN ($1)`, pks)
		if err == nil {
			for linkRows.Next() {
				var pk, aMetro, zMetro, aDevice, zDevice string
				if linkRows.Scan(&pk, &aMetro, &zMetro, &aDevice, &zDevice) == nil {
					if aMetro != "" {
						linkMetros[pk] = append(linkMetros[pk], aMetro)
					}
					if zMetro != "" {
						linkMetros[pk] = append(linkMetros[pk], zMetro)
					}
					if aDevice != "" {
						linkDevices[pk] = append(linkDevices[pk], aDevice)
					}
					if zDevice != "" {
						linkDevices[pk] = append(linkDevices[pk], zDevice)
					}
				}
			}
			if err := linkRows.Err(); err != nil {
				log.Printf("enrichTicketsWithMetros: link rows error: %v", err)
			}
			linkRows.Close()
		}
	}

	for i := range tickets {
		metroSeen := make(map[string]bool)
		var metros []string
		devSeen := make(map[string]bool)
		var enrichedDevices []string

		// Seed devSeen with explicitly listed device codes so we don't duplicate them.
		for _, d := range tickets[i].AffectedDevices {
			devSeen[d.Code] = true
		}

		for _, pk := range tickets[i].AffectedLinkPubkeys {
			for _, code := range linkMetros[pk] {
				if !metroSeen[code] {
					metroSeen[code] = true
					metros = append(metros, code)
				}
			}
			for _, code := range linkDevices[pk] {
				if !devSeen[code] {
					devSeen[code] = true
					enrichedDevices = append(enrichedDevices, code)
				}
			}
		}
		for _, pk := range tickets[i].DevicePubkeys {
			for _, code := range deviceMetros[pk] {
				if !metroSeen[code] {
					metroSeen[code] = true
					metros = append(metros, code)
				}
			}
		}
		sort.Strings(metros)
		sort.Strings(enrichedDevices)
		tickets[i].MetroCodes = metros
		tickets[i].EnrichedDeviceCodes = enrichedDevices
	}

	return tickets
}

// CreateOpsTicket proxies POST /api/ops-tickets.
// Requires an internal-domain authenticated user.
// Sets reporter_name and reporter_email from the authenticated session.
func (a *API) CreateOpsTicket(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil || !account.IsInternalUser {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	var req CreateOpsTicketRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Title == "" {
		http.Error(w, "title is required", http.StatusBadRequest)
		return
	}
	if req.Type == "" {
		req.Type = OpsTicketTypeIncident
	}
	if req.Status == "" {
		req.Status = "open"
	}

	// Override reporter fields from authenticated session — never trust client input.
	if account.DisplayName != nil {
		req.ReporterName = firstName(*account.DisplayName)
	}
	if account.Email != nil {
		req.ReporterEmail = *account.Email
	}

	client := newOpsClient()
	body, err := client.post(r.Context(), "/tickets", req)
	if err != nil {
		log.Printf("CreateOpsTicket: upstream error: %v", err)
		http.Error(w, "failed to create ops ticket", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(body)
}

// firstName returns the first word of a display name, e.g. "Thijs van Dijk" → "Thijs".
func firstName(displayName string) string {
	for i, c := range displayName {
		if c == ' ' {
			return displayName[:i]
		}
	}
	return displayName
}

// buildHistoryQuery constructs the query string for the history endpoint.
// entityType should be "link" or "device" to select the correct API filter param.
// Sending the same PK for both params would cause the Ops API to AND the conditions,
// matching nothing. We send only the relevant param based on entity type.
func buildHistoryQuery(entityPK, limit, ticketType, entityType string) string {
	var pkParam string
	switch entityType {
	case "device":
		pkParam = fmt.Sprintf("device_pubkey=%s", url.QueryEscape(entityPK))
	default: // "link" and unknown — default to link param
		pkParam = fmt.Sprintf("affected_link_pubkey=%s", url.QueryEscape(entityPK))
	}
	query := fmt.Sprintf("?%s&limit=%s&status=not_active", pkParam, url.QueryEscape(limit))
	if ticketType != "" {
		query += "&type=" + url.QueryEscape(ticketType)
	}
	return query
}

// GetOpsTicketHistory proxies GET /api/ops-tickets/history
// Required query param: entity_pk (link or device pubkey).
// Optional: limit (default 5), type (incident|maintenance).
// No auth required.
func (a *API) GetOpsTicketHistory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	entityPK := r.URL.Query().Get("entity_pk")
	if entityPK == "" {
		http.Error(w, "entity_pk is required", http.StatusBadRequest)
		return
	}

	limit := r.URL.Query().Get("limit")
	if limit == "" {
		limit = "5"
	}

	ticketType := r.URL.Query().Get("type")
	entityType := r.URL.Query().Get("entity_type")

	query := buildHistoryQuery(entityPK, limit, ticketType, entityType)

	client := newOpsClient()
	body, err := client.get(ctx, "/tickets"+query)
	if err != nil {
		http.Error(w, "failed to fetch ops ticket history", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// OpsAssignee is a single assignee/contributor entry, optionally enriched with a ClickHouse pubkey.
type OpsAssignee struct {
	Value  string `json:"value"`
	Label  string `json:"label"`
	Type   string `json:"type,omitempty"`
	Pubkey string `json:"pubkey,omitempty"`
}

// GetOpsAssignees serves GET /api/ops-tickets/assignees.
// Builds the contributor list directly from ClickHouse dz_contributors_current.
// The upstream /assignee-options endpoint was removed in API v1.1.0.
func (a *API) GetOpsAssignees(w http.ResponseWriter, r *http.Request) {
	if a.DB == nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}

	rows, err := a.DB.Query(r.Context(), `SELECT code, name, pk FROM dz_contributors_current ORDER BY name`)
	if err != nil {
		http.Error(w, "failed to fetch contributors", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	assignees := make([]OpsAssignee, 0)
	for rows.Next() {
		var code, name, pk string
		if err := rows.Scan(&code, &name, &pk); err != nil {
			http.Error(w, "failed to read contributors", http.StatusInternalServerError)
			return
		}
		assignees = append(assignees, OpsAssignee{
			Value:  code,
			Label:  name,
			Type:   "contributor",
			Pubkey: pk,
		})
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "failed to read contributors", http.StatusInternalServerError)
		return
	}

	data, err := json.Marshal(map[string]any{"assignees": assignees})
	if err != nil {
		http.Error(w, "failed to encode assignees", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}
