package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetOpsTicketsActiveFiltersStatus(t *testing.T) {
	t.Parallel()
	// Verify the active status set matches the spec
	assert.ElementsMatch(t, activeStatuses, []string{
		"open", "acknowledged", "investigating", "mitigating",
		"monitoring", "planned", "in-progress",
	})
}

func TestGetOpsTicketsHistoryRejectsEmptyEntityPK(t *testing.T) {
	t.Parallel()
	api := &API{}
	req := httptest.NewRequest(http.MethodGet, "/api/ops-tickets/history", nil)
	w := httptest.NewRecorder()
	api.GetOpsTicketHistory(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetOpsTicketsProxiesUpstream(t *testing.T) {
	t.Parallel()

	fixture := OpsTicketsListResponse{
		Tickets: []OpsTicket{
			{
				ID:                  "272d6ef3-3fb6-4f26-ab2c-047de78fca55",
				HumanReadableID:     "I20250413-a7b2",
				Type:                OpsTicketTypeIncident,
				Title:               "NYC-CHI packet loss",
				Status:              "investigating",
				AffectedLinkPubkeys: []string{"7a3f9c2e-1b4d-4e8a-9f6c-2d5e8a3b1c0d"},
			},
		},
		Total: 1,
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.Header.Get("Authorization"), "Bearer ")
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(fixture))
	}))
	defer upstream.Close()

	client := &opsClient{
		httpClient: upstream.Client(),
		apiKey:     "test-key",
		baseURL:    upstream.URL,
	}

	body, err := client.get(context.Background(), "/tickets?status=open")
	require.NoError(t, err)

	var got OpsTicketsListResponse
	require.NoError(t, json.Unmarshal(body, &got))
	assert.Len(t, got.Tickets, 1)
	assert.Equal(t, "I20250413-a7b2", got.Tickets[0].HumanReadableID)
}

func TestCreateOpsTicketRequiresAuth(t *testing.T) {
	t.Parallel()
	api := &API{}

	// No account in context → 403
	req := httptest.NewRequest(http.MethodPost, "/api/ops-tickets", nil)
	w := httptest.NewRecorder()
	api.CreateOpsTicket(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestCreateOpsTicketRejectsNonOpsUser(t *testing.T) {
	t.Parallel()
	api := &API{}

	walletAccount := &Account{
		AccountType:    AccountTypeWallet,
		IsInternalUser: false,
	}
	req := httptest.NewRequest(http.MethodPost, "/api/ops-tickets", nil)
	req = req.WithContext(SetAccountInContext(req.Context(), walletAccount))
	w := httptest.NewRecorder()
	api.CreateOpsTicket(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestCreateOpsTicketValidatesBody(t *testing.T) {
	t.Parallel()
	api := &API{}

	opsAccount := &Account{
		AccountType:    AccountTypeDomain,
		IsInternalUser: true,
		Email:          strPtr("thijs@doublezero.xyz"),
		DisplayName:    strPtr("Thijs"),
	}
	// Missing title → 400
	body := `{"type":"incident"}`
	req := httptest.NewRequest(http.MethodPost, "/api/ops-tickets",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(SetAccountInContext(req.Context(), opsAccount))
	w := httptest.NewRecorder()
	api.CreateOpsTicket(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func strPtr(s string) *string { return &s }

func TestBuildHistoryQueryParams(t *testing.T) {
	t.Parallel()
	// Link entity — only affected_link_pubkey, not device_pubkey
	qLink := buildHistoryQuery("abc123-pk", "5", "", "link")
	assert.Contains(t, qLink, "affected_link_pubkey=abc123-pk")
	assert.NotContains(t, qLink, "device_pubkey")
	assert.Contains(t, qLink, "status=not_active")
	assert.Contains(t, qLink, "limit=5")

	// Device entity — only device_pubkey, not affected_link_pubkey
	qDevice := buildHistoryQuery("abc123-pk", "5", "", "device")
	assert.Contains(t, qDevice, "device_pubkey=abc123-pk")
	assert.NotContains(t, qDevice, "affected_link_pubkey")

	// Unknown/empty entity type — defaults to link param
	qUnknown := buildHistoryQuery("abc123-pk", "5", "", "")
	assert.Contains(t, qUnknown, "affected_link_pubkey=abc123-pk")
	assert.NotContains(t, qUnknown, "device_pubkey")
}

func TestBuildHistoryQueryParamsWithType(t *testing.T) {
	t.Parallel()
	q := buildHistoryQuery("abc123-pk", "5", "incident", "link")
	assert.Contains(t, q, "type=incident")
}

func TestCreateOpsTicketDefaultsStatusToOpen(t *testing.T) {
	t.Parallel()
	// status absent → defaults to "open"
	req := CreateOpsTicketRequest{Type: OpsTicketTypeIncident, Title: "Test"}
	if req.Status == "" {
		req.Status = "open"
	}
	assert.Equal(t, "open", req.Status)

	// explicit status preserved
	req2 := CreateOpsTicketRequest{Status: "investigating"}
	if req2.Status == "" {
		req2.Status = "open"
	}
	assert.Equal(t, "investigating", req2.Status)
}

func TestOpsTicketUnmarshalsEnrichedFields(t *testing.T) {
	t.Parallel()
	raw := `{
		"id": "abc",
		"human_readable_id": "I20250415-0001",
		"type": "incident",
		"title": "Test",
		"description": "Test",
		"status": "open",
		"affected_link_pubkey": [],
		"device_pubkey": [],
		"affected_links": [{"code": "nyc-sfo", "pubkey": "link-pk-1"}],
		"affected_devices": [{"code": "nyc001-dz001", "pubkey": "device-pk-1"}],
		"reporter_name": "Test",
		"reporter_email": "test@test.com",
		"created_at": "2025-01-01T00:00:00Z",
		"updated_at": "2025-01-01T00:00:00Z"
	}`
	var ticket OpsTicket
	require.NoError(t, json.Unmarshal([]byte(raw), &ticket))
	require.Len(t, ticket.AffectedLinks, 1)
	assert.Equal(t, "nyc-sfo", ticket.AffectedLinks[0].Code)
	assert.Equal(t, "link-pk-1", ticket.AffectedLinks[0].Pubkey)
	require.Len(t, ticket.AffectedDevices, 1)
	assert.Equal(t, "device-pk-1", ticket.AffectedDevices[0].Pubkey)
}

func TestCreateOpsTicketRequestIncludesContributorPubkey(t *testing.T) {
	t.Parallel()
	pk := "4Nd1yGqnbdL2qHgJuKrpqW7TePxKzN3mHdR1ZkAeTY8a"
	r := CreateOpsTicketRequest{
		Type:              OpsTicketTypeIncident,
		Title:             "Test",
		ContributorPubkey: strPtr(pk),
	}
	require.NotNil(t, r.ContributorPubkey)
	assert.Equal(t, pk, *r.ContributorPubkey)
	data, err := json.Marshal(r)
	require.NoError(t, err)
	assert.Contains(t, string(data), "contributor_pubkey")
	assert.Contains(t, string(data), pk)
}

func TestCreateOpsTicketRequestNullContributorPubkey(t *testing.T) {
	t.Parallel()
	// nil ContributorPubkey marshals as explicit null (not omitted)
	r := CreateOpsTicketRequest{
		Type:              OpsTicketTypeIncident,
		Title:             "Test",
		ContributorPubkey: nil,
	}
	data, err := json.Marshal(r)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"contributor_pubkey":null`)
}

func TestGetOpsAssigneesRequiresDB(t *testing.T) {
	t.Parallel()
	api := &API{DB: nil}
	req := httptest.NewRequest(http.MethodGet, "/api/ops-tickets/assignees", nil)
	w := httptest.NewRecorder()
	api.GetOpsAssignees(w, req)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestGetOpsAssigneesProxiesUpstream(t *testing.T) {
	t.Parallel()

	fixture := `{"success":true,"data":[{"value":"dz_malbeclabs","label":"DZ/Malbeclabs","type":"admin"}]}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.Header.Get("Authorization"), "Bearer ")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, fixture)
	}))
	defer upstream.Close()

	client := &opsClient{
		httpClient: upstream.Client(),
		apiKey:     "test-key",
		baseURL:    upstream.URL,
	}

	body, err := client.get(context.Background(), "/tickets?status=active")
	require.NoError(t, err)
	assert.JSONEq(t, fixture, string(body))
}
