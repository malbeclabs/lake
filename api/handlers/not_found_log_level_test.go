package handlers_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/malbeclabs/lake/indexer/pkg/neo4j"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A lookup miss is an ordinary answer and must log at WARN: every ERR line
// pages through the lake-api-errors alert. A real failure at the same site must
// still log at ERR. These tests swap the global slog default, so none is parallel.

func recordLogs(t *testing.T) *[]slog.Record {
	t.Helper()
	var recs []slog.Record
	prev := slog.Default()
	slog.SetDefault(slog.New(recordingHandler{&recs}))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &recs
}

func requireLoggedAt(t *testing.T, recs []slog.Record, msg string, level slog.Level) {
	t.Helper()
	var found []slog.Record
	for _, r := range recs {
		if r.Message == msg {
			found = append(found, r)
		}
	}
	require.Len(t, found, 1, "want exactly one %q record", msg)
	assert.Equal(t, level, found[0].Level, "%q", msg)
}

func withPK(req *http.Request, pk string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", pk)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

// stubNeo4jClient answers every query with the same records, so a test can
// reach a result shape the uniqueness constraints keep out of a real graph.
type stubNeo4jClient struct {
	neo4j.Client
	records []*neo4jdriver.Record
}

func (c stubNeo4jClient) Session(context.Context) (neo4j.Session, error) {
	return stubNeo4jSession{records: c.records}, nil
}

type stubNeo4jSession struct {
	neo4j.Session
	records []*neo4jdriver.Record
}

func (s stubNeo4jSession) Run(context.Context, string, map[string]any) (neo4j.Result, error) {
	return stubNeo4jResult{records: s.records}, nil
}

func (s stubNeo4jSession) Close(context.Context) error { return nil }

type stubNeo4jResult struct {
	neo4j.Result
	records []*neo4jdriver.Record
}

func (r stubNeo4jResult) Collect(context.Context) ([]*neo4jdriver.Record, error) {
	return r.records, nil
}

func twoRecords() []*neo4jdriver.Record {
	return []*neo4jdriver.Record{
		{Keys: []string{"a"}, Values: []any{"1"}},
		{Keys: []string{"a"}, Values: []any{"2"}},
	}
}

func TestGetISISPath_NoPathLogsWarn(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)
	api.Neo4jClient = apitesting.SetupNeo4jWithDataForTest(t, testNeo4jDB, nil)
	recs := recordLogs(t)

	rr := httptest.NewRecorder()
	api.GetISISPath(rr, httptest.NewRequest(http.MethodGet, "/api/topology/path?from=dev-x&to=dev-y", nil))

	assert.Equal(t, http.StatusOK, rr.Code)
	var resp handlers.PathResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "No path found between devices", resp.Error)
	requireLoggedAt(t, *recs, "ISIS path no result", slog.LevelWarn)
}

func TestGetISISPath_MoreThanOneRecordLogsError(t *testing.T) {
	api := &handlers.API{Neo4jClient: stubNeo4jClient{records: twoRecords()}}
	recs := recordLogs(t)

	rr := httptest.NewRecorder()
	api.GetISISPath(rr, httptest.NewRequest(http.MethodGet, "/api/topology/path?from=dev-x&to=dev-y", nil))

	assert.Equal(t, http.StatusOK, rr.Code)
	var resp handlers.PathResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "No path found between devices", resp.Error)
	requireLoggedAt(t, *recs, "ISIS path no result", slog.LevelError)
}

func TestGetMetroDevicePaths_UnknownMetroLogsWarn(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)
	api.Neo4jClient = apitesting.SetupNeo4jWithDataForTest(t, testNeo4jDB, nil)
	recs := recordLogs(t)

	rr := httptest.NewRecorder()
	api.GetMetroDevicePaths(rr, httptest.NewRequest(http.MethodGet, "/api/topology/metro-device-paths?from=metro-x&to=metro-y", nil))

	assert.Equal(t, http.StatusOK, rr.Code)
	var resp handlers.MetroDevicePathsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "One or both metros not found", resp.Error)
	requireLoggedAt(t, *recs, "metro device paths metro query no result", slog.LevelWarn)
}

func TestGetMetroDevicePaths_MoreThanOneRecordLogsError(t *testing.T) {
	api := &handlers.API{Neo4jClient: stubNeo4jClient{records: twoRecords()}}
	recs := recordLogs(t)

	rr := httptest.NewRecorder()
	api.GetMetroDevicePaths(rr, httptest.NewRequest(http.MethodGet, "/api/topology/metro-device-paths?from=metro-x&to=metro-y", nil))

	assert.Equal(t, http.StatusOK, rr.Code)
	var resp handlers.MetroDevicePathsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "One or both metros not found", resp.Error)
	requireLoggedAt(t, *recs, "metro device paths metro query no result", slog.LevelError)
}

// The four multicast sites resolve the group the same way. A migrated database
// with no such group is the miss; a bare one, where the table does not exist,
// is a real query failure.
func TestMulticastGroupLookups_LogLevel(t *testing.T) {
	sites := []struct {
		name     string
		handler  func(*handlers.API) http.HandlerFunc
		msg      string
		wantCode int
		// wantJSONError is the error field of a 200 JSON response; empty means
		// the handler answers with a plain-text 404.
		wantJSONError string
	}{
		{"member counts", func(a *handlers.API) http.HandlerFunc { return a.GetMulticastGroupMemberCounts },
			"multicast group member counts group query error", http.StatusNotFound, ""},
		{"tree paths", func(a *handlers.API) http.HandlerFunc { return a.GetMulticastTreePaths },
			"multicast tree paths group query error", http.StatusOK, "multicast group not found"},
		{"tree segments", func(a *handlers.API) http.HandlerFunc { return a.GetMulticastTreeSegments },
			"multicast tree segments group query error", http.StatusOK, "multicast group not found"},
		{"shred stats", func(a *handlers.API) http.HandlerFunc { return a.GetMulticastGroupShredStats },
			"multicast group shred stats group query error", http.StatusNotFound, ""},
	}
	setups := []struct {
		name      string
		newAPI    func(*testing.T) *handlers.API
		wantLevel slog.Level
	}{
		{"unknown group", func(t *testing.T) *handlers.API { return apitesting.NewTestAPI(t, testChDB) }, slog.LevelWarn},
		{"query failure", func(t *testing.T) *handlers.API { return apitesting.NewTestAPIBare(t, testChDB) }, slog.LevelError},
	}

	for _, site := range sites {
		for _, setup := range setups {
			t.Run(site.name+"/"+setup.name, func(t *testing.T) {
				api := setup.newAPI(t)
				recs := recordLogs(t)

				rr := httptest.NewRecorder()
				site.handler(api)(rr, withPK(httptest.NewRequest(http.MethodGet, "/", nil), "does-not-exist"))

				assert.Equal(t, site.wantCode, rr.Code)
				if site.wantJSONError != "" {
					var resp struct{ Error string }
					require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
					assert.Equal(t, site.wantJSONError, resp.Error)
				} else {
					assert.Equal(t, "multicast group not found\n", rr.Body.String())
				}
				requireLoggedAt(t, *recs, site.msg, setup.wantLevel)
			})
		}
	}
}

// An unknown link still answers 500, as it did before; only the log level moves.
func TestGetSingleLinkHistory_LogLevel(t *testing.T) {
	tests := []struct {
		name      string
		newAPI    func(*testing.T) *handlers.API
		wantLevel slog.Level
	}{
		{"unknown link", func(t *testing.T) *handlers.API { return apitesting.NewTestAPI(t, testChDB) }, slog.LevelWarn},
		{"query failure", func(t *testing.T) *handlers.API { return apitesting.NewTestAPIBare(t, testChDB) }, slog.LevelError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := tt.newAPI(t)
			recs := recordLogs(t)

			rr := httptest.NewRecorder()
			api.GetSingleLinkHistory(rr, withPK(httptest.NewRequest(http.MethodGet, "/", nil), "does-not-exist"))

			assert.Equal(t, http.StatusInternalServerError, rr.Code)
			assert.Equal(t, "Internal server error\n", rr.Body.String())
			requireLoggedAt(t, *recs, "error fetching single link history", tt.wantLevel)
		})
	}
}
