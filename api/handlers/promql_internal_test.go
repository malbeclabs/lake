package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPromClient_QueryParsesAVector(t *testing.T) {
	var gotQuery, gotUser, gotPass, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		_ = r.ParseForm()
		gotQuery = r.Form.Get("query")
		gotUser, gotPass, _ = r.BasicAuth()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[
			{"metric":{"multicast_group":"233.84.178.3","result":"pass"},"value":[1757000000,"9000.5"]},
			{"metric":{"multicast_group":"233.84.178.3","result":"na"},"value":[1757000000,"12"]}
		]}}`))
	}))
	defer srv.Close()

	c := NewPromClient(srv.URL+"/api/prom", "1946814", "tok")
	got, err := c.Query(context.Background(), `sum by (result) (increase(x[15m]))`)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	// POST, not GET: these queries carry matchers and an aggregation and are past the length a
	// query string should be trusted with.
	if gotMethod != http.MethodPost {
		t.Fatalf("method = %s, want POST", gotMethod)
	}
	if gotQuery != `sum by (result) (increase(x[15m]))` {
		t.Fatalf("query = %q, not what was asked", gotQuery)
	}
	if gotUser != "1946814" || gotPass != "tok" {
		t.Fatalf("basic auth = %q/%q, want the configured credentials", gotUser, gotPass)
	}
	if len(got) != 2 {
		t.Fatalf("samples = %d, want 2", len(got))
	}
	if got[0].Label("multicast_group") != "233.84.178.3" || got[0].Value != 9000.5 {
		t.Fatalf("first sample = %+v", got[0])
	}
	if got[0].Label("nonexistent") != "" {
		t.Fatal("a missing label must read empty, never panic")
	}
}

// The store puts the actual complaint in the body — a bad matcher, an expired token — and the
// status code names none of them, so the body has to reach the log line.
func TestPromClient_QueryErrorCarriesTheBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("authentication error: invalid token"))
	}))
	defer srv.Close()

	_, err := NewPromClient(srv.URL, "u", "t").Query(context.Background(), "up")
	if err == nil {
		t.Fatal("want an error on a non-200")
	}
	if !strings.Contains(err.Error(), "invalid token") {
		t.Fatalf("error = %q, want it to carry the body", err)
	}
}

// A 200 carrying status:"error" is the shape a malformed query comes back as, and reading only the
// HTTP status would fold it into an empty result — a query that silently returns nothing.
func TestPromClient_QueryRejectsAnErrorEnvelope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"error","errorType":"bad_data","error":"parse error"}`))
	}))
	defer srv.Close()

	if _, err := NewPromClient(srv.URL, "u", "t").Query(context.Background(), "up{"); err == nil {
		t.Fatal("want an error when the envelope says error")
	}
}

// A NaN or an infinity is not a count. Folding one as zero would assert a measurement that was
// never made, so the element is dropped and the rest of the vector still lands.
func TestPromClient_QueryDropsNonNumericValues(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[
			{"metric":{"a":"1"},"value":[1757000000,"NaN"]},
			{"metric":{"a":"2"},"value":[1757000000,"+Inf"]},
			{"metric":{"a":"3"},"value":[1757000000,"7"]}
		]}}`))
	}))
	defer srv.Close()

	got, err := NewPromClient(srv.URL, "u", "t").Query(context.Background(), "up")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(got) != 1 || got[0].Label("a") != "3" {
		t.Fatalf("samples = %+v, want only the numeric one", got)
	}
}

// A matrix where a vector was expected means the query was not the one this client thinks it
// wrote. Returning it as an empty vector would read as "nothing is happening".
func TestPromClient_QueryRejectsAMatrix(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
	}))
	defer srv.Close()

	if _, err := NewPromClient(srv.URL, "u", "t").Query(context.Background(), "up[5m]"); err == nil {
		t.Fatal("want an error on a non-vector result")
	}
}

// Nil is a supported state, not a degraded one: an environment with no credentials renders no
// column. A client that existed and failed every query would log a failure every refresh cycle
// for data it was never meant to have.
func TestNewPromClientFromEnv_NilWithoutCredentials(t *testing.T) {
	t.Setenv("GRAFANA_PROM_URL", "")
	t.Setenv("GRAFANA_PROM_TOKEN", "")
	if c := NewPromClientFromEnv(); c != nil {
		t.Fatal("want nil with no credentials configured")
	}

	t.Setenv("GRAFANA_PROM_URL", "https://example.invalid/api/prom")
	if c := NewPromClientFromEnv(); c != nil {
		t.Fatal("want nil with a url but no token")
	}

	t.Setenv("GRAFANA_PROM_TOKEN", "tok")
	if c := NewPromClientFromEnv(); c == nil {
		t.Fatal("want a client once both are set")
	}
}

// The base URL is joined, so a trailing slash in configuration must not produce a double slash in
// the path — some gateways answer that with a 404 and nothing else says why.
func TestNewPromClient_TrimsTrailingSlash(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	}))
	defer srv.Close()

	if _, err := NewPromClient(srv.URL+"/api/prom/", "u", "t").Query(context.Background(), "up"); err != nil {
		t.Fatalf("Query: %v", err)
	}
	if gotPath != "/api/prom/api/v1/query" {
		t.Fatalf("path = %q, want /api/prom/api/v1/query", gotPath)
	}
}
