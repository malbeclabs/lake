package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	// The whole string, not a substring: an assertion that only looks for "invalid token" keeps
	// passing after the status or the wrapper is dropped from the message, which is the part an
	// operator reads first. See .cursor/BUGBOT.md.
	const want = "promql: http 401: authentication error: invalid token"
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

// A 200 carrying status:"error" is the shape a malformed query comes back as, and reading only the
// HTTP status would fold it into an empty result — a query that silently returns nothing.
func TestPromClient_QueryRejectsAnErrorEnvelope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"error","errorType":"bad_data","error":"parse error"}`))
	}))
	defer srv.Close()

	_, err := NewPromClient(srv.URL, "u", "t").Query(context.Background(), "up{")
	const want = "promql: bad_data: parse error"
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
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

	_, err := NewPromClient(srv.URL, "u", "t").Query(context.Background(), "up[5m]")
	const want = `promql: unexpected result type "matrix"`
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

// Nil is a supported state, not a degraded one: an environment with no credentials renders no
// column. A client that existed and failed every query would log a failure every refresh cycle
// for data it was never meant to have.
func TestNewPromClientFromEnv_NilUntilAllThreeAreSet(t *testing.T) {
	// The user matters as much as the token: the store authenticates with basic auth, so a
	// configuration missing it builds a client that sends an empty username and is refused 401
	// every cycle — behind a column that renders exactly like the supported unconfigured state.
	for _, tc := range []struct {
		name             string
		url, user, token string
	}{
		{"nothing set", "", "", ""},
		{"url only", "https://example.invalid/api/prom", "", ""},
		{"url and token, no user", "https://example.invalid/api/prom", "", "tok"},
		{"url and user, no token", "https://example.invalid/api/prom", "1946814", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GRAFANA_PROM_URL", tc.url)
			t.Setenv("GRAFANA_PROM_USER", tc.user)
			t.Setenv("GRAFANA_PROM_TOKEN", tc.token)
			if c := NewPromClientFromEnv(); c != nil {
				t.Fatal("want nil: a partial configuration must not build a client")
			}
		})
	}

	t.Setenv("GRAFANA_PROM_URL", "https://example.invalid/api/prom")
	t.Setenv("GRAFANA_PROM_USER", "1946814")
	t.Setenv("GRAFANA_PROM_TOKEN", "tok")
	if c := NewPromClientFromEnv(); c == nil {
		t.Fatal("want a client once all three are set")
	}
}

// A nil *PromClient stored in a PromQuerier makes the interface non-nil, so the fold's
// `a.Prom == nil` guard reads false and this method is reached with a nil receiver. It is fixed at
// the construction site; this is the guard that keeps the remaining path an error the refresher
// logs rather than a panic in a background goroutine, which ends the process.
func TestPromClient_QueryOnANilReceiverErrorsRatherThanPanics(t *testing.T) {
	// govet's nilness analyzer proves `q == nil` is false here, which is the trap stated as a
	// compile-time fact rather than a runtime one — so the assertion is left to the analyzer and
	// this test covers what it cannot: that reaching Query through it costs an error, not a panic.
	var c *PromClient
	var q PromQuerier = c
	_, err := q.Query(context.Background(), "up")
	const want = "promql: client not configured"
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

// The credentials go on the wire together or not at all. Half a pair is refused 401 exactly as no
// header is, while the header's presence suggests the credential was wrong rather than absent.
func TestPromClient_QuerySendsNoAuthWithHalfACredential(t *testing.T) {
	for _, tc := range []struct{ name, user, token string }{
		{"user only", "1946814", ""},
		{"token only", "", "tok"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sawAuth bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _, sawAuth = r.BasicAuth()
				_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
			}))
			defer srv.Close()
			if _, err := NewPromClient(srv.URL, tc.user, tc.token).Query(context.Background(), "up"); err != nil {
				t.Fatalf("Query: %v", err)
			}
			if sawAuth {
				t.Fatal("a half credential pair was sent as basic auth")
			}
		})
	}
}

// A URL logged verbatim is a credential in a log file for as long as the logs are kept.
func TestPromClient_SafeURLDropsEmbeddedCredentials(t *testing.T) {
	got := NewPromClient("https://user:secret@example.invalid/api/prom", "u", "t").SafeURL()
	const want = "https://example.invalid/api/prom"
	if got != want {
		t.Fatalf("SafeURL() = %q, want %q", got, want)
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
