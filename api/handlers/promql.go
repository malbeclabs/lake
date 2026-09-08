package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// A minimal PromQL instant-query client, and the only place in this API that reads a metrics
// store. It exists for one caller — the edge multicast conformance fold — and is deliberately
// smaller than a Prometheus client library: one instant query, no ranges, no series metadata,
// no service discovery.
//
// # Why this is here at all
//
// dz-conformance writes its verdicts nowhere but Prometheus. It has no database sink; its
// reporters are slog, a --json-report file and a metrics endpoint scraped by Alloy and
// remote-written to Grafana Cloud. Reading them therefore means speaking PromQL, and that makes
// this the only dependency class in the API besides ClickHouse and Postgres.
//
// It is an interim. The successor is recorder.conformance_finding in malbeclabs/edge-multicast-ref,
// whose rows carry the publisher source address and so can sit on the publisher LINES — see
// docs/superpowers/specs/2026-09-08-edge-multicast-conformance-column-design.md. When those rows
// land, one fetch function is replaced and this file is deleted.

// PromSample is one instant-vector element: its label set and its value.
type PromSample struct {
	Labels map[string]string
	Value  float64
}

// Label returns the named label, or "" when the sample does not carry it. A missing label is
// never an error here: a series scraped before a target label was added simply has less identity
// than one scraped after, and the caller decides whether that is usable.
func (s PromSample) Label(name string) string {
	return s.Labels[name]
}

// PromQuerier is the seam tests inject against. The conformance fetch talks to this and never to
// an HTTP client, so its folding logic is exercised without a server.
type PromQuerier interface {
	Query(ctx context.Context, query string) ([]PromSample, error)
}

// PromClient queries a Prometheus-compatible HTTP API with basic auth.
type PromClient struct {
	baseURL string
	user    string
	token   string
	client  *http.Client
}

// promTimeout bounds a single query. The conformance leg issues four of them inside the
// refresher's own three-minute budget, so this is comfortably under it while still being long
// enough for a cold query against a hosted store.
const promTimeout = 20 * time.Second

// NewPromClient builds a client against baseURL, which is the API root that carries /api/v1/query
// beneath it — for Grafana Cloud hosted Prometheus that is the ".../api/prom" form.
func NewPromClient(baseURL, user, token string) *PromClient {
	return &PromClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		user:    user,
		token:   token,
		client:  &http.Client{Timeout: promTimeout},
	}
}

// NewPromClientFromEnv builds the client from the environment, or returns nil when it is not
// configured.
//
// Nil is a supported state and not a degraded one: local dev, staging before a token exists and
// PR previews all run without it, and the column simply does not render. The alternative — a
// client that exists and fails every query — would log an error every refresh cycle for an
// environment that was never meant to have the data.
func NewPromClientFromEnv() *PromClient {
	base := strings.TrimSpace(os.Getenv("GRAFANA_PROM_URL"))
	user := strings.TrimSpace(os.Getenv("GRAFANA_PROM_USER"))
	token := strings.TrimSpace(os.Getenv("GRAFANA_PROM_TOKEN"))
	if base == "" || token == "" {
		return nil
	}
	return NewPromClient(base, user, token)
}

// promResponse is the instant-query envelope. Only the fields this client reads are declared.
type promResponse struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	Data      struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			// [ <unix seconds, number>, <value, string> ] — Prometheus encodes the value as a
			// string so that +Inf, -Inf and NaN survive JSON, which is why this is raw.
			Value []json.RawMessage `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// Query runs an instant query and returns its vector.
//
// POST rather than GET: these queries carry label matchers and an aggregation and are well past
// the length a query string should be trusted with, and every Prometheus-compatible API accepts
// the form-encoded POST form.
func (c *PromClient) Query(ctx context.Context, query string) ([]PromSample, error) {
	form := url.Values{"query": []string{query}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/api/v1/query", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if c.user != "" || c.token != "" {
		req.SetBasicAuth(c.user, c.token)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	// Bounded read: an unbounded io.ReadAll against a store that answers with something
	// unexpected is a memory question, and no answer this client wants is large.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		// The body is included because Prometheus puts the actual complaint there — a bad
		// matcher, an expired token — and the status alone names none of them. Truncated so a
		// stray HTML error page cannot fill a log line.
		return nil, fmt.Errorf("promql: http %d: %s", resp.StatusCode, truncateForLog(string(body), 200))
	}

	var parsed promResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("promql: decode: %w", err)
	}
	if parsed.Status != "success" {
		return nil, fmt.Errorf("promql: %s: %s", parsed.ErrorType, parsed.Error)
	}
	if parsed.Data.ResultType != "vector" {
		return nil, fmt.Errorf("promql: unexpected result type %q", parsed.Data.ResultType)
	}

	out := make([]PromSample, 0, len(parsed.Data.Result))
	for _, r := range parsed.Data.Result {
		if len(r.Value) != 2 {
			continue
		}
		var raw string
		if err := json.Unmarshal(r.Value[1], &raw); err != nil {
			continue
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			// A NaN or an infinity is not a count. Dropping the element is right: it carries
			// no quantity, and folding it as zero would assert a measurement that was not made.
			continue
		}
		out = append(out, PromSample{Labels: r.Metric, Value: v})
	}
	return out, nil
}

// truncateForLog shortens s for a log line, marking that it was cut.
func truncateForLog(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// promCount turns an increase() value into a count of events.
//
// increase() extrapolates to the window edges, so a counter that moved once inside the window can
// report 0.7 or 1.03 and never exactly 1. Rounding alone is wrong at the bottom of the range: it
// turns a real event that landed near a window edge into zero, which on this page would be a
// violation that happened and was not reported.
//
// So the rule is: a value at or below zero is no events — increase() cannot produce a positive
// number from a counter that did not move — and anything above zero is at least one.
func promCount(v float64) uint64 {
	if v <= 0 {
		return 0
	}
	if r := math.Round(v); r >= 1 {
		return uint64(r)
	}
	return 1
}
