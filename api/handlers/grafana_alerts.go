package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
	"github.com/slack-go/slack"
)

// Grafana webhook payload types.

type grafanaWebhook struct {
	Receiver    string            `json:"receiver"`
	Status      string            `json:"status"`
	Alerts      []grafanaAlert    `json:"alerts"`
	GroupLabels map[string]string `json:"groupLabels"`
	ExternalURL string            `json:"externalURL"`
	Title       string            `json:"title"`
}

type grafanaAlert struct {
	Status       string            `json:"status"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL"`
	Fingerprint  string            `json:"fingerprint"`
}

// HandleGrafanaAlerts receives a Grafana webhook, enriches alerts with
// live ClickHouse data, and posts a formatted message to Slack.
//
// Query parameters:
//   - channel: Slack channel ID to post to (required)
func (a *API) HandleGrafanaAlerts(w http.ResponseWriter, r *http.Request) {
	var payload grafanaWebhook
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		logError("grafana webhook: failed to parse", "error", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	channelID := r.URL.Query().Get("channel")
	if channelID == "" {
		logError("grafana webhook: missing channel parameter")
		http.Error(w, "missing channel parameter", http.StatusBadRequest)
		return
	}

	start := time.Now()
	alertName := payload.GroupLabels["alertname"]

	if err := a.postEnrichedAlerts(r.Context(), payload, channelID); err != nil {
		logError("grafana webhook: failed to post", "error", err, "channel", channelID)
		metrics.GrafanaWebhookTotal.WithLabelValues("error", alertName).Inc()
		metrics.GrafanaWebhookDuration.Observe(time.Since(start).Seconds())
		http.Error(w, "failed to post to slack", http.StatusInternalServerError)
		return
	}

	metrics.GrafanaWebhookTotal.WithLabelValues("success", alertName).Inc()
	metrics.GrafanaWebhookDuration.Observe(time.Since(start).Seconds())
	w.WriteHeader(http.StatusOK)
}

func (a *API) postEnrichedAlerts(ctx context.Context, payload grafanaWebhook, channelID string) error {
	botToken := os.Getenv("GRAFANA_SLACK_BOT_TOKEN")
	if botToken == "" {
		return fmt.Errorf("GRAFANA_SLACK_BOT_TOKEN not configured")
	}
	api := slack.New(botToken)

	var sections []string
	for _, alert := range payload.Alerts {
		sections = append(sections, a.enrichAndFormat(ctx, alert))
	}

	color := "#E01E5A" // red for firing
	if payload.Status == "resolved" {
		color = "#2EB67D" // green for resolved
	}

	var titleLink string
	if len(payload.Alerts) > 0 {
		titleLink = payload.Alerts[0].GeneratorURL
	}

	attachment := slack.Attachment{
		Color:      color,
		Title:      payload.Title,
		TitleLink:  titleLink,
		Text:       strings.Join(sections, "\n\n"),
		MarkdownIn: []string{"text"},
	}

	_, _, err := api.PostMessageContext(ctx, channelID,
		slack.MsgOptionAttachments(attachment),
		slack.MsgOptionDisableLinkUnfurl(),
	)
	return err
}

// enrichAndFormat dispatches to an alert-type-specific formatter, falling back
// to a generic renderer for unrecognised alert types.
func (a *API) enrichAndFormat(ctx context.Context, alert grafanaAlert) string {
	name := alert.Labels["alertname"]
	switch {
	case strings.Contains(name, "Link Down"), strings.Contains(name, "Link Degraded"):
		return a.formatLinkAlert(ctx, alert)
	case strings.Contains(name, "Link Interface"):
		return a.formatLinkIntfAlert(ctx, alert)
	case strings.Contains(name, "Device Interface"):
		return a.formatDeviceIntfAlert(ctx, alert)
	case strings.Contains(name, "Device Not Reporting"):
		return a.formatDeviceNotReportingAlert(ctx, alert)
	default:
		return formatGenericAlert(alert)
	}
}

// --- Link Down / Degraded ---

func (a *API) formatLinkAlert(ctx context.Context, alert grafanaAlert) string {
	l := alert.Labels
	linkPK := l["link_pk"]
	linkPKShort := l["link_pk_short"]
	contributor := l["contributor_code"]
	linkType := l["link_type"]
	bandwidth := l["bandwidth"]
	metro := l["metro"]

	e := a.enrichLink(ctx, linkPK, alert)

	linkURL := fmt.Sprintf("https://data.malbeclabs.com/dz/links/%s", linkPK)

	isis := "IS-IS UP"
	if e.IsisDown {
		isis = "IS-IS DOWN"
	}

	summary := fmt.Sprintf("*Link <%s|%s> · %s · %s ago*", linkURL, linkPKShort, e.StartedAt, e.Duration)
	desc := fmt.Sprintf("%s · %s · %s · %s · %s", contributor, linkType, bandwidth, metro, isis)
	loss := fmt.Sprintf("Loss: %.1f%% A→Z / %.1f%% Z→A (max %.1f%%)", e.ALossPct, e.ZLossPct, e.MaxLossPct)

	return joinLines(summary, desc, loss, footerLine(alert, "https://data.malbeclabs.com/status/links"))
}

type linkEnrichment struct {
	ALossPct   float64
	ZLossPct   float64
	MaxLossPct float64
	IsisDown   bool
	Duration   string
	StartedAt  string
}

func (a *API) enrichLink(ctx context.Context, linkPK string, alert grafanaAlert) linkEnrichment {
	db := a.DB
	if db == nil {
		return linkEnrichment{Duration: "-", StartedAt: "-"}
	}

	var e linkEnrichment

	// Current state — latest row, no condition filter.
	row := db.QueryRow(ctx, `
		SELECT r.a_loss_pct, r.z_loss_pct, greatest(r.a_loss_pct, r.z_loss_pct), r.isis_down
		FROM lake.link_rollup_5m r FINAL
		WHERE r.link_pk = ?
		  AND r.bucket_ts >= now() - INTERVAL 20 MINUTE
		  AND NOT r.provisioning
		ORDER BY r.bucket_ts DESC
		LIMIT 1`, linkPK)
	if err := row.Scan(&e.ALossPct, &e.ZLossPct, &e.MaxLossPct, &e.IsisDown); err != nil {
		slog.Warn("grafana webhook: link rollup query failed", "error", err, "link_pk", linkPK)
	}

	// Duration — from Grafana's startsAt if available, otherwise ClickHouse lookup.
	if !alert.StartsAt.IsZero() {
		e.StartedAt = alert.StartsAt.UTC().Format("Jan 02 15:04 UTC")
		e.Duration = fmtDuration(time.Since(alert.StartsAt))
	} else {
		alertName := alert.Labels["alertname"]
		okCond := "NOT (greatest(a_loss_pct, z_loss_pct) > 10 OR isis_down)"
		if strings.Contains(alertName, "Degraded") {
			okCond = "greatest(a_loss_pct, z_loss_pct) <= 1"
		}
		e.Duration, e.StartedAt = a.queryDuration(ctx,
			"lake.link_rollup_5m", "link_pk", linkPK,
			"AND NOT provisioning AND "+okCond)
	}

	return e
}

// --- Link Interface (carrier/discards/errors) ---

func (a *API) formatLinkIntfAlert(ctx context.Context, alert grafanaAlert) string {
	l := alert.Labels
	linkPK := l["link_pk"]
	linkPKShort := l["link_pk_short"]
	linkSide := l["link_side"]
	intf := l["intf"]
	contributor := l["contributor_code"]
	metro := l["metro"]

	metricCol, okCond := intfMetricInfo(alert.Labels["alertname"])
	e := a.enrichIntf(ctx, "link_pk", linkPK, intf, metricCol, okCond)

	linkURL := fmt.Sprintf("https://data.malbeclabs.com/dz/links/%s", linkPK)

	summary := fmt.Sprintf("*Link <%s|%s> · side %s %s · %s · %s ago*",
		linkURL, linkPKShort, linkSide, intf, e.StartedAt, e.Duration)
	desc := fmt.Sprintf("%s · %s · total: %d", contributor, metro, e.MetricTotal)
	if e.AffectedBuckets > 0 {
		desc = fmt.Sprintf("%s · %s\n%d/6 buckets (30m) · total: %d",
			contributor, metro, e.AffectedBuckets, e.MetricTotal)
	}

	return joinLines(summary, desc, footerLine(alert, "https://data.malbeclabs.com/status/links"))
}

// --- Device Interface (carrier/discards/errors) ---

func (a *API) formatDeviceIntfAlert(ctx context.Context, alert grafanaAlert) string {
	l := alert.Labels
	devicePK := l["device_pk"]
	devicePKShort := l["device_pk_short"]
	intf := l["intf"]
	contributor := l["contributor_code"]
	metro := l["metro"]

	metricCol, okCond := intfMetricInfo(alert.Labels["alertname"])
	e := a.enrichIntf(ctx, "device_pk", devicePK, intf, metricCol, okCond)

	deviceURL := fmt.Sprintf("https://data.malbeclabs.com/dz/devices/%s", devicePK)

	summary := fmt.Sprintf("*Device <%s|%s> · %s · %s · %s ago*",
		deviceURL, devicePKShort, intf, e.StartedAt, e.Duration)
	desc := fmt.Sprintf("%s · %s · total: %d", contributor, metro, e.MetricTotal)
	if e.AffectedBuckets > 0 {
		desc = fmt.Sprintf("%s · %s\n%d/6 buckets (30m) · total: %d",
			contributor, metro, e.AffectedBuckets, e.MetricTotal)
	}

	return joinLines(summary, desc, footerLine(alert, "https://data.malbeclabs.com/status/devices"))
}

// Shared enrichment for device/link interface alerts.

type intfEnrichment struct {
	MetricTotal     uint64
	AffectedBuckets int
	Duration        string
	StartedAt       string
}

// intfMetricInfo returns the ClickHouse expression for the metric column
// and the "OK" condition for duration computation, based on the alert name.
func intfMetricInfo(alertName string) (metricExpr string, okCond string) {
	switch {
	case strings.Contains(alertName, "Carrier"):
		return "carrier_transitions", "carrier_transitions = 0"
	case strings.Contains(alertName, "Discards"):
		return "(in_discards + out_discards)", "(in_discards + out_discards) = 0"
	default: // Errors
		return "(in_errors + out_errors + in_fcs_errors)", "(in_errors + out_errors + in_fcs_errors) = 0"
	}
}

func (a *API) enrichIntf(ctx context.Context, pkCol, pkVal, intf, metricExpr, okCond string) intfEnrichment {
	db := a.DB
	if db == nil {
		return intfEnrichment{Duration: "-", StartedAt: "-"}
	}

	var e intfEnrichment

	// Latest metric total.
	row := db.QueryRow(ctx, fmt.Sprintf(`
		SELECT %s
		FROM lake.device_interface_rollup_5m FINAL
		WHERE %s = ? AND intf = ?
		  AND bucket_ts >= now() - INTERVAL 20 MINUTE
		ORDER BY bucket_ts DESC
		LIMIT 1`, metricExpr, pkCol), pkVal, intf)
	if err := row.Scan(&e.MetricTotal); err != nil {
		slog.Warn("grafana webhook: intf rollup query failed", "error", err, pkCol, pkVal, "intf", intf)
		return intfEnrichment{Duration: "-", StartedAt: "-"}
	}

	// Affected buckets in the last 30 minutes (for sustained alerts).
	row = db.QueryRow(ctx, fmt.Sprintf(`
		SELECT count()
		FROM lake.device_interface_rollup_5m FINAL
		WHERE %s = ? AND intf = ?
		  AND bucket_ts >= now() - INTERVAL 30 MINUTE
		  AND %s > 0`, pkCol, metricExpr), pkVal, intf)
	if err := row.Scan(&e.AffectedBuckets); err != nil {
		e.AffectedBuckets = 0
	}

	// Duration since last clean bucket.
	e.Duration, e.StartedAt = a.queryDuration(ctx,
		"lake.device_interface_rollup_5m", pkCol, pkVal,
		fmt.Sprintf("AND intf = '%s' AND %s", intf, okCond))

	return e
}

// --- Device Not Reporting ---

func (a *API) formatDeviceNotReportingAlert(ctx context.Context, alert grafanaAlert) string {
	l := alert.Labels
	devicePK := l["device_pk"]
	devicePKShort := l["device_pk_short"]
	contributor := l["contributor_code"]
	deviceType := l["device_type"]
	metro := l["metro_code"]

	lastReport, duration := a.enrichDeviceNotReporting(ctx, devicePK)

	deviceURL := fmt.Sprintf("https://data.malbeclabs.com/dz/devices/%s", devicePK)

	summary := fmt.Sprintf("*Device <%s|%s> · %s · %s ago*", deviceURL, devicePKShort, lastReport, duration)
	desc := fmt.Sprintf("%s · %s · %s", contributor, deviceType, metro)

	return joinLines(summary, desc, footerLine(alert, "https://data.malbeclabs.com/status/devices"))
}

func (a *API) enrichDeviceNotReporting(ctx context.Context, devicePK string) (lastReport, duration string) {
	db := a.DB
	if db == nil {
		return "-", "-"
	}

	var lastTS time.Time
	row := db.QueryRow(ctx, `
		SELECT max(written_at)
		FROM lake.fact_dz_device_link_latency_sample_header
		WHERE origin_device_pk = ?
		  AND written_at >= now() - INTERVAL 1 HOUR`, devicePK)
	if err := row.Scan(&lastTS); err != nil || lastTS.IsZero() {
		return ">1h ago", ">1h"
	}
	return lastTS.UTC().Format("Jan 02 15:04 UTC"), fmtDuration(time.Since(lastTS))
}

// --- Generic fallback ---

func formatGenericAlert(alert grafanaAlert) string {
	var lines []string
	if s := alert.Annotations["summary"]; s != "" {
		lines = append(lines, fmt.Sprintf("*%s*", s))
	}
	if d := alert.Annotations["description"]; d != "" {
		lines = append(lines, d)
	}
	if u := alert.Annotations["runbook_url"]; u != "" {
		if !strings.Contains(strings.Join(lines, ""), "Runbook") {
			lines = append(lines, fmt.Sprintf("<%s|Runbook>", u))
		}
	}
	return strings.Join(lines, "\n")
}

// --- Helpers ---

// queryDuration finds the last bucket matching the OK condition in the past 7 days
// and returns a human-readable duration and start timestamp.
func (a *API) queryDuration(ctx context.Context, table, pkCol, pkVal, extraCond string) (duration, startedAt string) {
	db := a.DB
	if db == nil {
		return "-", "-"
	}
	var lastOK time.Time
	row := db.QueryRow(ctx, fmt.Sprintf(`
		SELECT max(bucket_ts)
		FROM %s FINAL
		WHERE %s = ?
		  AND bucket_ts >= now() - INTERVAL 7 DAY
		  %s`, table, pkCol, extraCond), pkVal)
	if err := row.Scan(&lastOK); err != nil || lastOK.IsZero() {
		return ">7d", time.Now().Add(-7 * 24 * time.Hour).UTC().Format("Jan 02 15:04 UTC")
	}
	return fmtDuration(time.Since(lastOK)), lastOK.UTC().Format("Jan 02 15:04 UTC")
}

// footerLine builds the Dashboard · Runbook footer.
func footerLine(alert grafanaAlert, dashboardURL string) string {
	parts := []string{fmt.Sprintf("<%s|Dashboard>", dashboardURL)}
	if u := alert.Annotations["runbook_url"]; u != "" {
		parts = append(parts, fmt.Sprintf("<%s|Runbook>", u))
	}
	return strings.Join(parts, " · ")
}

func joinLines(lines ...string) string {
	return strings.Join(lines, "\n")
}

func fmtDuration(d time.Duration) string {
	s := int(math.Round(d.Seconds()))
	days := s / 86400
	hours := (s % 86400) / 3600
	minutes := (s % 3600) / 60
	if days > 0 {
		return fmt.Sprintf("%dd %dh", days, hours)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	return fmt.Sprintf("%dm", minutes)
}
