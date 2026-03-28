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

	"github.com/malbeclabs/lake/api/config"
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

// Enrichment data fetched from ClickHouse.

type linkEnrichment struct {
	ALossPct   float64
	ZLossPct   float64
	MaxLossPct float64
	IsisDown   bool
	Duration   string
	StartedAt  string
}

// HandleGrafanaAlerts receives a Grafana webhook, enriches alerts with
// live ClickHouse data, and posts a formatted message to Slack.
//
// Query parameters:
//   - channel: Slack channel ID to post to (required)
func HandleGrafanaAlerts(w http.ResponseWriter, r *http.Request) {
	var payload grafanaWebhook
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		slog.Error("grafana webhook: failed to parse", "error", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	channelID := r.URL.Query().Get("channel")
	if channelID == "" {
		slog.Error("grafana webhook: missing channel parameter")
		http.Error(w, "missing channel parameter", http.StatusBadRequest)
		return
	}

	start := time.Now()
	alertName := payload.GroupLabels["alertname"]

	if err := postEnrichedAlerts(r.Context(), payload, channelID); err != nil {
		slog.Error("grafana webhook: failed to post", "error", err, "channel", channelID)
		metrics.GrafanaWebhookTotal.WithLabelValues("error", alertName).Inc()
		metrics.GrafanaWebhookDuration.Observe(time.Since(start).Seconds())
		http.Error(w, "failed to post to slack", http.StatusInternalServerError)
		return
	}

	metrics.GrafanaWebhookTotal.WithLabelValues("success", alertName).Inc()
	metrics.GrafanaWebhookDuration.Observe(time.Since(start).Seconds())
	w.WriteHeader(http.StatusOK)
}

func postEnrichedAlerts(ctx context.Context, payload grafanaWebhook, channelID string) error {
	botToken := os.Getenv("GRAFANA_SLACK_BOT_TOKEN")
	if botToken == "" {
		return fmt.Errorf("GRAFANA_SLACK_BOT_TOKEN not configured")
	}
	api := slack.New(botToken)

	// Build a message section for each alert in the group.
	var sections []string
	for _, alert := range payload.Alerts {
		sections = append(sections, enrichAndFormat(ctx, alert))
	}

	color := "#E01E5A" // red for firing
	if payload.Status == "resolved" {
		color = "#2EB67D" // green for resolved
	}

	attachment := slack.Attachment{
		Color:    color,
		Title:    payload.Title,
		Text:     strings.Join(sections, "\n\n"),
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
func enrichAndFormat(ctx context.Context, alert grafanaAlert) string {
	name := alert.Labels["alertname"]
	switch {
	case strings.Contains(name, "Link Down"), strings.Contains(name, "Link Degraded"):
		return formatLinkAlert(ctx, alert)
	default:
		return formatGenericAlert(alert)
	}
}

// --- Link Down / Degraded ---

func formatLinkAlert(ctx context.Context, alert grafanaAlert) string {
	l := alert.Labels
	linkPK := l["link_pk"]
	linkPKShort := l["link_pk_short"]
	contributor := l["contributor_code"]
	linkType := l["link_type"]
	bandwidth := l["bandwidth"]
	metro := l["metro"]

	e := enrichLink(ctx, linkPK, alert.Labels["alertname"])

	linkURL := fmt.Sprintf("https://data.malbeclabs.com/dz/links/%s", linkPK)

	isis := "IS-IS UP"
	if e.IsisDown {
		isis = "IS-IS DOWN"
	}

	summary := fmt.Sprintf("*Link <%s|%s> · %s · %s ago*", linkURL, linkPKShort, e.StartedAt, e.Duration)
	desc := fmt.Sprintf("%s · %s · %s · %s · %s", contributor, linkType, bandwidth, metro, isis)
	loss := fmt.Sprintf("Loss: %.1f%% A→Z / %.1f%% Z→A (max %.1f%%)", e.ALossPct, e.ZLossPct, e.MaxLossPct)

	var footerParts []string
	footerParts = append(footerParts, "<https://data.malbeclabs.com/status/links|Dashboard>")
	if runbook := alert.Annotations["runbook_url"]; runbook != "" {
		footerParts = append(footerParts, fmt.Sprintf("<%s|Runbook>", runbook))
	}

	return strings.Join([]string{summary, desc, loss, strings.Join(footerParts, " · ")}, "\n")
}

func enrichLink(ctx context.Context, linkPK, alertName string) linkEnrichment {
	db := config.DB
	if db == nil {
		return linkEnrichment{Duration: "-", StartedAt: "-"}
	}

	var e linkEnrichment

	// Current rollup values.
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
		return linkEnrichment{Duration: "-", StartedAt: "-"}
	}

	// Duration — find last healthy bucket in the past 7 days.
	okCond := "NOT (greatest(a_loss_pct, z_loss_pct) > 10 OR isis_down)" // link down
	if strings.Contains(alertName, "Degraded") {
		okCond = "greatest(a_loss_pct, z_loss_pct) <= 1" // link degraded
	}

	var lastOK time.Time
	row = db.QueryRow(ctx, fmt.Sprintf(`
		SELECT max(bucket_ts)
		FROM lake.link_rollup_5m FINAL
		WHERE link_pk = ?
		  AND bucket_ts >= now() - INTERVAL 7 DAY
		  AND NOT provisioning
		  AND %s`, okCond), linkPK)
	if err := row.Scan(&lastOK); err != nil || lastOK.IsZero() {
		e.Duration = ">7d"
		e.StartedAt = time.Now().Add(-7 * 24 * time.Hour).UTC().Format("Jan 02 15:04 UTC")
	} else {
		e.Duration = fmtDuration(time.Since(lastOK))
		e.StartedAt = lastOK.UTC().Format("Jan 02 15:04 UTC")
	}

	return e
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
		// Only add if not already referenced in description.
		if !strings.Contains(strings.Join(lines, ""), "Runbook") {
			lines = append(lines, fmt.Sprintf("<%s|Runbook>", u))
		}
	}
	return strings.Join(lines, "\n")
}

// --- Helpers ---

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
