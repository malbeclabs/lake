// Package ingestionlog writes structured ingestion run logs to ClickHouse.
// It records when each Temporal activity starts, finishes, and whether it
// succeeded or failed — giving queryable visibility into data freshness,
// latency, and errors.
package ingestionlog

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/malbeclabs/lake/utils/pkg/redact"
)

// Inserter is the subset of ClickHouse connection methods needed to write
// ingestion logs. Satisfied by both clickhouse.Connection and driver.Conn.
type Inserter interface {
	AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error
}

// Writer writes ingestion run records to ClickHouse. Safe to use from
// multiple goroutines. A nil *Writer is safe to call — all methods are no-ops.
type Writer struct {
	inserter Inserter
	log      *slog.Logger
}

// NewWriter creates a Writer that logs ingestion runs to ClickHouse.
func NewWriter(inserter Inserter, log *slog.Logger) *Writer {
	return &Writer{inserter: inserter, log: log}
}

// RefreshResult holds metadata about a refresh/ingestion operation.
// Views populate whichever fields are meaningful for their data source.
type RefreshResult struct {
	// RowsAffected is the number of rows written to ClickHouse.
	RowsAffected int64

	// SourceMinEventTS is the earliest source timestamp in the ingested batch.
	// Nil when not applicable (e.g. snapshot-based views with no source timestamps).
	SourceMinEventTS *time.Time

	// SourceMaxEventTS is the latest source timestamp in the ingested batch.
	// For snapshot-based views, this is typically the fetchedAt time.
	SourceMaxEventTS *time.Time

	// Partial marks a run that completed without error but knowingly left work
	// undone — a drain that banked its committed chunks and stopped, expecting the
	// next cycle to continue. Such a run is recorded as status="partial" rather than
	// "success".
	//
	// The distinction is load-bearing for staleness alerting, which asks for the last
	// successful run's finished_at. A partial cycle that reported "success" reset that
	// clock while the data it covers stayed hours behind, so a drain that never
	// converged looked healthy indefinitely. Reporting the honest source timestamp is
	// not sufficient on its own: nothing currently reads source_max_event_ts.
	Partial bool
}

// record writes a single ingestion run to ClickHouse. Errors are logged but
// never returned — ingestion logs must not interfere with data ingestion.
func (w *Writer) record(rec runRecord) {
	if w == nil {
		return
	}

	query := `INSERT INTO log_ingestion_runs
		(run_id, workflow, activity, network, status, started_at, finished_at, duration_ms,
		 rows_affected, error_message, source_min_event_ts, source_max_event_ts)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	// Use a detached context so the insert isn't cancelled if the activity
	// context is done (e.g. during shutdown).
	insertCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.inserter.AsyncInsert(insertCtx, query, false,
		rec.RunID,
		rec.Workflow,
		rec.Activity,
		rec.Network,
		rec.Status,
		rec.StartedAt,
		rec.FinishedAt,
		rec.DurationMs,
		rec.RowsAffected,
		rec.ErrorMessage,
		rec.SourceMinEventTS,
		rec.SourceMaxEventTS,
	); err != nil {
		w.log.Warn("ingestionlog: failed to write run record",
			"workflow", rec.Workflow,
			"activity", rec.Activity,
			"error", err,
		)
	}
}

// runRecord is the internal representation of an ingestion run log entry.
type runRecord struct {
	RunID            uuid.UUID
	Workflow         string
	Activity         string
	Network          string
	Status           string
	StartedAt        time.Time
	FinishedAt       time.Time
	DurationMs       uint64
	RowsAffected     *int64
	ErrorMessage     *string
	SourceMinEventTS *time.Time
	SourceMaxEventTS *time.Time
}

func buildRecord(workflow, activity, network string, start time.Time, result RefreshResult, err error) runRecord {
	now := time.Now()
	rec := runRecord{
		RunID:            uuid.New(),
		Workflow:         workflow,
		Activity:         activity,
		Network:          network,
		StartedAt:        start,
		FinishedAt:       now,
		DurationMs:       uint64(now.Sub(start).Milliseconds()),
		SourceMinEventTS: result.SourceMinEventTS,
		SourceMaxEventTS: result.SourceMaxEventTS,
	}
	if result.RowsAffected > 0 {
		rec.RowsAffected = &result.RowsAffected
	}
	switch {
	case err != nil:
		rec.Status = "error"
		// hide endpoint url from caller since solana-go puts the the whole string in the error message
		msg := redact.Error(err)
		rec.ErrorMessage = &msg
	case result.Partial:
		rec.Status = "partial"
	default:
		rec.Status = "success"
	}
	return rec
}

// Wrap executes fn and records the result as an ingestion log entry.
// If w is nil, fn is called directly without recording.
func (w *Writer) Wrap(ctx context.Context, workflow, activity, network string, fn func() (RefreshResult, error)) error {
	if w == nil {
		_, err := fn()
		return err
	}
	start := time.Now()
	result, err := fn()
	w.record(buildRecord(workflow, activity, network, start, result, err))
	return err
}

// WrapSkipped records a skipped activity (dependency not configured).
// If w is nil, this is a no-op.
func (w *Writer) WrapSkipped(ctx context.Context, workflow, activity, network string) {
	if w == nil {
		return
	}
	now := time.Now()
	w.record(runRecord{
		RunID:      uuid.New(),
		Workflow:   workflow,
		Activity:   activity,
		Network:    network,
		Status:     "skipped",
		StartedAt:  now,
		FinishedAt: now,
		DurationMs: 0,
	})
}
