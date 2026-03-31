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

// record writes a single ingestion run to ClickHouse. Errors are logged but
// never returned — ingestion logs must not interfere with data ingestion.
func (w *Writer) record(rec runRecord) {
	if w == nil {
		return
	}

	query := `INSERT INTO log_ingestion_runs
		(run_id, workflow, activity, network, status, started_at, finished_at, duration_ms, rows_affected, error_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

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
	RunID        uuid.UUID
	Workflow     string
	Activity     string
	Network      string
	Status       string
	StartedAt    time.Time
	FinishedAt   time.Time
	DurationMs   uint64
	RowsAffected *int64
	ErrorMessage *string
}

func buildRecord(workflow, activity, network string, start time.Time, rowsAffected *int64, err error) runRecord {
	now := time.Now()
	rec := runRecord{
		RunID:        uuid.New(),
		Workflow:     workflow,
		Activity:     activity,
		Network:      network,
		StartedAt:    start,
		FinishedAt:   now,
		DurationMs:   uint64(now.Sub(start).Milliseconds()),
		RowsAffected: rowsAffected,
	}
	if err != nil {
		rec.Status = "error"
		msg := err.Error()
		rec.ErrorMessage = &msg
	} else {
		rec.Status = "success"
	}
	return rec
}

// Wrap executes fn and records the result as an ingestion log entry.
// If w is nil, fn is called directly without recording.
func (w *Writer) Wrap(ctx context.Context, workflow, activity, network string, fn func() error) error {
	if w == nil {
		return fn()
	}
	start := time.Now()
	err := fn()
	w.record(buildRecord(workflow, activity, network, start, nil, err))
	return err
}

// WrapWithCount executes fn (which returns a count) and records the result.
// The count is stored as rows_affected. If w is nil, fn is called directly.
func (w *Writer) WrapWithCount(ctx context.Context, workflow, activity, network string, fn func() (int, error)) (int, error) {
	if w == nil {
		return fn()
	}
	start := time.Now()
	count, err := fn()
	rows := int64(count)
	w.record(buildRecord(workflow, activity, network, start, &rows, err))
	return count, err
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
