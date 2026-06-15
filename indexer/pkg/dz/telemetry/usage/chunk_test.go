package dztelemusage

import (
	"context"
	"errors"
	"testing"
	"time"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/require"
)

func TestLake_TelemetryUsage_QueryIntfCountersChunked_SplitsWindow(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)
	end := start.Add(1 * time.Hour)
	chunk := 5 * time.Minute

	var windows [][2]time.Time
	mock := &mockInfluxDBClient{
		queryIntfCountersFunc: func(_ context.Context, s, e time.Time) ([]map[string]any, error) {
			windows = append(windows, [2]time.Time{s, e})
			return []map[string]any{{"intf": "eth0", "time": s.Format(time.RFC3339Nano)}}, nil
		},
	}

	rows, err := queryIntfCountersChunked(context.Background(), mock, start, end, chunk)
	require.NoError(t, err)

	// 1 hour split into 5-minute chunks => 12 bounded sub-queries.
	require.Len(t, windows, 12, "expected one sub-query per chunk")
	// Rows from every chunk are concatenated.
	require.Len(t, rows, 12)

	// No sub-query exceeds the chunk size — this is what bounds InfluxDB heap.
	for _, w := range windows {
		require.LessOrEqualf(t, w[1].Sub(w[0]), chunk, "sub-query window %v..%v exceeds chunk %v", w[0], w[1], chunk)
	}

	// Sub-windows are contiguous and exactly cover [start, end).
	require.Equal(t, start, windows[0][0])
	require.Equal(t, end, windows[len(windows)-1][1])
	for i := 1; i < len(windows); i++ {
		require.Equalf(t, windows[i-1][1], windows[i][0], "gap/overlap between chunk %d and %d", i-1, i)
	}
}

func TestLake_TelemetryUsage_QueryIntfCountersChunked_RemainderWindow(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)
	end := start.Add(12 * time.Minute)
	chunk := 5 * time.Minute

	var windows [][2]time.Time
	mock := &mockInfluxDBClient{
		queryIntfCountersFunc: func(_ context.Context, s, e time.Time) ([]map[string]any, error) {
			windows = append(windows, [2]time.Time{s, e})
			return nil, nil
		},
	}

	_, err := queryIntfCountersChunked(context.Background(), mock, start, end, chunk)
	require.NoError(t, err)

	// 12 minutes => [0,5), [5,10), [10,12): three chunks, last is the 2-minute remainder.
	require.Len(t, windows, 3)
	require.Equal(t, end, windows[len(windows)-1][1], "final chunk must clamp to end, never overshoot")
	require.Equal(t, 2*time.Minute, windows[2][1].Sub(windows[2][0]))
}

func TestLake_TelemetryUsage_QueryIntfCountersChunked_PropagatesError(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)
	end := start.Add(1 * time.Hour)
	wantErr := errors.New("request too large: Heap exhausted")

	calls := 0
	mock := &mockInfluxDBClient{
		queryIntfCountersFunc: func(_ context.Context, _, _ time.Time) ([]map[string]any, error) {
			calls++
			if calls == 2 {
				return nil, wantErr
			}
			return nil, nil
		},
	}

	_, err := queryIntfCountersChunked(context.Background(), mock, start, end, 5*time.Minute)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 2, calls, "must stop at the first failing chunk")
}

// Backfill takes arbitrary, potentially multi-day ranges — the case most likely
// to trigger the InfluxDB heap exhaustion this chunking guards against. It must
// split its range into bounded sub-queries just like the steady-state refresh.
func TestLake_TelemetryUsage_BackfillForTimeRange_ChunksQuery(t *testing.T) {
	t.Parallel()

	var windows [][2]time.Time
	influx := &mockInfluxDBClient{
		queryIntfCountersFunc: func(_ context.Context, s, e time.Time) ([]map[string]any, error) {
			windows = append(windows, [2]time.Time{s, e})
			return nil, nil
		},
	}

	view, err := NewView(ViewConfig{
		Logger:          laketesting.NewLogger(),
		ClickHouse:      testClient(t),
		InfluxDB:        influx,
		Bucket:          "test-bucket",
		RefreshInterval: time.Second,
		QueryWindow:     time.Hour,
		QueryChunk:      5 * time.Minute,
	})
	require.NoError(t, err)

	start := time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)
	end := start.Add(1 * time.Hour)
	_, err = view.BackfillForTimeRange(context.Background(), start, end)
	require.NoError(t, err)

	require.Len(t, windows, 12, "backfill must split its range into bounded chunks")
	for _, w := range windows {
		require.LessOrEqualf(t, w[1].Sub(w[0]), 5*time.Minute, "backfill sub-query %v..%v exceeds chunk", w[0], w[1])
	}
}
