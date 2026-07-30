package handlers

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var maxExecutionTimeRe = regexp.MustCompile(`max_execution_time = (\d+)`)

// A multicast-delivery entity query must lose the deadline race against the
// handler's own budget. When the client deadline fires first the driver returns
// context.DeadlineExceeded, which names no query and which the request path
// treats as a disconnect — so an overrun used to return a 500 with no log line
// at all. With a lower SQL cap ClickHouse fails first with TIMEOUT_EXCEEDED.
func TestMulticastDeliveryQueryTimeoutLosesToRequestDeadline(t *testing.T) {
	t.Parallel()

	queryTimeout := time.Duration(multicastDeliveryQueryTimeoutSeconds) * time.Second
	require.Less(t, queryTimeout, multicastDeliveryRequestTimeout,
		"query cap must expire before the request deadline")
	require.Contains(t, multicastDeliveryQuerySettings,
		"max_execution_time = "+strconv.Itoa(multicastDeliveryQueryTimeoutSeconds))

	// Catch a literal cap that skipped the templated clause. Matching nothing is
	// a pass: it means every cap is built from the constants above.
	files, err := filepath.Glob("multicast_delivery_entity*.go")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, file := range files {
		src, err := os.ReadFile(file)
		require.NoError(t, err)
		for _, match := range maxExecutionTimeRe.FindAllStringSubmatch(string(src), -1) {
			seconds, err := strconv.Atoi(match[1])
			require.NoError(t, err)
			require.Less(t, time.Duration(seconds)*time.Second, multicastDeliveryRequestTimeout,
				"%s: max_execution_time = %d does not expire before the request deadline", file, seconds)
		}
	}
}

// A count fallback runs second within its fan-out branch, so it gets what is
// left of the request deadline instead of the full per-query cap — two queries
// at the full cap would together outlast the handler's own budget.
func TestMulticastDeliveryFallbackQuerySettingsUseRemainingDeadline(t *testing.T) {
	t.Parallel()

	require.Equal(t, multicastDeliveryQuerySettings,
		multicastDeliveryFallbackQuerySettings(context.Background()),
		"no deadline: full per-query cap")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.Contains(t, multicastDeliveryFallbackQuerySettings(ctx), "max_execution_time = 4")

	expired, cancelExpired := context.WithTimeout(context.Background(), -time.Second)
	defer cancelExpired()
	require.Contains(t, multicastDeliveryFallbackQuerySettings(expired), "max_execution_time = 1",
		"an expired deadline still yields a valid cap")
}
