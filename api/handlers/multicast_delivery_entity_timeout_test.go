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
	require.Contains(t, multicastDeliveryQuerySettings(context.Background()),
		"max_execution_time = "+strconv.Itoa(multicastDeliveryQueryTimeoutSeconds),
		"no deadline in play: full per-query cap")

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

// ClickHouse counts max_execution_time from when the query starts executing, so
// the cap has to come off the time actually left on the request: a query that
// queued on the fan-out semaphore, or that runs second within its branch, would
// otherwise get a full fresh budget and win the race the cap exists to lose.
func TestMulticastDeliveryQuerySettingsUseRemainingDeadline(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.Contains(t, multicastDeliveryQuerySettings(ctx), "max_execution_time = 4",
		"less time left than the per-query cap: cap to the remainder, minus a second of margin")

	expired, cancelExpired := context.WithTimeout(context.Background(), -time.Second)
	defer cancelExpired()
	require.Contains(t, multicastDeliveryQuerySettings(expired), "max_execution_time = 1",
		"an expired deadline still yields a valid cap")
}
