package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseRepairSince(t *testing.T) {
	got, err := parseRepairSince("")
	require.NoError(t, err)
	require.True(t, got.IsZero(), "unset means no repair")

	got, err = parseRepairSince("2026-09-14")
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC), got)

	got, err = parseRepairSince("2026-09-14T06:00:00-03:00")
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC), got)

	_, err = parseRepairSince("14/09/2026")
	require.EqualError(t, err, `invalid PCAP_WAREHOUSE_REPAIR_SINCE "14/09/2026": want YYYY-MM-DD or RFC 3339`)
}
