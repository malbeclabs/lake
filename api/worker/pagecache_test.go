package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEnvDuration(t *testing.T) {
	require.Equal(t, 30*time.Second, envDuration("PC_TEST_DUR_UNSET", 30*time.Second), "unset → default")

	t.Setenv("PC_TEST_DUR", "90s")
	require.Equal(t, 90*time.Second, envDuration("PC_TEST_DUR", 30*time.Second), "valid → parsed")

	t.Setenv("PC_TEST_DUR_BAD", "not-a-duration")
	require.Equal(t, 30*time.Second, envDuration("PC_TEST_DUR_BAD", 30*time.Second), "invalid → default")
}

func TestEnvInt(t *testing.T) {
	require.Equal(t, 8, envInt("PC_TEST_INT_UNSET", 8), "unset → default")

	t.Setenv("PC_TEST_INT", "3")
	require.Equal(t, 3, envInt("PC_TEST_INT", 8), "valid → parsed")

	t.Setenv("PC_TEST_INT_BAD", "x")
	require.Equal(t, 8, envInt("PC_TEST_INT_BAD", 8), "invalid → default")
}
