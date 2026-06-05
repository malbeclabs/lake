package handlers

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenizeHealthSearch(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []healthSearchToken
	}{
		{
			name: "empty",
			in:   "",
			want: []healthSearchToken{},
		},
		{
			name: "single bare term",
			in:   "3b2Ze7VY",
			want: []healthSearchToken{{value: "3b2Ze7VY"}},
		},
		{
			name: "single field token",
			in:   "device:nyc001",
			want: []healthSearchToken{{field: "device", value: "nyc001"}},
		},
		{
			name: "mixed bare + field",
			in:   "unhealthy device:nyc001",
			want: []healthSearchToken{
				{value: "unhealthy"},
				{field: "device", value: "nyc001"},
			},
		},
		{
			name: "field prefix is lowercased",
			in:   "DEVICE:NYC001",
			want: []healthSearchToken{{field: "device", value: "NYC001"}},
		},
		{
			name: "trailing colon with no value falls back to bare",
			in:   "device:",
			want: []healthSearchToken{{value: "device"}},
		},
		{
			name: "leading colon is not a field token",
			in:   ":foo",
			want: []healthSearchToken{{value: ":foo"}},
		},
		{
			name: "multiple field tokens AND together",
			in:   "device:nyc001 status:unhealthy mode:P",
			want: []healthSearchToken{
				{field: "device", value: "nyc001"},
				{field: "status", value: "unhealthy"},
				{field: "mode", value: "P"},
			},
		},
		{
			name: "extra whitespace ignored",
			in:   "  device:nyc001     status:unhealthy  ",
			want: []healthSearchToken{
				{field: "device", value: "nyc001"},
				{field: "status", value: "unhealthy"},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tokenizeHealthSearch(tc.in)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestBuildHealthSearchClause(t *testing.T) {
	fieldMap := map[string][]string{
		"device": {"user_device_code", "user_device_pk"},
		"status": {"health_status"},
	}
	fallback := []string{"user_pk", "user_owner_pubkey"}

	t.Run("empty search → no clause", func(t *testing.T) {
		clause, args := buildHealthSearchClause("", fieldMap, fallback)
		assert.Equal(t, "", clause)
		assert.Nil(t, args)
	})

	t.Run("bare term → fallback columns OR'd", func(t *testing.T) {
		clause, args := buildHealthSearchClause("3b2Ze7VY", fieldMap, fallback)
		// Expect: AND (pos(user_pk, ?)>0 OR pos(user_owner_pubkey, ?)>0)
		assert.Contains(t, clause, "user_pk")
		assert.Contains(t, clause, "user_owner_pubkey")
		assert.Equal(t, strings.Count(clause, " OR "), 1)
		require.Len(t, args, 2)
		assert.Equal(t, "3b2Ze7VY", args[0])
		assert.Equal(t, "3b2Ze7VY", args[1])
	})

	t.Run("known field → mapped columns OR'd", func(t *testing.T) {
		clause, args := buildHealthSearchClause("device:nyc001", fieldMap, fallback)
		assert.Contains(t, clause, "user_device_code")
		assert.Contains(t, clause, "user_device_pk")
		assert.NotContains(t, clause, "user_pk,")
		require.Len(t, args, 2)
		assert.Equal(t, "nyc001", args[0])
	})

	t.Run("unknown field falls back to bare", func(t *testing.T) {
		clause, args := buildHealthSearchClause("nonsense:foo", fieldMap, fallback)
		// "nonsense" not in fieldMap → treated as substring match against fallback
		assert.Contains(t, clause, "user_pk")
		assert.Contains(t, clause, "user_owner_pubkey")
		require.Len(t, args, 2)
		assert.Equal(t, "foo", args[0])
	})

	t.Run("multiple tokens AND together", func(t *testing.T) {
		clause, args := buildHealthSearchClause("device:nyc001 status:unhealthy", fieldMap, fallback)
		// Two ANDs: the leading " AND " that attaches to caller's WHERE, plus
		// one between the two token groups.
		assert.Equal(t, 2, strings.Count(clause, " AND "))
		assert.Contains(t, clause, "user_device_code")
		assert.Contains(t, clause, "health_status")
		require.Len(t, args, 3) // 2 device cols + 1 status col
	})

	t.Run("placeholders survive into args in token order", func(t *testing.T) {
		_, args := buildHealthSearchClause("device:nyc status:unhealthy", fieldMap, fallback)
		// device token first (2 cols, 2 args), status token second (1 col, 1 arg)
		require.Len(t, args, 3)
		assert.Equal(t, "nyc", args[0])
		assert.Equal(t, "nyc", args[1])
		assert.Equal(t, "unhealthy", args[2])
	})
}
