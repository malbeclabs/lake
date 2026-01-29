package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueryResult_QueryText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		result   QueryResult
		expected string
	}{
		{
			name:     "sql query",
			result:   QueryResult{SQL: "SELECT 1"},
			expected: "SELECT 1",
		},
		{
			name:     "cypher query",
			result:   QueryResult{Cypher: "MATCH (n) RETURN n"},
			expected: "MATCH (n) RETURN n",
		},
		{
			name:     "cypher takes precedence over sql",
			result:   QueryResult{SQL: "SELECT 1", Cypher: "MATCH (n) RETURN n"},
			expected: "MATCH (n) RETURN n",
		},
		{
			name:     "empty returns empty",
			result:   QueryResult{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.expected, tt.result.QueryText())
		})
	}
}

func TestGeneratedQuery_QueryText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		query    GeneratedQuery
		expected string
	}{
		{
			name:     "sql query",
			query:    GeneratedQuery{SQL: "SELECT 1"},
			expected: "SELECT 1",
		},
		{
			name:     "cypher query",
			query:    GeneratedQuery{Cypher: "MATCH (n) RETURN n"},
			expected: "MATCH (n) RETURN n",
		},
		{
			name:     "cypher takes precedence",
			query:    GeneratedQuery{SQL: "SELECT 1", Cypher: "MATCH (n) RETURN n"},
			expected: "MATCH (n) RETURN n",
		},
		{
			name:     "empty returns empty",
			query:    GeneratedQuery{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.expected, tt.query.QueryText())
		})
	}
}

func TestGeneratedQuery_IsCypher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		query    GeneratedQuery
		expected bool
	}{
		{
			name:     "sql query",
			query:    GeneratedQuery{SQL: "SELECT 1"},
			expected: false,
		},
		{
			name:     "cypher query",
			query:    GeneratedQuery{Cypher: "MATCH (n) RETURN n"},
			expected: true,
		},
		{
			name:     "empty is not cypher",
			query:    GeneratedQuery{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.expected, tt.query.IsCypher())
		})
	}
}
