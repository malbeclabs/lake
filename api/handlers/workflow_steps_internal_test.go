package handlers

import (
	"testing"

	"github.com/malbeclabs/lake/agent/pkg/workflow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToClientFormat(t *testing.T) {
	t.Parallel()

	t.Run("maps Count to Rows and Rows to Data", func(t *testing.T) {
		t.Parallel()
		step := WorkflowStep{
			ID:       "step-1",
			Type:     "sql_query",
			Question: "how many?",
			SQL:      "SELECT count() FROM t",
			Status:   "completed",
			Columns:  []string{"count"},
			Rows:     [][]any{{42}},
			Count:    1,
			Env:      "devnet",
		}

		client := step.toClientFormat()

		assert.Equal(t, "step-1", client.ID)
		assert.Equal(t, "sql_query", client.Type)
		assert.Equal(t, "how many?", client.Question)
		assert.Equal(t, "SELECT count() FROM t", client.SQL)
		assert.Equal(t, "completed", client.Status)
		assert.Equal(t, 1, client.Rows)             // Count → Rows
		assert.Equal(t, [][]any{{42}}, client.Data) // Rows → Data
		assert.Equal(t, []string{"count"}, client.Columns)
		assert.Equal(t, "devnet", client.Env)
	})

	t.Run("preserves cypher fields", func(t *testing.T) {
		t.Parallel()
		step := WorkflowStep{
			ID:     "step-2",
			Type:   "cypher_query",
			Cypher: "MATCH (n) RETURN n",
			Nodes:  []any{"node1"},
			Edges:  []any{"edge1"},
			Env:    "mainnet-beta",
		}

		client := step.toClientFormat()

		assert.Equal(t, "MATCH (n) RETURN n", client.Cypher)
		assert.Equal(t, []any{"node1"}, client.Nodes)
		assert.Equal(t, []any{"edge1"}, client.Edges)
		assert.Equal(t, "mainnet-beta", client.Env)
	})

	t.Run("preserves thinking step", func(t *testing.T) {
		t.Parallel()
		step := WorkflowStep{
			ID:      "step-3",
			Type:    "thinking",
			Content: "Let me think...",
		}

		client := step.toClientFormat()

		assert.Equal(t, "thinking", client.Type)
		assert.Equal(t, "Let me think...", client.Content)
	})
}

func TestBuildFinalSteps(t *testing.T) {
	t.Parallel()

	t.Run("enriches sql_query with row data from result", func(t *testing.T) {
		t.Parallel()
		steps := []WorkflowStep{
			{
				ID:       "s1",
				Type:     "sql_query",
				Question: "count devices",
				SQL:      "SELECT count() FROM devices",
				Status:   "completed",
				Count:    1,
				Env:      "devnet",
			},
		}
		result := &workflow.WorkflowResult{
			ExecutedQueries: []workflow.ExecutedQuery{
				{
					GeneratedQuery: workflow.GeneratedQuery{
						DataQuestion: workflow.DataQuestion{Question: "count devices"},
						SQL:          "SELECT count() FROM devices",
					},
					Result: workflow.QueryResult{
						SQL:     "SELECT count() FROM devices",
						Columns: []string{"count()"},
						Rows:    []map[string]any{{"count()": 42}},
						Count:   1,
					},
				},
			},
		}

		final := buildFinalSteps(steps, result)

		require.Len(t, final, 1)
		assert.Equal(t, "s1", final[0].ID)
		assert.Equal(t, []string{"count()"}, final[0].Columns)
		assert.Equal(t, [][]any{{42}}, final[0].Rows)
		assert.Equal(t, 1, final[0].Count)
		assert.Equal(t, "devnet", final[0].Env)
	})

	t.Run("enriches cypher_query with row data", func(t *testing.T) {
		t.Parallel()
		steps := []WorkflowStep{
			{
				ID:     "s2",
				Type:   "cypher_query",
				Cypher: "MATCH (n) RETURN n.name",
				Status: "completed",
				Env:    "mainnet-beta",
			},
		}
		result := &workflow.WorkflowResult{
			ExecutedQueries: []workflow.ExecutedQuery{
				{
					GeneratedQuery: workflow.GeneratedQuery{
						DataQuestion: workflow.DataQuestion{Question: "get names"},
						Cypher:       "MATCH (n) RETURN n.name",
					},
					Result: workflow.QueryResult{
						Cypher:  "MATCH (n) RETURN n.name",
						Columns: []string{"n.name"},
						Rows:    []map[string]any{{"n.name": "alice"}},
						Count:   1,
					},
				},
			},
		}

		final := buildFinalSteps(steps, result)

		require.Len(t, final, 1)
		assert.Equal(t, []string{"n.name"}, final[0].Columns)
		assert.Equal(t, [][]any{{"alice"}}, final[0].Rows)
		assert.Equal(t, "mainnet-beta", final[0].Env)
	})

	t.Run("leaves thinking and read_docs steps unchanged", func(t *testing.T) {
		t.Parallel()
		steps := []WorkflowStep{
			{ID: "t1", Type: "thinking", Content: "reasoning"},
			{ID: "d1", Type: "read_docs", Page: "overview", Content: "doc content"},
		}
		result := &workflow.WorkflowResult{}

		final := buildFinalSteps(steps, result)

		require.Len(t, final, 2)
		assert.Equal(t, "thinking", final[0].Type)
		assert.Equal(t, "reasoning", final[0].Content)
		assert.Equal(t, "read_docs", final[1].Type)
		assert.Equal(t, "overview", final[1].Page)
	})

	t.Run("preserves unmatched sql_query step as-is", func(t *testing.T) {
		t.Parallel()
		steps := []WorkflowStep{
			{ID: "s3", Type: "sql_query", SQL: "SELECT 1", Env: "testnet"},
		}
		result := &workflow.WorkflowResult{}

		final := buildFinalSteps(steps, result)

		require.Len(t, final, 1)
		assert.Equal(t, "SELECT 1", final[0].SQL)
		assert.Equal(t, "testnet", final[0].Env)
	})
}
