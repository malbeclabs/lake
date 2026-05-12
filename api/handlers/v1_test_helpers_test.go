package handlers_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func assertJSONKeys(t *testing.T, obj map[string]any, want []string, label string) {
	t.Helper()
	got := make([]string, 0, len(obj))
	for k := range obj {
		got = append(got, k)
	}
	assert.ElementsMatch(t, want, got, "%s: public JSON keys must match the v1 contract exactly (renames/removals require a new API version)", label)
}
