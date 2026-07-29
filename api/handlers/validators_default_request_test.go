package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestValidatorsSortFieldsKeyParity pins that the query-column sort map stays
// key-synced with the request-facing allowlist. If they drift, a valid sort_by
// passes ParseSort but OrderByClause returns "" and silently drops the ORDER BY.
func TestValidatorsSortFieldsKeyParity(t *testing.T) {
	t.Parallel()
	if len(validatorsQuerySortFields) != len(validatorSortFields) {
		t.Fatalf("key count mismatch: query=%d allowlist=%d", len(validatorsQuerySortFields), len(validatorSortFields))
	}
	for k := range validatorSortFields {
		if _, ok := validatorsQuerySortFields[k]; !ok {
			t.Errorf("sort key %q in validatorSortFields is missing from validatorsQuerySortFields", k)
		}
	}
}

// TestIsDefaultValidatorsRequest pins that both the bare request and the exact
// polled query string are detected as the cacheable default shape, while any
// filter, non-default page, or sort bypasses the cache.
func TestIsDefaultValidatorsRequest(t *testing.T) {
	t.Parallel()

	// DefaultLimit (not 100) so this stays in lockstep with GetValidators' parse
	// call — a changed handler default must fail here, not silently stop caching.
	parse := func(rawQuery string) (PaginationParams, SortParams, MultiFilterParams) {
		r := httptest.NewRequest(http.MethodGet, "/api/solana/validators?"+rawQuery, nil)
		return ParsePagination(r, DefaultLimit), ParseSort(r, "stake", validatorSortFields), ParseFilters(r)
	}

	tests := []struct {
		name     string
		query    string
		wantTrue bool
	}{
		{"bare request", "", true},
		{"explicit default (polled shape)", "limit=100&offset=0&sort_by=stake&sort_dir=desc", true},
		{"second page", "limit=100&offset=100&sort_by=stake&sort_dir=desc", false},
		{"non-default limit", "limit=50", false},
		{"ascending sort", "sort_by=stake&sort_dir=asc", false},
		{"different sort field", "sort_by=commission&sort_dir=desc", false},
		{"with filter", "filters=dz:yes", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, s, f := parse(tc.query)
			if got := isDefaultValidatorsRequest(p, s, f); got != tc.wantTrue {
				t.Fatalf("isDefaultValidatorsRequest(%q) = %v, want %v", tc.query, got, tc.wantTrue)
			}
		})
	}
}
