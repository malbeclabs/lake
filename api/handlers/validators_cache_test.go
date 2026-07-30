package handlers

import (
	"bytes"
	"encoding/json"
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

// TestIsCacheableValidatorsRequest pins that every unfiltered stake-desc shape is
// cache-eligible regardless of page size — notably the external poller's
// limit=900, which the old limit==DefaultLimit gate sent live on every call — while
// any filter or different sort still bypasses the cache.
func TestIsCacheableValidatorsRequest(t *testing.T) {
	t.Parallel()

	parse := func(rawQuery string) (SortParams, MultiFilterParams) {
		r := httptest.NewRequest(http.MethodGet, "/api/solana/validators?"+rawQuery, nil)
		return ParseSort(r, "stake", validatorSortFields), ParseFilters(r)
	}

	tests := []struct {
		name     string
		query    string
		wantTrue bool
	}{
		{"bare request", "", true},
		{"UI first page", "limit=100&offset=0&sort_by=stake&sort_dir=desc", true},
		{"polled shape", "limit=900&offset=0&sort_by=stake&sort_dir=desc", true},
		{"max limit", "limit=1000", true},
		{"deep page", "limit=100&offset=1200&sort_by=stake&sort_dir=desc", true},
		{"ascending sort", "sort_by=stake&sort_dir=asc", false},
		{"different sort field", "sort_by=commission&sort_dir=desc", false},
		{"with filter", "filters=dz:yes", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, f := parse(tc.query)
			if got := isCacheableValidatorsRequest(s, f); got != tc.wantTrue {
				t.Fatalf("isCacheableValidatorsRequest(%q) = %v, want %v", tc.query, got, tc.wantTrue)
			}
		})
	}
}

// TestValidatorsCacheHoldsMoreThanAnyRequest pins the invariant the whole design
// rests on: the cached entry must be able to outgrow the largest page a client can
// ask for. If the cap were ever lowered to a request-sized value (DefaultLimit, say),
// every entry would fail the completeness check, every request would go live, and
// the only signal would be a WARN — silently reverting this fix.
func TestValidatorsCacheHoldsMoreThanAnyRequest(t *testing.T) {
	t.Parallel()
	if validatorsCacheMaxRows <= MaxLimit {
		t.Fatalf("validatorsCacheMaxRows (%d) must exceed MaxLimit (%d)", validatorsCacheMaxRows, MaxLimit)
	}
}

// TestSliceCachedValidators covers the page slicing and, more importantly, the
// completeness gate: an entry that doesn't hold the whole set must fall through to
// the live query rather than report a truncated page as the full listing.
func TestSliceCachedValidators(t *testing.T) {
	t.Parallel()

	// A complete cached set: Total equals the number of stored items.
	complete := func(n int) []byte {
		items := make([]ValidatorListItem, n)
		for i := range items {
			items[i] = ValidatorListItem{VotePubkey: string(rune('a' + i%26)), StakeSol: float64(n - i)}
		}
		b, err := json.Marshal(ValidatorListResponse{
			Items: items, Total: n, OnDZCount: 7, Limit: validatorsCacheMaxRows, Offset: 0,
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return b
	}

	// An incomplete entry: count() OVER () saw 1300 rows, only 100 were stored.
	// This is both the >validatorsCacheMaxRows case and a stale pre-upgrade entry.
	incomplete, err := json.Marshal(ValidatorListResponse{
		Items: make([]ValidatorListItem, 100), Total: 1300, OnDZCount: 7, Limit: 100, Offset: 0,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	t.Run("polled shape over complete set", func(t *testing.T) {
		got, ok := sliceCachedValidators(complete(300), PaginationParams{Limit: 900, Offset: 0})
		if !ok {
			t.Fatal("want cache hit")
		}
		if len(got.Items) != 300 {
			t.Errorf("items = %d, want 300", len(got.Items))
		}
		// Whole-set aggregates from the cache; pagination echoed from the request.
		if got.Total != 300 || got.OnDZCount != 7 {
			t.Errorf("Total/OnDZCount = %d/%d, want 300/7", got.Total, got.OnDZCount)
		}
		if got.Limit != 900 || got.Offset != 0 {
			t.Errorf("Limit/Offset = %d/%d, want 900/0", got.Limit, got.Offset)
		}
	})

	t.Run("page clamped at end of set", func(t *testing.T) {
		got, ok := sliceCachedValidators(complete(250), PaginationParams{Limit: 100, Offset: 200})
		if !ok {
			t.Fatal("want cache hit")
		}
		if len(got.Items) != 50 {
			t.Errorf("items = %d, want 50", len(got.Items))
		}
		if got.Offset != 200 {
			t.Errorf("Offset = %d, want 200", got.Offset)
		}
	})

	t.Run("offset past end encodes as empty array", func(t *testing.T) {
		got, ok := sliceCachedValidators(complete(10), PaginationParams{Limit: 100, Offset: 500})
		if !ok {
			t.Fatal("want cache hit")
		}
		if len(got.Items) != 0 {
			t.Fatalf("items = %d, want 0", len(got.Items))
		}
		b, err := json.Marshal(got)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		// null items would break clients that iterate the array.
		if !bytes.Contains(b, []byte(`"items":[]`)) {
			t.Errorf("items not encoded as []: %s", b)
		}
	})

	// A refresh that caught the view mid-reload must not pin an empty listing as an
	// authoritative answer for the rest of the refresh cycle.
	t.Run("empty cached set falls through to live", func(t *testing.T) {
		b, err := json.Marshal(ValidatorListResponse{Items: []ValidatorListItem{}, Total: 0})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if _, ok := sliceCachedValidators(b, PaginationParams{Limit: 900, Offset: 0}); ok {
			t.Error("want cache miss on an empty cached set")
		}
	})

	t.Run("incomplete entry falls through to live", func(t *testing.T) {
		for _, p := range []PaginationParams{{Limit: 900, Offset: 0}, {Limit: 100, Offset: 0}} {
			if _, ok := sliceCachedValidators(incomplete, p); ok {
				t.Errorf("limit=%d offset=%d: want cache miss on a truncated entry", p.Limit, p.Offset)
			}
		}
	})

	t.Run("malformed payload falls through to live", func(t *testing.T) {
		if _, ok := sliceCachedValidators([]byte(`{"items":`), PaginationParams{Limit: 900}); ok {
			t.Error("want cache miss on unparseable payload")
		}
	})
}
