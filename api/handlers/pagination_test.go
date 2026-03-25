package handlers_test

import (
	"net/http/httptest"
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	"github.com/stretchr/testify/assert"
)

func TestParsePagination_Defaults(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest("GET", "/api/test", nil)

	params := handlers.ParsePagination(req, 0)

	assert.Equal(t, handlers.DefaultLimit, params.Limit)
	assert.Equal(t, 0, params.Offset)
}

func TestParsePagination_Custom(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest("GET", "/api/test?limit=25&offset=50", nil)

	params := handlers.ParsePagination(req, 0)

	assert.Equal(t, 25, params.Limit)
	assert.Equal(t, 50, params.Offset)
}

func TestParsePagination_CustomDefault(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest("GET", "/api/test", nil)

	params := handlers.ParsePagination(req, 50)

	assert.Equal(t, 50, params.Limit)
	assert.Equal(t, 0, params.Offset)
}

func TestParsePagination_MaxLimit(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest("GET", "/api/test?limit=5000", nil)

	params := handlers.ParsePagination(req, 0)

	assert.Equal(t, handlers.MaxLimit, params.Limit)
}

func TestParsePagination_NegativeValues(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest("GET", "/api/test?limit=-10&offset=-5", nil)

	params := handlers.ParsePagination(req, 100)

	// Negative limit should use default
	assert.Equal(t, 100, params.Limit)
	// Negative offset should stay at 0
	assert.Equal(t, 0, params.Offset)
}

func TestParsePagination_InvalidValues(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest("GET", "/api/test?limit=abc&offset=xyz", nil)

	params := handlers.ParsePagination(req, 100)

	// Invalid values should use defaults
	assert.Equal(t, 100, params.Limit)
	assert.Equal(t, 0, params.Offset)
}

func TestParsePagination_ZeroLimit(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest("GET", "/api/test?limit=0", nil)

	params := handlers.ParsePagination(req, 100)

	// Zero limit should use default
	assert.Equal(t, 100, params.Limit)
}

func TestParsePagination_ExactMaxLimit(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest("GET", "/api/test?limit=1000", nil)

	params := handlers.ParsePagination(req, 0)

	assert.Equal(t, 1000, params.Limit)
}

func TestParsePagination_JustOverMaxLimit(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest("GET", "/api/test?limit=1001", nil)

	params := handlers.ParsePagination(req, 0)

	assert.Equal(t, handlers.MaxLimit, params.Limit)
}

func TestParseFilters_RepeatedParams(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest("GET", "/api/test?filters=vote:abc&filters=city:NYC", nil)
	mf := handlers.ParseFilters(req)
	assert.Len(t, mf.Filters, 2)
	assert.Equal(t, "vote", mf.Filters[0].Field)
	assert.Equal(t, "abc", mf.Filters[0].Value)
	assert.Equal(t, "city", mf.Filters[1].Field)
	assert.Equal(t, "NYC", mf.Filters[1].Value)
}

func TestParseFilters_PlainValue(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest("GET", "/api/test?filters=hello", nil)
	mf := handlers.ParseFilters(req)
	assert.Len(t, mf.Filters, 1)
	assert.Equal(t, "all", mf.Filters[0].Field)
	assert.Equal(t, "hello", mf.Filters[0].Value)
}

func TestParseFilters_LegacyFallback(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest("GET", "/api/test?filter_field=vote&filter_value=abc", nil)
	mf := handlers.ParseFilters(req)
	assert.Len(t, mf.Filters, 1)
	assert.Equal(t, "vote", mf.Filters[0].Field)
	assert.Equal(t, "abc", mf.Filters[0].Value)
}

func TestParseFilters_Empty(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest("GET", "/api/test", nil)
	mf := handlers.ParseFilters(req)
	assert.True(t, mf.IsEmpty())
}

func TestMultiFilterBuildClause_SingleFilter(t *testing.T) {
	t.Parallel()
	fields := map[string]handlers.FilterFieldConfig{
		"vote": {Column: "vote_pubkey", Type: handlers.FieldTypeText},
		"city": {Column: "city", Type: handlers.FieldTypeText},
	}
	mf := handlers.MultiFilterParams{
		Filters: []handlers.FilterParams{{Field: "vote", Value: "abc"}},
	}
	clause, args := mf.BuildFilterClause(fields)
	assert.Equal(t, "positionCaseInsensitive(vote_pubkey, ?) > 0", clause)
	assert.Equal(t, []any{"abc"}, args)
}

func TestMultiFilterBuildClause_CrossFieldAND(t *testing.T) {
	t.Parallel()
	fields := map[string]handlers.FilterFieldConfig{
		"vote": {Column: "vote_pubkey", Type: handlers.FieldTypeText},
		"city": {Column: "city", Type: handlers.FieldTypeText},
	}
	mf := handlers.MultiFilterParams{
		Filters: []handlers.FilterParams{
			{Field: "vote", Value: "abc"},
			{Field: "city", Value: "NYC"},
		},
	}
	clause, args := mf.BuildFilterClause(fields)
	assert.Equal(t, "(positionCaseInsensitive(vote_pubkey, ?) > 0 AND positionCaseInsensitive(city, ?) > 0)", clause)
	assert.Equal(t, []any{"abc", "NYC"}, args)
}

func TestMultiFilterBuildClause_SameFieldOR(t *testing.T) {
	t.Parallel()
	fields := map[string]handlers.FilterFieldConfig{
		"city": {Column: "city", Type: handlers.FieldTypeText},
	}
	mf := handlers.MultiFilterParams{
		Filters: []handlers.FilterParams{
			{Field: "city", Value: "NYC"},
			{Field: "city", Value: "LAX"},
		},
	}
	clause, args := mf.BuildFilterClause(fields)
	assert.Equal(t, "(positionCaseInsensitive(city, ?) > 0 OR positionCaseInsensitive(city, ?) > 0)", clause)
	assert.Equal(t, []any{"NYC", "LAX"}, args)
}

func TestMultiFilterBuildClause_MixedANDOR(t *testing.T) {
	t.Parallel()
	fields := map[string]handlers.FilterFieldConfig{
		"city": {Column: "city", Type: handlers.FieldTypeText},
		"dz":   {Column: "on_dz", Type: handlers.FieldTypeBoolean},
	}
	mf := handlers.MultiFilterParams{
		Filters: []handlers.FilterParams{
			{Field: "city", Value: "NYC"},
			{Field: "city", Value: "LAX"},
			{Field: "dz", Value: "yes"},
		},
	}
	clause, args := mf.BuildFilterClause(fields)
	assert.Equal(t, "((positionCaseInsensitive(city, ?) > 0 OR positionCaseInsensitive(city, ?) > 0) AND on_dz = true)", clause)
	assert.Equal(t, []any{"NYC", "LAX"}, args)
}

func TestMultiFilterBuildClause_Empty(t *testing.T) {
	t.Parallel()
	fields := map[string]handlers.FilterFieldConfig{
		"vote": {Column: "vote_pubkey", Type: handlers.FieldTypeText},
	}
	mf := handlers.MultiFilterParams{}
	clause, args := mf.BuildFilterClause(fields)
	assert.Equal(t, "", clause)
	assert.Nil(t, args)
}

func TestPaginatedResponse_JSONStructure(t *testing.T) {
	t.Parallel()
	// Test that the generic type works correctly
	type Item struct {
		Name string `json:"name"`
	}

	response := handlers.PaginatedResponse[Item]{
		Items:  []Item{{Name: "test1"}, {Name: "test2"}},
		Total:  100,
		Limit:  10,
		Offset: 0,
	}

	assert.Len(t, response.Items, 2)
	assert.Equal(t, 100, response.Total)
	assert.Equal(t, 10, response.Limit)
	assert.Equal(t, 0, response.Offset)
}
