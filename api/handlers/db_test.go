package handlers

import "testing"

func TestCompactType(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"String", "String"},
		{"UInt64", "UInt64"},
		{"Nullable(String)", "String"},
		{"LowCardinality(String)", "String"},
		{"LowCardinality(Nullable(String))", "String"},
		{"Nullable(LowCardinality(String))", "String"},
		{"DateTime64(3)", "DateTime64"},
		{"DateTime64(9)", "DateTime64"},
		{"Array(Nullable(UInt64))", "Array(UInt64)"},
		{"Array(LowCardinality(Nullable(String)))", "Array(String)"},
		{"Map(String, UInt64)", "Map(String, UInt64)"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := compactType(tt.input)
			if got != tt.want {
				t.Errorf("compactType(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
