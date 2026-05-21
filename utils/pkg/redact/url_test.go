package redact_test

import (
	"errors"
	"testing"

	"github.com/malbeclabs/lake/utils/pkg/redact"
	"github.com/stretchr/testify/assert"
)

func TestURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "basic auth user:pass",
			in:   "https://user:secret@api.example.com/v1",
			want: "https://redacted@api.example.com/v1",
		},
		{
			name: "basic auth user only",
			in:   "https://admin@api.example.com/v1",
			want: "https://redacted@api.example.com/v1",
		},
		{
			name: "api-key query param",
			in:   "https://mainnet.helius-rpc.com/?api-key=abc123secret",
			want: "https://mainnet.helius-rpc.com/?api-key=redacted",
		},
		{
			name: "token query param case-insensitive",
			in:   "https://api.example.com/v1?Token=secret&foo=bar",
			want: "https://api.example.com/v1?Token=redacted&foo=bar",
		},
		{
			name: "non-sensitive query params preserved",
			in:   "postgres://host:5432/db?sslmode=disable&pool_max=10",
			want: "postgres://host:5432/db?sslmode=disable&pool_max=10",
		},
		{
			name: "path-embedded UUID token",
			in:   "https://doublezero-mainnet-beta.rpcpool.com/db336024-e7a8-46b1-80e5-352dd77060ab",
			want: "https://doublezero-mainnet-beta.rpcpool.com/redacted",
		},
		{
			name: "path-embedded long hex token",
			in:   "https://api.example.com/abc123def456abc123def456abc123def456",
			want: "https://api.example.com/redacted",
		},
		{
			name: "path-embedded long alphanumeric token",
			in:   "https://name.solana.quiknode.pro/abc123def456ghi789jkl012/",
			want: "https://name.solana.quiknode.pro/redacted/",
		},
		{
			name: "normal path components preserved",
			in:   "https://api.example.com/v1/getValidators/mainnet-beta",
			want: "https://api.example.com/v1/getValidators/mainnet-beta",
		},
		{
			name: "path with both normal and token segments",
			in:   "https://api.example.com/v1/db336024-e7a8-46b1-80e5-352dd77060ab/data",
			want: "https://api.example.com/v1/redacted/data",
		},
		{
			name: "all three forms combined",
			in:   "https://user:pass@api.example.com/abc123def456abc123def456abc123def456?token=xxx&keep=yes",
			want: "https://redacted@api.example.com/redacted?keep=yes&token=redacted",
		},
		{
			name: "url with no credentials passes through",
			in:   "https://api.mainnet-beta.solana.com",
			want: "https://api.mainnet-beta.solana.com",
		},
		{
			name: "non-url returned unchanged",
			in:   "not a url",
			want: "not a url",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, redact.URL(tt.in))
		})
	}
}

func TestString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "empty string",
			in:   "",
			want: "",
		},
		{
			name: "no url",
			in:   "something went wrong",
			want: "something went wrong",
		},
		{
			name: "url in error message",
			in:   `Get "https://mainnet.helius-rpc.com/?api-key=secret123": connection refused`,
			want: `Get "https://mainnet.helius-rpc.com/?api-key=redacted": connection refused`,
		},
		{
			name: "url at end of sentence with period",
			in:   "see https://api.example.com/?token=secret.",
			want: "see https://api.example.com/?token=redacted.",
		},
		{
			name: "url with path token in error",
			in:   "failed: GET https://x.com/db336024-e7a8-46b1-80e5-352dd77060ab returned 500",
			want: "failed: GET https://x.com/redacted returned 500",
		},
		{
			name: "multiple urls in one string",
			in:   "from https://a.com/?key=secret1 to https://b.com/?token=secret2",
			want: "from https://a.com/?key=redacted to https://b.com/?token=redacted",
		},
		{
			name: "basic auth in error",
			in:   "failed to connect: postgres://user:pass@localhost:5432/db",
			want: "failed to connect: postgres://redacted@localhost:5432/db",
		},
		{
			name: "non-sensitive query preserved in error",
			in:   "connect: postgres://localhost:5432/db?sslmode=disable",
			want: "connect: postgres://localhost:5432/db?sslmode=disable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, redact.String(tt.in))
		})
	}
}

func TestError(t *testing.T) {
	t.Parallel()
	t.Run("nil", func(t *testing.T) {
		assert.Equal(t, "", redact.Error(nil))
	})
	t.Run("non-nil with url", func(t *testing.T) {
		err := errors.New("connection failed: https://api.example.com/?api-key=secret")
		assert.Equal(t,
			"connection failed: https://api.example.com/?api-key=redacted",
			redact.Error(err),
		)
	})
}
