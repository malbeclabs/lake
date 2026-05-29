package mroute

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotsPrefix(t *testing.T) {
	t.Run("no key prefix", func(t *testing.T) {
		s := &S3Source{keyPrefix: ""}
		assert.Equal(t, "snapshots/ip-mroute/", s.snapshotsPrefix())
	})
	t.Run("with key prefix", func(t *testing.T) {
		s := &S3Source{keyPrefix: "env/testnet"}
		assert.Equal(t, "env/testnet/snapshots/ip-mroute/", s.snapshotsPrefix())
	})
}

func TestSnapshotTSFromKey(t *testing.T) {
	t.Run("parses filename produced by state-ingest", func(t *testing.T) {
		key := "snapshots/ip-mroute/device=DzPkAbCdEf/date=2026-05-18/hour=12/20260518T123456Z.json"
		ts := snapshotTSFromKey(key)
		require.False(t, ts.IsZero())
		assert.Equal(t, time.Date(2026, 5, 18, 12, 34, 56, 0, time.UTC), ts)
	})
	t.Run("returns zero on malformed filename", func(t *testing.T) {
		assert.True(t, snapshotTSFromKey("not-a-key").IsZero())
		assert.True(t, snapshotTSFromKey("snapshots/ip-mroute/device=x/date=2026-05-18/hour=12/garbage.json").IsZero())
	})
}

func TestMockSource(t *testing.T) {
	t.Run("returns configured dumps", func(t *testing.T) {
		want := []*Dump{
			{DevicePubkey: "pk1", FileName: "k1.json"},
			{DevicePubkey: "pk2", FileName: "k2.json"},
		}
		ms := NewMockSource(want...)
		got, err := ms.FetchLatest(context.Background())
		require.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("returns configured error", func(t *testing.T) {
		ms := &MockSource{FetchErr: errors.New("boom")}
		dumps, err := ms.FetchLatest(context.Background())
		require.Error(t, err)
		assert.Nil(t, dumps)
	})

	t.Run("close marks closed", func(t *testing.T) {
		ms := NewMockSource()
		assert.False(t, ms.Closed)
		require.NoError(t, ms.Close())
		assert.True(t, ms.Closed)
	})
}

func TestS3SourceClose(t *testing.T) {
	s := &S3Source{}
	assert.NoError(t, s.Close())
}

func TestSourceInterface(t *testing.T) {
	var _ Source = (*MockSource)(nil)
	var _ Source = (*S3Source)(nil)
}
