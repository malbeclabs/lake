package validatorsapp

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/require"
)

type mockClient struct {
	getValidatorsFunc func(ctx context.Context) ([]Validator, error)
}

func (m *mockClient) GetValidators(ctx context.Context) ([]Validator, error) {
	if m.getValidatorsFunc != nil {
		return m.getValidatorsFunc(ctx)
	}
	return []Validator{}, nil
}

func TestView_Ready(t *testing.T) {
	t.Parallel()

	t.Run("returns false before refresh", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			Clock:           clockwork.NewFakeClock(),
			Client:          &mockClient{},
			ClickHouse:      db,
			RefreshInterval: time.Second,
		})
		require.NoError(t, err)
		require.False(t, view.Ready(), "view should not be ready before first refresh")
	})

	t.Run("returns true after successful refresh", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		validators := loadFixtureValidators(t)

		view, err := NewView(ViewConfig{
			Logger: laketesting.NewLogger(),
			Clock:  clockwork.NewFakeClock(),
			Client: &mockClient{
				getValidatorsFunc: func(ctx context.Context) ([]Validator, error) {
					return validators, nil
				},
			},
			ClickHouse:      db,
			RefreshInterval: time.Second,
		})
		require.NoError(t, err)

		err = view.Refresh(context.Background())
		require.NoError(t, err)
		require.True(t, view.Ready(), "view should be ready after successful refresh")
	})
}

func TestView_WaitReady(t *testing.T) {
	t.Parallel()

	t.Run("returns error when context is cancelled", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			Clock:           clockwork.NewFakeClock(),
			Client:          &mockClient{},
			ClickHouse:      db,
			RefreshInterval: time.Second,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err = view.WaitReady(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "context cancelled")
	})
}

func TestView_Refresh(t *testing.T) {
	t.Parallel()

	t.Run("writes fixture data to ClickHouse", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		validators := loadFixtureValidators(t)

		view, err := NewView(ViewConfig{
			Logger: laketesting.NewLogger(),
			Clock:  clockwork.NewFakeClock(),
			Client: &mockClient{
				getValidatorsFunc: func(ctx context.Context) ([]Validator, error) {
					return validators, nil
				},
			},
			ClickHouse:      db,
			RefreshInterval: time.Second,
		})
		require.NoError(t, err)

		err = view.Refresh(context.Background())
		require.NoError(t, err)

		// Verify data was written
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer conn.Close()

		rows, err := conn.Query(context.Background(), `
			SELECT count()
			FROM dim_validatorsapp_validators_history
			WHERE is_deleted = 0
		`)
		require.NoError(t, err)
		require.True(t, rows.Next())
		var count uint64
		require.NoError(t, rows.Scan(&count))
		rows.Close()
		require.Equal(t, uint64(len(validators)), count, "should have written all validators")
	})

	t.Run("rejects empty response", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		view, err := NewView(ViewConfig{
			Logger: laketesting.NewLogger(),
			Clock:  clockwork.NewFakeClock(),
			Client: &mockClient{
				getValidatorsFunc: func(ctx context.Context) ([]Validator, error) {
					return []Validator{}, nil
				},
			},
			ClickHouse:      db,
			RefreshInterval: time.Second,
		})
		require.NoError(t, err)

		err = view.Refresh(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "rejecting empty validator response")
		require.False(t, view.Ready(), "view should not be ready after empty response")
	})

	t.Run("propagates API errors", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		view, err := NewView(ViewConfig{
			Logger: laketesting.NewLogger(),
			Clock:  clockwork.NewFakeClock(),
			Client: &mockClient{
				getValidatorsFunc: func(ctx context.Context) ([]Validator, error) {
					return nil, fmt.Errorf("API rate limited")
				},
			},
			ClickHouse:      db,
			RefreshInterval: time.Second,
		})
		require.NoError(t, err)

		err = view.Refresh(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "API rate limited")
		require.False(t, view.Ready(), "view should not be ready after API error")
	})
}
