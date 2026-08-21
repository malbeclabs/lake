package dzingest

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	shredssdk "github.com/malbeclabs/doublezero/sdk/shreds/go"
	dzshreds "github.com/malbeclabs/lake/indexer/pkg/dz/shreds"
	"github.com/malbeclabs/lake/indexer/pkg/dz/shreds/feedsubscription"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingShredsRPC makes dzshreds.View.Refresh fail at its first call.
type failingShredsRPC struct{}

func (failingShredsRPC) FetchExecutionController(context.Context) (*shredssdk.ExecutionController, error) {
	return nil, errors.New("shreds rpc is down")
}

func (failingShredsRPC) FetchShredDistribution(context.Context, uint64) (*shredssdk.ShredDistribution, error) {
	return nil, errors.New("shreds rpc is down")
}

// countingRawRPC records whether the feed-subscription read was attempted.
type countingRawRPC struct{ calls atomic.Int64 }

func (c *countingRawRPC) GetProgramAccountsWithOpts(context.Context, solana.PublicKey, *solanarpc.GetProgramAccountsOpts) (solanarpc.GetProgramAccountsResult, error) {
	c.calls.Add(1)
	return solanarpc.GetProgramAccountsResult{}, nil
}

// The shreds and feed-subscription reads are of different programs and share
// nothing, so a shreds outage must not stop feed indexing. The two used to run in
// sequence with an early return between them, which made the feed read depend on
// the shreds read for no reason beyond the order of the statements — a shreds RPC
// outage would have silently frozen feed revenue for its whole duration.
func TestRefreshShreds_FeedReadRunsWhenShredsFails(t *testing.T) {
	client := laketesting.NewClient(t, sharedDB)
	log := laketesting.NewLogger()

	shredsView, err := dzshreds.NewView(dzshreds.ViewConfig{
		Logger:          log,
		ShredsRPC:       failingShredsRPC{},
		ShredsRawRPC:    &countingRawRPC{},
		ProgramID:       solana.NewWallet().PublicKey(),
		RefreshInterval: time.Minute,
		ClickHouse:      client,
	})
	require.NoError(t, err)

	feedRPC := &countingRawRPC{}
	feedView, err := feedsubscription.NewView(feedsubscription.ViewConfig{
		Logger:     log,
		RPC:        feedRPC,
		ProgramID:  feedsubscription.ProgramID,
		ClickHouse: client,
	})
	require.NoError(t, err)

	a := &Activities{Log: log, Shreds: shredsView, FeedSubscription: feedView}

	// The activity swallows the failure into the escalator, so a nil return here
	// is expected and is not evidence the shreds read succeeded.
	require.NoError(t, a.RefreshShreds(t.Context()))

	assert.Equal(t, int64(1), feedRPC.calls.Load(),
		"the feed-subscription read must still run when the shreds read fails")
}

// With no feed view configured the activity is unchanged, so an environment
// without the feed program keeps working exactly as before.
func TestRefreshShreds_NoFeedViewConfigured(t *testing.T) {
	client := laketesting.NewClient(t, sharedDB)
	log := laketesting.NewLogger()

	shredsView, err := dzshreds.NewView(dzshreds.ViewConfig{
		Logger:          log,
		ShredsRPC:       failingShredsRPC{},
		ShredsRawRPC:    &countingRawRPC{},
		ProgramID:       solana.NewWallet().PublicKey(),
		RefreshInterval: time.Minute,
		ClickHouse:      client,
	})
	require.NoError(t, err)

	a := &Activities{Log: log, Shreds: shredsView}
	require.NoError(t, a.RefreshShreds(t.Context()))
}
