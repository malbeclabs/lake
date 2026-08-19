package feedsubscription

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRPC returns a fixed getProgramAccounts result.
type fakeRPC struct {
	result rpc.GetProgramAccountsResult
	err    error
	calls  int
}

func (f *fakeRPC) GetProgramAccountsWithOpts(ctx context.Context, publicKey solana.PublicKey, opts *rpc.GetProgramAccountsOpts) (rpc.GetProgramAccountsResult, error) {
	f.calls++
	return f.result, f.err
}

func account(t *testing.T, pubkey, dataBase64 string) *rpc.KeyedAccount {
	t.Helper()
	data, err := base64.StdEncoding.DecodeString(dataBase64)
	require.NoError(t, err)
	return &rpc.KeyedAccount{
		Pubkey:  solana.MustPublicKeyFromBase58(pubkey),
		Account: &rpc.Account{Data: rpc.DataBytesOrJSONFromBytes(data)},
	}
}

func newTestView(t *testing.T, r RawRPC) *View {
	t.Helper()
	v, err := NewView(ViewConfig{
		Logger:     laketesting.NewLogger(),
		RPC:        r,
		ProgramID:  ProgramID,
		ClickHouse: testClient(t),
	})
	require.NoError(t, err)
	return v
}

// A refresh writes one row per FeedDistribution account and ignores every other
// account the program owns. The program's ProgramConfig account sits in the same
// getProgramAccounts result, so skipping by discriminator rather than by size is
// what keeps it from being misread as a distribution.
func TestView_Refresh_WritesDistributionsAndSkipsOtherAccounts(t *testing.T) {
	fake := &fakeRPC{result: rpc.GetProgramAccountsResult{
		account(t, "crW8HCYDpQVyCxYG7m3hXeC42rAnjoLroGGfgGLLXM2", realAccountBase64),
		// The program's real ProgramConfig account, first 56 bytes: right owner,
		// different discriminator. It is why the split is by discriminator and
		// not by data size.
		account(t, "7qRWt44BRDKzQz3Q85U5fgfRiaSavesCm6zSN33uLEHn", "z7SF7DAn8RsAAAAAAAAAAAtPRMW4EhleaNSxZ91MJ+qh351quSOOkrihBU4MCyDKAAAAAAAAAAA="),
	}}
	v := newTestView(t, fake)

	result, err := v.Refresh(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsAffected)

	conn, err := v.cfg.ClickHouse.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	rows, err := conn.Query(t.Context(), `
		SELECT pk, feed_key, year, month, collected_usdc
		FROM dim_dz_shred_feed_distributions_current
	`)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var got FeedDistributionRow
	require.NoError(t, rows.Scan(&got.PK, &got.FeedKey, &got.Year, &got.Month, &got.CollectedUSDC))
	assert.Equal(t, "crW8HCYDpQVyCxYG7m3hXeC42rAnjoLroGGfgGLLXM2", got.PK)
	assert.Equal(t, "4Fc1Fyd1x8BoWYPWN8vFhbP6fpgayybQuLUSPRwfE7Wi", got.FeedKey)
	assert.Equal(t, uint16(2026), got.Year)
	assert.Equal(t, uint8(8), got.Month)
	assert.Equal(t, uint64(2080645159), got.CollectedUSDC)
	assert.False(t, rows.Next())
}

// A cluster without the program deployed returns an empty list, not an error.
// That must be a clean no-op rather than a failure, because it is the normal
// state on testnet and in local development.
func TestView_Refresh_EmptyResultIsNotAnError(t *testing.T) {
	v := newTestView(t, &fakeRPC{result: rpc.GetProgramAccountsResult{}})

	result, err := v.Refresh(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsAffected)
}

// An RPC failure fails the refresh rather than being swallowed as an empty
// result: the two look identical unless the error is checked, and a swallowed
// RPC failure would read as "no feeds collected anything," not as "the read
// didn't happen."
func TestView_Refresh_RPCErrorFails(t *testing.T) {
	fake := &fakeRPC{err: errors.New("boom")}
	v := newTestView(t, fake)

	_, err := v.Refresh(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetching feed-subscription program accounts")
	assert.Equal(t, 1, fake.calls)
}

// A result that holds accounts but no FeedDistribution among them fails the
// refresh. That is what a renamed discriminator (a ::v3) looks like, and it has
// to be louder than an empty write: ReplaceFeedDistributions no-ops on an empty
// batch, so the alternative is a table frozen at its last good totals while the
// refresh keeps reporting success.
func TestView_Refresh_NoMatchingDiscriminatorFails(t *testing.T) {
	v := newTestView(t, &fakeRPC{result: rpc.GetProgramAccountsResult{
		// The program's real ProgramConfig account, and nothing else.
		account(t, "7qRWt44BRDKzQz3Q85U5fgfRiaSavesCm6zSN33uLEHn", "z7SF7DAn8RsAAAAAAAAAAAtPRMW4EhleaNSxZ91MJ+qh351quSOOkrihBU4MCyDKAAAAAAAAAAA="),
	}})

	_, err := v.Refresh(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "discriminator")
	assert.Contains(t, err.Error(), hexDiscriminator())
}

// This pins the refresh failing outright on a malformed FeedDistribution,
// rather than writing the accounts that did decode. A discriminator match that
// fails to decode means the on-chain layout moved, and a partial write would
// under-report revenue for the accounts silently dropped.
func TestView_Refresh_BadAccountFailsRefresh(t *testing.T) {
	v := newTestView(t, &fakeRPC{result: rpc.GetProgramAccountsResult{
		account(t, "crW8HCYDpQVyCxYG7m3hXeC42rAnjoLroGGfgGLLXM2", "OGd+UVWaSNwAAA=="),
	}})

	_, err := v.Refresh(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}
