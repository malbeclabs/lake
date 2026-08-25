package dzingest

import (
	"context"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/jonboulle/clockwork"
	"github.com/malbeclabs/lake/indexer/pkg/dz/serviceability/permissionevents"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/require"
)

// stubPermissionRPC satisfies permissionevents.SolanaRPC; the truncate activity never
// touches the RPC, it only needs a constructible view for its ClickHouse client.
type stubPermissionRPC struct{}

func (stubPermissionRPC) GetProgramAccountsWithOpts(context.Context, solana.PublicKey, *solanarpc.GetProgramAccountsOpts) (solanarpc.GetProgramAccountsResult, error) {
	return nil, nil
}

func (stubPermissionRPC) GetSignaturesForAddressWithOpts(context.Context, solana.PublicKey, *solanarpc.GetSignaturesForAddressOpts) ([]*solanarpc.TransactionSignature, error) {
	return nil, nil
}

func (stubPermissionRPC) GetTransaction(context.Context, solana.Signature, *solanarpc.GetTransactionOpts) (*solanarpc.GetTransactionResult, error) {
	return nil, nil
}

// TestLake_DZIngest_TruncatePermissionEvents_ResetsFactAndCursors: the --truncate
// recovery path must clear the fact table AND both cursors. A surviving account cursor
// would sit stale-newer than the re-derived high-water marks and make the steady-state
// watch silently skip everything below it after a reset.
func TestLake_DZIngest_TruncatePermissionEvents_ResetsFactAndCursors(t *testing.T) {
	t.Parallel()

	ch := laketesting.NewClient(t, sharedDB)
	view, err := permissionevents.NewView(permissionevents.ViewConfig{
		Logger:          laketesting.NewLogger(),
		Clock:           clockwork.NewRealClock(),
		RPC:             stubPermissionRPC{},
		ProgramID:       solana.MustPublicKeyFromBase58("DZtnuQ839pSaDMFG5q1ad2V95G82S5EC4RrB3Ndw2Heb"),
		RefreshInterval: time.Second,
		ClickHouse:      ch,
	})
	require.NoError(t, err)

	conn, err := ch.Conn(t.Context())
	require.NoError(t, err)
	seeds := []string{
		`INSERT INTO fact_dz_permission_events
			(event_ts, ingested_at, tx_signature, slot, instruction_index, permission_pk, event_type)
			VALUES (now(), now(), 'sig', 1, 0, 'pk', 'Create')`,
		`INSERT INTO dz_permission_events_scan_cursor
			(program_pk, last_tx_signature, last_slot, updated_at) VALUES ('prog', 'sig', 1, now())`,
		`INSERT INTO dz_permission_events_account_cursor
			(permission_pk, last_tx_signature, last_slot, updated_at) VALUES ('pk', 'sig', 1, now())`,
	}
	for _, seed := range seeds {
		require.NoError(t, conn.Exec(t.Context(), seed))
	}

	a := &Activities{Log: laketesting.NewLogger(), PermissionEvents: view}
	require.NoError(t, a.TruncatePermissionEvents(t.Context()))

	for _, table := range []string{
		"fact_dz_permission_events",
		"dz_permission_events_scan_cursor",
		"dz_permission_events_account_cursor",
	} {
		rows, err := conn.Query(t.Context(), "SELECT count() FROM "+table)
		require.NoError(t, err)
		require.True(t, rows.Next())
		var n uint64
		require.NoError(t, rows.Scan(&n))
		require.NoError(t, rows.Close())
		require.Zero(t, n, "table %s must be empty after truncate", table)
	}
}
