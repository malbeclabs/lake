package handlers_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeatAlertsMigration_TablesExist(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()

	var n int
	err := api.PgPool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables
		 WHERE table_name IN ('telegram_contacts','seat_alerts')`).Scan(&n)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

func TestSeatAlertLifecycle(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	acct := createTestAccount(t, ctx, api)

	// create -> pending, has token, no contact
	a, err := api.CreateSeatAlert(ctx, acct.ID, "seat-pk-1", "epochs_left", 2, true)
	require.NoError(t, err)
	require.NotEmpty(t, a.ActivationToken)
	assert.Equal(t, "pending_activation", a.Status)
	assert.Nil(t, a.ContactID)

	// The Postgres test DB is shared across the package, so the global active
	// list may hold alerts from other tests. Scope assertions to this alert.
	findMine := func(list []handlers.AlertWithContact) *handlers.AlertWithContact {
		for i := range list {
			if list[i].Alert.ID == a.ID {
				return &list[i]
			}
		}
		return nil
	}

	// not in active list yet
	active, err := api.ListActiveAlertsWithContacts(ctx)
	require.NoError(t, err)
	assert.Nil(t, findMine(active), "not active before activation")

	// activate via token -> active, linked contact
	act, err := api.ActivateSeatAlertByToken(ctx, a.ActivationToken, int64(4242), "tester")
	require.NoError(t, err)
	assert.Equal(t, "active", act.Status)
	require.NotNil(t, act.ContactID)

	// activation token is single-use: a second activation fails
	_, err = api.ActivateSeatAlertByToken(ctx, a.ActivationToken, int64(4242), "tester")
	require.Error(t, err)

	// now in active list with chat id
	active, err = api.ListActiveAlertsWithContacts(ctx)
	require.NoError(t, err)
	mine := findMine(active)
	require.NotNil(t, mine, "activated alert should be in active list")
	assert.Equal(t, int64(4242), mine.ChatID)

	// mark notified -> stored and readable back
	require.NoError(t, api.MarkAlertNotified(ctx, act.ID, 1000))
	refetched, err := api.GetSeatAlertByToken(ctx, a.ActivationToken)
	require.NoError(t, err)
	require.NotNil(t, refetched.LastNotifiedEpoch)
	assert.Equal(t, int64(1000), *refetched.LastNotifiedEpoch)

	// stop by chat -> removed from active
	n, err := api.StopAlertsByChatID(ctx, 4242)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	active, err = api.ListActiveAlertsWithContacts(ctx)
	require.NoError(t, err)
	assert.Nil(t, findMine(active), "not active after stop")
}

func TestActivateSeatAlert_BadToken(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	_, err := api.ActivateSeatAlertByToken(ctx, uuid.NewString(), 1, "x")
	require.Error(t, err)
}
