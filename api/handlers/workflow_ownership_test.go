package handlers_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newAccountSession creates a chat session owned by an account and returns its ID.
func newAccountSession(t *testing.T, ctx context.Context, api *handlers.API, accountID uuid.UUID) uuid.UUID {
	t.Helper()
	sessionID := uuid.New()
	_, err := api.PgPool.Exec(ctx, `
		INSERT INTO sessions (id, type, name, content, account_id)
		VALUES ($1, 'chat', 'Test Session', '[]', $2)
	`, sessionID, accountID)
	require.NoError(t, err)
	return sessionID
}

// newAnonSession creates a chat session owned by an anonymous ID and returns its ID.
func newAnonSession(t *testing.T, ctx context.Context, api *handlers.API, anonID string) uuid.UUID {
	t.Helper()
	sessionID := uuid.New()
	_, err := api.PgPool.Exec(ctx, `
		INSERT INTO sessions (id, type, name, content, anonymous_id)
		VALUES ($1, 'chat', 'Test Session', '[]', $2)
	`, sessionID, anonID)
	require.NoError(t, err)
	return sessionID
}

// --- GetWorkflow ---

func TestGetWorkflow_Ownership(t *testing.T) {
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()

	owner := createTestAccount(t, ctx, api)
	other := createTestAccount(t, ctx, api)
	sessionID := newAccountSession(t, ctx, api, owner.ID)
	run, err := api.CreateWorkflowRun(ctx, sessionID, "Test question")
	require.NoError(t, err)

	newReq := func() *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/api/workflows/"+run.ID.String(), nil)
		return withChiURLParams(req, map[string]string{"id": run.ID.String()})
	}

	t.Run("owner gets 200", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.GetWorkflow(rr, withAccount(newReq(), owner))
		assert.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("different account gets 404", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.GetWorkflow(rr, withAccount(newReq(), other))
		assert.Equal(t, http.StatusNotFound, rr.Code)
	})

	t.Run("no owner context gets 404", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.GetWorkflow(rr, newReq())
		assert.Equal(t, http.StatusNotFound, rr.Code)
	})
}

func TestGetWorkflow_Ownership_Anonymous(t *testing.T) {
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()

	anonID := "anon-" + uuid.New().String()
	sessionID := newAnonSession(t, ctx, api, anonID)
	run, err := api.CreateWorkflowRun(ctx, sessionID, "Test question")
	require.NoError(t, err)

	newReq := func(query string) *http.Request {
		url := "/api/workflows/" + run.ID.String()
		if query != "" {
			url += "?" + query
		}
		req := httptest.NewRequest(http.MethodGet, url, nil)
		return withChiURLParams(req, map[string]string{"id": run.ID.String()})
	}

	t.Run("matching anonymous_id gets 200", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.GetWorkflow(rr, newReq("anonymous_id="+anonID))
		assert.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("wrong anonymous_id gets 404", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.GetWorkflow(rr, newReq("anonymous_id=wrong"))
		assert.Equal(t, http.StatusNotFound, rr.Code)
	})

	t.Run("missing anonymous_id gets 404", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.GetWorkflow(rr, newReq(""))
		assert.Equal(t, http.StatusNotFound, rr.Code)
	})
}

// --- GetWorkflowForSession ---

func TestGetWorkflowForSession_Ownership(t *testing.T) {
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()

	owner := createTestAccount(t, ctx, api)
	other := createTestAccount(t, ctx, api)
	sessionID := newAccountSession(t, ctx, api, owner.ID)
	_, err := api.CreateWorkflowRun(ctx, sessionID, "Test question")
	require.NoError(t, err)

	newReq := func() *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/api/sessions/"+sessionID.String()+"/workflow", nil)
		return withChiURLParams(req, map[string]string{"id": sessionID.String()})
	}

	t.Run("owner gets 200", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.GetWorkflowForSession(rr, withAccount(newReq(), owner))
		assert.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("different account gets 404", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.GetWorkflowForSession(rr, withAccount(newReq(), other))
		assert.Equal(t, http.StatusNotFound, rr.Code)
	})

	t.Run("no owner context gets 404", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.GetWorkflowForSession(rr, newReq())
		assert.Equal(t, http.StatusNotFound, rr.Code)
	})
}

func TestGetWorkflowForSession_Ownership_Anonymous(t *testing.T) {
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()

	anonID := "anon-" + uuid.New().String()
	sessionID := newAnonSession(t, ctx, api, anonID)
	_, err := api.CreateWorkflowRun(ctx, sessionID, "Test question")
	require.NoError(t, err)

	newReq := func(query string) *http.Request {
		url := "/api/sessions/" + sessionID.String() + "/workflow"
		if query != "" {
			url += "?" + query
		}
		req := httptest.NewRequest(http.MethodGet, url, nil)
		return withChiURLParams(req, map[string]string{"id": sessionID.String()})
	}

	t.Run("matching anonymous_id gets 200", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.GetWorkflowForSession(rr, newReq("anonymous_id="+anonID))
		assert.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("wrong anonymous_id gets 404", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.GetWorkflowForSession(rr, newReq("anonymous_id=wrong"))
		assert.Equal(t, http.StatusNotFound, rr.Code)
	})

	t.Run("missing anonymous_id gets 404", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.GetWorkflowForSession(rr, newReq(""))
		assert.Equal(t, http.StatusNotFound, rr.Code)
	})
}

// --- StreamWorkflow ---
// Completed workflows are used so the handler emits catch-up events and returns
// without blocking on the live subscription.

func completedRun(t *testing.T, ctx context.Context, api *handlers.API, sessionID uuid.UUID) uuid.UUID {
	t.Helper()
	run, err := api.CreateWorkflowRun(ctx, sessionID, "Test question")
	require.NoError(t, err)
	require.NoError(t, api.CompleteWorkflowRun(ctx, run.ID, "Answer", &handlers.WorkflowCheckpoint{}))
	return run.ID
}

func TestStreamWorkflow_Ownership(t *testing.T) {
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()

	owner := createTestAccount(t, ctx, api)
	other := createTestAccount(t, ctx, api)
	sessionID := newAccountSession(t, ctx, api, owner.ID)
	runID := completedRun(t, ctx, api, sessionID)

	newReq := func() *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/api/workflows/"+runID.String()+"/stream", nil)
		return withChiURLParams(req, map[string]string{"id": runID.String()})
	}

	t.Run("owner stream proceeds", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.StreamWorkflow(rr, withAccount(newReq(), owner))
		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "text/event-stream", rr.Header().Get("Content-Type"))
	})

	t.Run("different account gets 404 with no SSE data", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.StreamWorkflow(rr, withAccount(newReq(), other))
		assertRejectedBeforeStream(t, rr)
	})

	t.Run("no owner context gets 404 with no SSE data", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.StreamWorkflow(rr, newReq())
		assertRejectedBeforeStream(t, rr)
	})
}

func TestStreamWorkflow_Ownership_Anonymous(t *testing.T) {
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()

	anonID := "anon-" + uuid.New().String()
	sessionID := newAnonSession(t, ctx, api, anonID)
	runID := completedRun(t, ctx, api, sessionID)

	newReq := func(query string) *http.Request {
		url := "/api/workflows/" + runID.String() + "/stream"
		if query != "" {
			url += "?" + query
		}
		req := httptest.NewRequest(http.MethodGet, url, nil)
		return withChiURLParams(req, map[string]string{"id": runID.String()})
	}

	t.Run("matching anonymous_id stream proceeds", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.StreamWorkflow(rr, newReq("anonymous_id="+anonID))
		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "text/event-stream", rr.Header().Get("Content-Type"))
	})

	t.Run("wrong anonymous_id gets 404 with no SSE data", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.StreamWorkflow(rr, newReq("anonymous_id=wrong"))
		assertRejectedBeforeStream(t, rr)
	})

	t.Run("missing anonymous_id gets 404 with no SSE data", func(t *testing.T) {
		rr := httptest.NewRecorder()
		api.StreamWorkflow(rr, newReq(""))
		assertRejectedBeforeStream(t, rr)
	})
}

// assertRejectedBeforeStream verifies a 404 was returned without any SSE headers
// or events being written.
func assertRejectedBeforeStream(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()
	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.NotEqual(t, "text/event-stream", rr.Header().Get("Content-Type"))
	assert.NotContains(t, rr.Body.String(), "event:")
	assert.NotContains(t, rr.Body.String(), "data:")
}

// --- Auto-created session ownership (StartWorkflow / ensureSessionExists) ---
// Auto-created sessions must be stamped with the caller's identity, or the owner
// would fail the ownership check on their own runs. Background execution is
// disabled so the test exercises only StartWorkflow's synchronous session upsert
// (the live runner needs ClickHouse + an LLM client).

func sessionOwner(t *testing.T, ctx context.Context, api *handlers.API, sessionID uuid.UUID) (*uuid.UUID, *string) {
	t.Helper()
	var accountID *uuid.UUID
	var anonymousID *string
	err := api.PgPool.QueryRow(ctx, `
		SELECT account_id, anonymous_id FROM sessions WHERE id = $1
	`, sessionID).Scan(&accountID, &anonymousID)
	require.NoError(t, err)
	return accountID, anonymousID
}

func TestStartWorkflow_StampsAccountOwner(t *testing.T) {
	api := apitesting.NewTestAPIPg(t, testPgDB)
	api.Manager.DisableBackgroundExecutionForTest()
	ctx := t.Context()

	account := createTestAccount(t, ctx, api)
	sessionID := uuid.New() // not yet persisted - StartWorkflow must auto-create it

	_, err := api.Manager.StartWorkflow(sessionID, "Test question", nil, "", &account.ID, nil)
	require.NoError(t, err)

	accountID, anonymousID := sessionOwner(t, ctx, api, sessionID)
	require.NotNil(t, accountID)
	assert.Equal(t, account.ID, *accountID)
	assert.Nil(t, anonymousID)
}

func TestStartWorkflow_StampsAnonymousOwner(t *testing.T) {
	api := apitesting.NewTestAPIPg(t, testPgDB)
	api.Manager.DisableBackgroundExecutionForTest()
	ctx := t.Context()

	anonID := "anon-" + uuid.New().String()
	sessionID := uuid.New()

	_, err := api.Manager.StartWorkflow(sessionID, "Test question", nil, "", nil, &anonID)
	require.NoError(t, err)

	accountID, anonymousID := sessionOwner(t, ctx, api, sessionID)
	assert.Nil(t, accountID)
	require.NotNil(t, anonymousID)
	assert.Equal(t, anonID, *anonymousID)
}

func TestStartWorkflow_PreservesExistingOwner(t *testing.T) {
	api := apitesting.NewTestAPIPg(t, testPgDB)
	api.Manager.DisableBackgroundExecutionForTest()
	ctx := t.Context()

	// Session already owned by an account; a workflow started with different
	// (or no) identity must not overwrite the existing owner.
	owner := createTestAccount(t, ctx, api)
	sessionID := newAccountSession(t, ctx, api, owner.ID)

	_, err := api.Manager.StartWorkflow(sessionID, "Test question", nil, "", nil, nil)
	require.NoError(t, err)

	accountID, anonymousID := sessionOwner(t, ctx, api, sessionID)
	require.NotNil(t, accountID)
	assert.Equal(t, owner.ID, *accountID)
	assert.Nil(t, anonymousID)
}
