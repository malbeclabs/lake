package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSeatAlert_ReturnsDeepLink(t *testing.T) {
	// Not t.Parallel(): t.Setenv is disallowed in parallel tests (panics at runtime).
	t.Setenv("TELEGRAM_BOT_USERNAME", "dz_test_bot")
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	account := createTestAccount(t, ctx, api)

	body, _ := json.Marshal(map[string]any{
		"seat_pk": "seat-abc", "trigger_type": "epochs_left",
		"threshold_value": 2, "announcements_opt_in": true,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/dz/shreds/seat-alerts", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = withAccount(req, account)
	rr := httptest.NewRecorder()
	api.CreateSeatAlertHandler(rr, req)

	require.Equal(t, http.StatusCreated, rr.Code, "body: %s", rr.Body.String())
	var resp map[string]any
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.NotEmpty(t, resp["activation_token"])
	assert.Contains(t, resp["telegram_deep_link"], "https://t.me/dz_test_bot?start=")
}

func TestCreateSeatAlert_BadTrigger(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	account := createTestAccount(t, ctx, api)
	body, _ := json.Marshal(map[string]any{"seat_pk": "s", "trigger_type": "nope", "threshold_value": 1})
	req := withAccount(httptest.NewRequest(http.MethodPost, "/api/dz/shreds/seat-alerts", bytes.NewReader(body)), account)
	rr := httptest.NewRecorder()
	api.CreateSeatAlertHandler(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

type fakeSender struct {
	lastChat int64
	lastText string
	calls    int
}

func (f *fakeSender) SendMessage(ctx context.Context, chatID int64, text string) error {
	f.lastChat = chatID
	f.lastText = text
	f.calls++
	return nil
}

func TestSendTestAlert(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	account := createTestAccount(t, ctx, api)
	sender := &fakeSender{}
	api.TelegramSender = sender

	// create + activate an alert so it has a chat id
	a, err := api.CreateSeatAlert(ctx, account.ID, "seat-xyz", "epochs_left", 2, true)
	require.NoError(t, err)
	_, err = api.ActivateSeatAlertByToken(ctx, a.ActivationToken, 9001, "tester")
	require.NoError(t, err)

	req := withChiURLParams(withAccount(httptest.NewRequest(http.MethodPost,
		"/api/dz/shreds/seat-alerts/"+a.ID.String()+"/test", nil), account),
		map[string]string{"id": a.ID.String()})
	rr := httptest.NewRecorder()
	api.SendTestAlertHandler(rr, req)

	require.Equal(t, http.StatusNoContent, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, 1, sender.calls)
	assert.Equal(t, int64(9001), sender.lastChat)
}
