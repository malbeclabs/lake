package bot

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeStore struct {
	activatedToken     string
	activatedChatID    int64
	stoppedChatID      int64
	stopOneChatID      int64
	stopOneIndex       int
	announceChatID     int64
	announceOptIn      bool
	announceOptInCalls int
}

func (f *fakeStore) Activate(ctx context.Context, token string, chatID int64, username string) (string, error) {
	f.activatedToken = token
	f.activatedChatID = chatID
	return "seat-abc", nil // returns a short seat description
}
func (f *fakeStore) List(ctx context.Context, chatID int64) ([]string, error) {
	return []string{"seat-abc"}, nil
}
func (f *fakeStore) Stop(ctx context.Context, chatID int64) (int64, error) {
	f.stoppedChatID = chatID
	return 1, nil
}
func (f *fakeStore) StopOne(ctx context.Context, chatID int64, index int) (string, bool, error) {
	f.stopOneChatID = chatID
	f.stopOneIndex = index
	return "seat-abc", true, nil
}
func (f *fakeStore) SetAnnouncements(ctx context.Context, chatID int64, optIn bool) (bool, error) {
	f.announceChatID = chatID
	f.announceOptIn = optIn
	f.announceOptInCalls++
	return true, nil
}

func newTestHandler(t *testing.T, store Store) (*EventHandler, *httptest.Server, *[]map[string]any) {
	var sent []map[string]any
	tg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		sent = append(sent, m)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(tg.Close)
	client := NewClient("TESTTOKEN")
	client.baseURL = tg.URL
	return NewEventHandler(client, store, "s3cret", slog.Default()), tg, &sent
}

func postUpdate(t *testing.T, h *EventHandler, secret string, update map[string]any) *httptest.ResponseRecorder {
	body, _ := json.Marshal(update)
	req := httptest.NewRequest(http.MethodPost, "/telegram/webhook", bytes.NewReader(body))
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)
	rr := httptest.NewRecorder()
	h.HandleHTTP(rr, req)
	return rr
}

func TestWebhook_RejectsBadSecret(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t, &fakeStore{})
	rr := postUpdate(t, h, "wrong", map[string]any{})
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d want 401", rr.Code)
	}
}

func TestWebhook_StartActivates(t *testing.T) {
	t.Parallel()
	store := &fakeStore{}
	h, _, sent := newTestHandler(t, store)
	update := map[string]any{
		"update_id": 1,
		"message": map[string]any{
			"text": "/start tok-123",
			"chat": map[string]any{"id": 4242},
			"from": map[string]any{"username": "tester"},
		},
	}
	rr := postUpdate(t, h, "s3cret", update)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d want 200", rr.Code)
	}
	h.WaitForTest() // drain async processing (test-only helper)
	if store.activatedToken != "tok-123" || store.activatedChatID != 4242 {
		t.Fatalf("activation not recorded: %+v", store)
	}
	if len(*sent) == 0 {
		t.Fatalf("expected a confirmation message to be sent")
	}
}

func TestWebhook_StopAll(t *testing.T) {
	t.Parallel()
	store := &fakeStore{}
	h, _, _ := newTestHandler(t, store)
	update := map[string]any{
		"update_id": 2,
		"message": map[string]any{
			"text": "/stop",
			"chat": map[string]any{"id": 555},
		},
	}
	rr := postUpdate(t, h, "s3cret", update)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d want 200", rr.Code)
	}
	h.WaitForTest() // drain async processing (test-only helper)
	if store.stoppedChatID != 555 {
		t.Fatalf("expected Stop (all) to be called for chat 555, got %+v", store)
	}
	if store.stopOneIndex != 0 {
		t.Fatalf("expected StopOne not to be called, got index %d", store.stopOneIndex)
	}
}

func TestWebhook_Help(t *testing.T) {
	t.Parallel()
	h, _, sent := newTestHandler(t, &fakeStore{})
	update := map[string]any{
		"update_id": 4,
		"message": map[string]any{
			"text": "/help",
			"chat": map[string]any{"id": 888},
		},
	}
	rr := postUpdate(t, h, "s3cret", update)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d want 200", rr.Code)
	}
	h.WaitForTest() // drain async processing (test-only helper)
	if len(*sent) == 0 {
		t.Fatalf("expected a reply to be sent")
	}
	text, _ := (*sent)[0]["text"].(string)
	if !strings.Contains(text, "<pre>") || !strings.Contains(text, "/stop [n]") {
		t.Fatalf("help reply missing expected content: %q", text)
	}
}

func TestWebhook_Topup(t *testing.T) {
	t.Parallel()
	h, _, sent := newTestHandler(t, &fakeStore{})
	update := map[string]any{
		"update_id": 5,
		"message": map[string]any{
			"text": "/topup",
			"chat": map[string]any{"id": 999},
		},
	}
	rr := postUpdate(t, h, "s3cret", update)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d want 200", rr.Code)
	}
	h.WaitForTest() // drain async processing (test-only helper)
	if len(*sent) == 0 {
		t.Fatalf("expected a reply to be sent")
	}
	text, _ := (*sent)[0]["text"].(string)
	if !strings.Contains(text, "<pre>") || !strings.Contains(text, "doublezero-solana shreds pay") {
		t.Fatalf("topup reply missing expected content: %q", text)
	}
}

func TestWebhook_AnnouncementsOff(t *testing.T) {
	t.Parallel()
	store := &fakeStore{}
	h, _, sent := newTestHandler(t, store)
	update := map[string]any{
		"update_id": 6,
		"message": map[string]any{
			"text": "/announcements off",
			"chat": map[string]any{"id": 111},
		},
	}
	rr := postUpdate(t, h, "s3cret", update)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d want 200", rr.Code)
	}
	h.WaitForTest() // drain async processing (test-only helper)
	if store.announceOptInCalls != 1 || store.announceChatID != 111 || store.announceOptIn != false {
		t.Fatalf("expected SetAnnouncements(chat=111, optIn=false), got %+v", store)
	}
	if len(*sent) == 0 {
		t.Fatalf("expected a confirmation message to be sent")
	}
	text, _ := (*sent)[0]["text"].(string)
	if !strings.Contains(text, "OFF") {
		t.Fatalf("announcements-off reply missing expected content: %q", text)
	}
}

func TestWebhook_AnnouncementsOn(t *testing.T) {
	t.Parallel()
	store := &fakeStore{}
	h, _, sent := newTestHandler(t, store)
	update := map[string]any{
		"update_id": 7,
		"message": map[string]any{
			"text": "/announcements on",
			"chat": map[string]any{"id": 222},
		},
	}
	rr := postUpdate(t, h, "s3cret", update)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d want 200", rr.Code)
	}
	h.WaitForTest() // drain async processing (test-only helper)
	if store.announceOptInCalls != 1 || store.announceChatID != 222 || store.announceOptIn != true {
		t.Fatalf("expected SetAnnouncements(chat=222, optIn=true), got %+v", store)
	}
	if len(*sent) == 0 {
		t.Fatalf("expected a confirmation message to be sent")
	}
	text, _ := (*sent)[0]["text"].(string)
	if !strings.Contains(text, "ON") {
		t.Fatalf("announcements-on reply missing expected content: %q", text)
	}
}

func TestWebhook_StopOne(t *testing.T) {
	t.Parallel()
	store := &fakeStore{}
	h, _, sent := newTestHandler(t, store)
	update := map[string]any{
		"update_id": 3,
		"message": map[string]any{
			"text": "/stop 2",
			"chat": map[string]any{"id": 777},
		},
	}
	rr := postUpdate(t, h, "s3cret", update)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d want 200", rr.Code)
	}
	h.WaitForTest() // drain async processing (test-only helper)
	if store.stopOneChatID != 777 || store.stopOneIndex != 2 {
		t.Fatalf("expected StopOne(chat=777, index=2), got %+v", store)
	}
	if store.stoppedChatID != 0 {
		t.Fatalf("expected Stop (all) not to be called, got chat %d", store.stoppedChatID)
	}
	if len(*sent) == 0 {
		t.Fatalf("expected a confirmation message to be sent")
	}
}
