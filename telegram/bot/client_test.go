package bot

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientSendMessage(t *testing.T) {
	t.Parallel()
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	c := NewClient("TESTTOKEN")
	c.baseURL = srv.URL // override for test
	err := c.SendMessage(context.Background(), 4242, "hello")
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if gotPath != "/botTESTTOKEN/sendMessage" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotBody["chat_id"].(float64) != 4242 || gotBody["text"].(string) != "hello" {
		t.Fatalf("body = %v", gotBody)
	}
	if gotBody["parse_mode"] != "HTML" {
		t.Fatalf("parse_mode = %v, want HTML", gotBody["parse_mode"])
	}
}

// TestClientSendMessage_RetriesPlainOnParseError covers the degrade-to-
// plain-text path: if Telegram rejects the HTML payload with a "can't parse"
// error, the client retries once with the same text and no parse_mode, so a
// formatting bug loses formatting instead of losing the message entirely.
func TestClientSendMessage_RetriesPlainOnParseError(t *testing.T) {
	t.Parallel()
	var bodies []map[string]any
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		bodies = append(bodies, m)
		if calls == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"ok":false,"description":"Bad Request: can't parse entities: unsupported tag"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	c := NewClient("TESTTOKEN")
	c.baseURL = srv.URL
	err := c.SendMessage(context.Background(), 4242, "hello <b>world</b>")
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2 (initial + retry)", calls)
	}
	if bodies[0]["parse_mode"] != "HTML" {
		t.Fatalf("first call parse_mode = %v, want HTML", bodies[0]["parse_mode"])
	}
	if _, ok := bodies[1]["parse_mode"]; ok {
		t.Fatalf("retry should omit parse_mode, got %v", bodies[1]["parse_mode"])
	}
	if bodies[1]["text"] != "hello <b>world</b>" {
		t.Fatalf("retry text = %v, want unchanged", bodies[1]["text"])
	}
}
