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
}
