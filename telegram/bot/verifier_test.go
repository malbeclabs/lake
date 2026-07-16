package bot

import (
	"net/http/httptest"
	"testing"
)

func TestVerifyTelegramSecret(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("POST", "/telegram/webhook", nil)
	r.Header.Set("X-Telegram-Bot-Api-Secret-Token", "s3cret")
	if !VerifyTelegramSecret(r, "s3cret") {
		t.Fatal("expected valid secret to pass")
	}
	if VerifyTelegramSecret(r, "wrong") {
		t.Fatal("expected wrong secret to fail")
	}
	if VerifyTelegramSecret(r, "") {
		t.Fatal("expected empty expected-secret to fail")
	}
}
