package bot

import (
	"crypto/subtle"
	"net/http"
)

// VerifyTelegramSecret constant-time compares Telegram's secret-token header
// (set at setWebhook time) against the expected value.
func VerifyTelegramSecret(r *http.Request, expected string) bool {
	if expected == "" {
		return false
	}
	got := r.Header.Get("X-Telegram-Bot-Api-Secret-Token")
	return subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}
