// Package bot implements a minimal Telegram bot for DoubleZero seat-expiry
// notifications: webhook-based activation and outbound alert delivery.
package bot

import (
	"fmt"
	"os"
)

type Config struct {
	BotToken      string
	WebhookSecret string
	BotUsername   string
	WebBaseURL    string
}

func LoadConfig() (Config, error) {
	c := Config{
		BotToken:      os.Getenv("TELEGRAM_BOT_TOKEN"),
		WebhookSecret: os.Getenv("TELEGRAM_WEBHOOK_SECRET"),
		BotUsername:   os.Getenv("TELEGRAM_BOT_USERNAME"),
		WebBaseURL:    os.Getenv("WEB_BASE_URL"),
	}
	if c.BotToken == "" {
		return Config{}, fmt.Errorf("TELEGRAM_BOT_TOKEN is required")
	}
	if c.WebhookSecret == "" {
		return Config{}, fmt.Errorf("TELEGRAM_WEBHOOK_SECRET is required")
	}
	return c, nil
}
