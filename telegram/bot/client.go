package bot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	token   string
	baseURL string
	http    *http.Client
}

func NewClient(botToken string) *Client {
	return &Client{
		token:   botToken,
		baseURL: "https://api.telegram.org",
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *Client) apiURL(method string) string {
	return fmt.Sprintf("%s/bot%s/%s", c.baseURL, c.token, method)
}

// SendMessage sends a plain-text message to a chat. parse_mode is omitted so
// the text is delivered literally (no markdown escaping needed).
func (c *Client) SendMessage(ctx context.Context, chatID int64, text string) error {
	payload, _ := json.Marshal(map[string]any{"chat_id": chatID, "text": text})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiURL("sendMessage"), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("telegram sendMessage status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
