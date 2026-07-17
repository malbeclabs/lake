package bot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"strings"
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

// SendMessage sends a message to a chat using Telegram's HTML parse mode, so
// callers can use <b>, <pre>, etc. for layout. Callers must escape any
// dynamic value they interpolate (see htmlEscape) so it can't be misread as a
// tag.
//
// If Telegram rejects the message because of a formatting bug (a "can't
// parse" error), SendMessage retries once with the same text but no
// parse_mode, so a bad tag degrades to plain text instead of the message
// being dropped entirely.
func (c *Client) SendMessage(ctx context.Context, chatID int64, text string) error {
	err := c.sendMessage(ctx, chatID, text, "HTML")
	if err == nil {
		return nil
	}
	var sErr *sendError
	if errors.As(err, &sErr) && sErr.looksLikeParseError() {
		return c.sendMessage(ctx, chatID, text, "")
	}
	return err
}

// sendError carries the status and body of a non-OK Telegram response so
// SendMessage can decide whether the failure was a parse-mode issue worth
// retrying without parse_mode.
type sendError struct {
	status int
	body   string
}

func (e *sendError) Error() string {
	return fmt.Sprintf("telegram sendMessage status %d: %s", e.status, e.body)
}

func (e *sendError) looksLikeParseError() bool {
	return strings.Contains(e.body, "can't parse")
}

func (c *Client) sendMessage(ctx context.Context, chatID int64, text, parseMode string) error {
	payload := map[string]any{"chat_id": chatID, "text": text}
	if parseMode != "" {
		payload["parse_mode"] = parseMode
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiURL("sendMessage"), bytes.NewReader(body))
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
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &sendError{status: resp.StatusCode, body: string(b)}
	}
	return nil
}

// htmlEscape escapes &, <, and > so a dynamic value (seat pk, device code,
// IP, username, etc.) can be safely interpolated into an HTML-parse-mode
// Telegram message without being misread as markup.
func htmlEscape(s string) string {
	return html.EscapeString(s)
}
