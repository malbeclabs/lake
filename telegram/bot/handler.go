package bot

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// Store is the channel-agnostic persistence the bot needs. The lake api adapts
// its handlers.API to this interface (see api/main.go startTelegramBot) so this
// package does not import handlers (avoids an import cycle).
type Store interface {
	// Activate links a chat to a pending alert identified by token; returns a
	// short human description of the seat for the confirmation message.
	Activate(ctx context.Context, token string, chatID int64, username string) (seatDesc string, err error)
	List(ctx context.Context, chatID int64) (seatDescs []string, err error)
	Stop(ctx context.Context, chatID int64) (stopped int64, err error)
	// StopOne stops a single alert, identified by its 1-based position in the
	// order List returns (the same order/numbering shown to the user).
	StopOne(ctx context.Context, chatID int64, index int) (desc string, ok bool, err error)
}

type EventHandler struct {
	client *Client
	store  Store
	secret string
	log    *slog.Logger

	mu   sync.Mutex
	seen map[int64]bool // update_id dedup
	wg   sync.WaitGroup // test-only drain
}

func NewEventHandler(client *Client, store Store, secret string, log *slog.Logger) *EventHandler {
	return &EventHandler{client: client, store: store, secret: secret, log: log, seen: map[int64]bool{}}
}

// WaitForTest drains in-flight async processing. Intended for tests only.
func (h *EventHandler) WaitForTest() { h.wg.Wait() }

type tgUpdate struct {
	UpdateID int64 `json:"update_id"`
	Message  struct {
		Text string `json:"text"`
		Chat struct {
			ID int64 `json:"id"`
		} `json:"chat"`
		From struct {
			Username string `json:"username"`
		} `json:"from"`
	} `json:"message"`
}

func (h *EventHandler) HandleHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !VerifyTelegramSecret(r, h.secret) {
		h.log.Warn("invalid Telegram webhook secret")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var u tgUpdate
	if err := json.Unmarshal(body, &u); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Dedup on update_id (Telegram retries on non-2xx).
	h.mu.Lock()
	dup := h.seen[u.UpdateID]
	if !dup {
		h.seen[u.UpdateID] = true
	}
	h.mu.Unlock()

	w.WriteHeader(http.StatusOK) // ack fast
	if dup || u.Message.Chat.ID == 0 {
		return
	}
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.process(context.Background(), u)
	}()
}

func (h *EventHandler) process(ctx context.Context, u tgUpdate) {
	chatID := u.Message.Chat.ID
	text := strings.TrimSpace(u.Message.Text)
	switch {
	case strings.HasPrefix(text, "/start"):
		token := strings.TrimSpace(strings.TrimPrefix(text, "/start"))
		if token == "" {
			h.send(ctx, chatID, "Open your alert on the DoubleZero site and tap Activate to connect.")
			return
		}
		seat, err := h.store.Activate(ctx, token, chatID, u.Message.From.Username)
		if err != nil {
			h.send(ctx, chatID, "That activation link is invalid or already used. Create a new alert on the site.")
			return
		}
		h.send(ctx, chatID, fmt.Sprintf("Connected. I'll warn you when seat %s runs low. Reply /list or /stop anytime.", seat))
	case strings.HasPrefix(text, "/list"):
		seats, err := h.store.List(ctx, chatID)
		if err != nil || len(seats) == 0 {
			h.send(ctx, chatID, "You have no active alerts.")
			return
		}
		lines := make([]string, len(seats))
		for i, s := range seats {
			lines[i] = fmt.Sprintf("%d. %s", i+1, s)
		}
		h.send(ctx, chatID, "Your active alerts:\n"+strings.Join(lines, "\n"))
	case strings.HasPrefix(text, "/stop"):
		arg := strings.TrimSpace(strings.TrimPrefix(text, "/stop"))
		switch {
		case arg == "" || arg == "all":
			n, _ := h.store.Stop(ctx, chatID)
			h.send(ctx, chatID, fmt.Sprintf("Stopped %d alert(s). You won't get further messages.", n))
		default:
			n, err := strconv.Atoi(arg)
			if err != nil || n <= 0 {
				h.send(ctx, chatID, "Use /stop <number> (see /list), or /stop all.")
				return
			}
			desc, ok, err := h.store.StopOne(ctx, chatID, n)
			if err != nil || !ok {
				h.send(ctx, chatID, fmt.Sprintf("No alert #%d. Use /list to see your alerts.", n))
				return
			}
			h.send(ctx, chatID, fmt.Sprintf("Stopped alert #%d: %s", n, desc))
		}
	default:
		h.send(ctx, chatID, "Commands: /list, /stop. To add an alert, use the DoubleZero site.")
	}
}

func (h *EventHandler) send(ctx context.Context, chatID int64, text string) {
	if err := h.client.SendMessage(ctx, chatID, text); err != nil {
		h.log.Warn("telegram send failed", "error", err)
	}
}
