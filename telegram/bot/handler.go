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
	// SetAnnouncements toggles the announcements_opt_in flag for the chat's
	// contact, separate from its seat alerts. Returns ok=false if the chat has
	// no contact yet (no alert has ever been activated there).
	SetAnnouncements(ctx context.Context, chatID int64, optIn bool) (ok bool, err error)
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
		h.send(ctx, chatID, fmt.Sprintf(startConfirmedTmpl, htmlEscape(seat)))
	case strings.HasPrefix(text, "/list"):
		seats, err := h.store.List(ctx, chatID)
		if err != nil || len(seats) == 0 {
			h.send(ctx, chatID, "You have no active alerts.")
			return
		}
		lines := make([]string, len(seats))
		for i, s := range seats {
			lines[i] = fmt.Sprintf("%d. %s", i+1, htmlEscape(s))
		}
		h.send(ctx, chatID, "<b>Your active alerts:</b>\n"+strings.Join(lines, "\n"))
	case strings.HasPrefix(text, "/stop"):
		arg := strings.TrimSpace(strings.TrimPrefix(text, "/stop"))
		switch {
		case arg == "" || arg == "all":
			n, _ := h.store.Stop(ctx, chatID)
			h.send(ctx, chatID, fmt.Sprintf("Stopped %d alert(s). You won't get further messages.", n))
		default:
			n, err := strconv.Atoi(arg)
			if err != nil || n <= 0 {
				h.send(ctx, chatID, "Use /stop [number] (see /list), or /stop all.")
				return
			}
			desc, ok, err := h.store.StopOne(ctx, chatID, n)
			if err != nil || !ok {
				h.send(ctx, chatID, fmt.Sprintf("No alert #%d. Use /list to see your alerts.", n))
				return
			}
			h.send(ctx, chatID, fmt.Sprintf("Stopped alert #%d: %s", n, htmlEscape(desc)))
		}
	case strings.HasPrefix(text, "/help"):
		h.send(ctx, chatID, helpText)
	case strings.HasPrefix(text, "/topup"):
		h.send(ctx, chatID, topupText)
	case strings.HasPrefix(text, "/announcements"):
		arg := strings.TrimSpace(strings.TrimPrefix(text, "/announcements"))
		switch arg {
		case "on":
			ok, err := h.store.SetAnnouncements(ctx, chatID, true)
			if err != nil {
				h.log.Warn("set announcements opt-in failed", "error", err)
				h.send(ctx, chatID, "Something went wrong, please try again.")
				return
			}
			if !ok {
				h.send(ctx, chatID, "You have no active alerts here yet — activate one first, then you can manage announcements.")
				return
			}
			h.send(ctx, chatID, "Product update messages: ON. I'll only message you when we have something worth sharing.")
		case "off":
			ok, err := h.store.SetAnnouncements(ctx, chatID, false)
			if err != nil {
				h.log.Warn("set announcements opt-in failed", "error", err)
				h.send(ctx, chatID, "Something went wrong, please try again.")
				return
			}
			if !ok {
				h.send(ctx, chatID, "You have no active alerts here yet — activate one first, then you can manage announcements.")
				return
			}
			h.send(ctx, chatID, "Product update messages: OFF. You'll still get your seat alerts.")
		default:
			h.send(ctx, chatID, "Use /announcements on or /announcements off.")
		}
	default:
		h.send(ctx, chatID, "Commands: /list, /stop, /topup, /help. To add an alert, use the DoubleZero site.")
	}
}

// startConfirmedTmpl is the /start success reply. %s is the htmlEscape'd seat
// description returned by Store.Activate.
const startConfirmedTmpl = "✅ <b>Connected</b>\n\nI'll watch <b>%s</b>\n\nCommands: /list · /stop · /topup · /help"

const helpText = `<b>DoubleZero Seat Alerts</b>

I warn you before a shred seat runs out of prepaid escrow, so you don't lose the seat or its tenure. Create alerts on the DoubleZero site (the bell on a seat), then activate them here.

<pre>/list                  your active alerts
/stop [n]              stop alert number n
/stop all             stop every alert
/announcements on|off  product updates (on)
/topup                how to top up a seat
/help                 this message</pre>`

const topupText = `<b>Topping up a seat's escrow</b>

Run this with the wallet that funds the seat:
<pre>doublezero-solana shreds pay --device-code [CODE] --client-ip [IP] --amount [USDC]</pre>
[CODE] — the device your seat is on
[IP] — your machine's public IP (curl -4 ifconfig.me)
[USDC] — amount to add; at least one epoch's price (more is better)

Check the price:
<pre>doublezero-solana shreds price --device-code [CODE]</pre>
Check your balance:
<pre>doublezero-solana shreds list --client-ip [IP]</pre>

Re-running just adds to your existing escrow.`

func (h *EventHandler) send(ctx context.Context, chatID int64, text string) {
	if err := h.client.SendMessage(ctx, chatID, text); err != nil {
		h.log.Warn("telegram send failed", "error", err)
	}
}
