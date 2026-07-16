package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// TelegramSender is the minimal outbound interface the handlers need. The real
// implementation is the telegram bot client, injected in main.
type TelegramSender interface {
	SendMessage(ctx context.Context, chatID int64, text string) error
}

type createSeatAlertRequest struct {
	SeatPK             string  `json:"seat_pk"`
	TriggerType        string  `json:"trigger_type"`
	ThresholdValue     float64 `json:"threshold_value"`
	AnnouncementsOptIn bool    `json:"announcements_opt_in"`
}

func validTrigger(t string) bool { return t == "epochs_left" || t == "balance_below_usdc" }

func (a *API) CreateSeatAlertHandler(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}
	var req createSeatAlertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.SeatPK == "" || !validTrigger(req.TriggerType) || req.ThresholdValue < 0 {
		http.Error(w, "seat_pk, a valid trigger_type, and a non-negative threshold_value are required", http.StatusBadRequest)
		return
	}
	alert, err := a.CreateSeatAlert(r.Context(), account.ID, req.SeatPK, req.TriggerType, req.ThresholdValue, req.AnnouncementsOptIn)
	if err != nil {
		http.Error(w, internalError("Failed to create alert", err), http.StatusInternalServerError)
		return
	}
	botUser := os.Getenv("TELEGRAM_BOT_USERNAME")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":                 alert.ID,
		"activation_token":   alert.ActivationToken,
		"telegram_deep_link": fmt.Sprintf("https://t.me/%s?start=%s", botUser, alert.ActivationToken),
		"status":             alert.Status,
	})
}

func (a *API) ListMySeatAlertsHandler(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}
	items, err := a.ListSeatAlertsByAccount(r.Context(), account.ID)
	if err != nil {
		http.Error(w, internalError("Failed to list alerts", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"items": items})
}

func (a *API) DeleteSeatAlertHandler(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "Invalid id", http.StatusBadRequest)
		return
	}
	if err := a.DeleteSeatAlert(r.Context(), id, account.ID); err != nil {
		http.Error(w, "Alert not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) SendTestAlertHandler(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}
	if a.TelegramSender == nil {
		http.Error(w, "Telegram is not configured", http.StatusServiceUnavailable)
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "Invalid id", http.StatusBadRequest)
		return
	}
	chatID, ok, err := a.chatIDForAlert(r.Context(), id, account.ID)
	if err != nil {
		http.Error(w, internalError("Failed to look up alert", err), http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "Alert not found or not activated", http.StatusNotFound)
		return
	}
	if err := a.TelegramSender.SendMessage(r.Context(), chatID,
		"Test alert: this is what a low-balance warning will look like."); err != nil {
		http.Error(w, "Failed to send test message", http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
