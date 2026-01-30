package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/config"
)

// OnSlackInstallationChange is called when a Slack installation is created, updated, or removed.
// Set this to invalidate cached clients in the ClientManager.
var OnSlackInstallationChange func(teamID string)

// isSlackTeamAllowed checks if a Slack team ID is permitted to install the app.
// If SLACK_ALLOWED_TEAM_IDS is not set, all teams are allowed.
func isSlackTeamAllowed(teamID string) bool {
	allowed := os.Getenv("SLACK_ALLOWED_TEAM_IDS")
	if allowed == "" {
		return true
	}
	for _, id := range strings.Split(allowed, ",") {
		if strings.TrimSpace(id) == teamID {
			return true
		}
	}
	return false
}

// SlackInstallation represents a Slack workspace installation
type SlackInstallation struct {
	ID          string    `json:"id"`
	TeamID      string    `json:"team_id"`
	TeamName    *string   `json:"team_name,omitempty"`
	BotToken    string    `json:"-"`
	BotUserID   string    `json:"bot_user_id"`
	Scope       *string   `json:"scope,omitempty"`
	InstalledBy *string   `json:"installed_by,omitempty"`
	IsActive    bool      `json:"is_active"`
	InstalledAt time.Time `json:"installed_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// CreateOAuthState creates a new OAuth state token tied to an account
func CreateOAuthState(ctx context.Context, accountID string) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate state: %w", err)
	}
	state := hex.EncodeToString(b)

	_, err := config.PgPool.Exec(ctx,
		`INSERT INTO slack_oauth_states (state, account_id) VALUES ($1, $2)`,
		state, accountID,
	)
	if err != nil {
		return "", fmt.Errorf("failed to store oauth state: %w", err)
	}

	return state, nil
}

// ValidateOAuthState validates and consumes an OAuth state token, returning the account ID
func ValidateOAuthState(ctx context.Context, state string) (string, error) {
	var accountID string
	err := config.PgPool.QueryRow(ctx,
		`DELETE FROM slack_oauth_states WHERE state = $1 AND expires_at > NOW() RETURNING account_id`,
		state,
	).Scan(&accountID)
	if err != nil {
		return "", fmt.Errorf("invalid or expired oauth state: %w", err)
	}
	return accountID, nil
}

// UpsertSlackInstallation creates or updates a Slack installation
func UpsertSlackInstallation(ctx context.Context, teamID, teamName, botToken, botUserID, scope, installedBy string) error {
	_, err := config.PgPool.Exec(ctx,
		`INSERT INTO slack_installations (team_id, team_name, bot_token, bot_user_id, scope, installed_by, is_active, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, true, NOW())
		 ON CONFLICT (team_id) DO UPDATE SET
		   team_name = EXCLUDED.team_name,
		   bot_token = EXCLUDED.bot_token,
		   bot_user_id = EXCLUDED.bot_user_id,
		   scope = EXCLUDED.scope,
		   installed_by = EXCLUDED.installed_by,
		   is_active = true,
		   updated_at = NOW()`,
		teamID, teamName, botToken, botUserID, scope, installedBy,
	)
	return err
}

// DeactivateSlackInstallation deactivates a Slack installation by team ID
func DeactivateSlackInstallation(ctx context.Context, teamID string) error {
	_, err := config.PgPool.Exec(ctx,
		`UPDATE slack_installations SET is_active = false, updated_at = NOW() WHERE team_id = $1`,
		teamID,
	)
	return err
}

// ListSlackInstallations returns active installations for a specific account
func ListSlackInstallations(ctx context.Context, accountID string) ([]SlackInstallation, error) {
	rows, err := config.PgPool.Query(ctx,
		`SELECT id, team_id, team_name, bot_user_id, scope, installed_by, is_active, installed_at, updated_at
		 FROM slack_installations WHERE is_active = true AND installed_by = $1 ORDER BY installed_at DESC`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var installations []SlackInstallation
	for rows.Next() {
		var inst SlackInstallation
		if err := rows.Scan(&inst.ID, &inst.TeamID, &inst.TeamName, &inst.BotUserID, &inst.Scope, &inst.InstalledBy, &inst.IsActive, &inst.InstalledAt, &inst.UpdatedAt); err != nil {
			return nil, err
		}
		installations = append(installations, inst)
	}
	return installations, rows.Err()
}

// GetSlackInstallationByTeamID returns an active installation by team ID
func GetSlackInstallationByTeamID(ctx context.Context, teamID string) (*SlackInstallation, error) {
	var inst SlackInstallation
	err := config.PgPool.QueryRow(ctx,
		`SELECT id, team_id, team_name, bot_token, bot_user_id, scope, installed_by, is_active, installed_at, updated_at
		 FROM slack_installations WHERE team_id = $1 AND is_active = true`,
		teamID,
	).Scan(&inst.ID, &inst.TeamID, &inst.TeamName, &inst.BotToken, &inst.BotUserID, &inst.Scope, &inst.InstalledBy, &inst.IsActive, &inst.InstalledAt, &inst.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &inst, nil
}

// GetSlackOAuthStart initiates the Slack OAuth flow
func GetSlackOAuthStart(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	clientID := os.Getenv("SLACK_CLIENT_ID")
	if clientID == "" {
		http.Error(w, "Slack OAuth not configured", http.StatusInternalServerError)
		return
	}

	state, err := CreateOAuthState(r.Context(), account.ID.String())
	if err != nil {
		slog.Error("failed to create oauth state", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	scopes := "app_mentions:read,channels:history,channels:read,chat:write,groups:history,groups:read,im:history,im:read,mpim:history,reactions:write,users:read"
	redirectURI := getSlackRedirectURI(r)

	authURL := fmt.Sprintf(
		"https://slack.com/oauth/v2/authorize?client_id=%s&scope=%s&state=%s&redirect_uri=%s",
		clientID, scopes, state, redirectURI,
	)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"url": authURL})
}

// getSlackRedirectURI returns the OAuth callback URI.
// Uses SLACK_REDIRECT_URL env var if set, otherwise derives from the request.
func getSlackRedirectURI(r *http.Request) string {
	if redirectURL := os.Getenv("SLACK_REDIRECT_URL"); redirectURL != "" {
		return redirectURL
	}
	scheme := "https"
	if r.TLS == nil {
		if fwd := r.Header.Get("X-Forwarded-Proto"); fwd != "" {
			scheme = fwd
		} else {
			scheme = "http"
		}
	}
	host := r.Host
	return fmt.Sprintf("%s://%s/api/slack/oauth/callback", scheme, host)
}

// settingsRedirect redirects to the settings page, using WEB_BASE_URL if set.
func settingsRedirect(w http.ResponseWriter, r *http.Request, query string) {
	base := os.Getenv("WEB_BASE_URL")
	if base == "" {
		http.Redirect(w, r, "/settings?"+query, http.StatusFound)
		return
	}
	http.Redirect(w, r, strings.TrimSuffix(base, "/")+"/settings?"+query, http.StatusFound)
}

// GetSlackOAuthCallback handles the Slack OAuth callback
func GetSlackOAuthCallback(w http.ResponseWriter, r *http.Request) {
	errParam := r.URL.Query().Get("error")
	if errParam != "" {
		slog.Warn("slack oauth error", "error", errParam)
		settingsRedirect(w, r, "slack=error&reason="+errParam)
		return
	}

	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")
	if code == "" || state == "" {
		settingsRedirect(w, r, "slack=error&reason=missing_params")
		return
	}

	// Validate state
	accountID, err := ValidateOAuthState(r.Context(), state)
	if err != nil {
		slog.Warn("invalid oauth state", "error", err)
		settingsRedirect(w, r, "slack=error&reason=invalid_state")
		return
	}

	// Exchange code for token
	clientID := os.Getenv("SLACK_CLIENT_ID")
	clientSecret := os.Getenv("SLACK_CLIENT_SECRET")
	redirectURI := getSlackRedirectURI(r)

	tokenResp, err := exchangeSlackCode(r.Context(), clientID, clientSecret, code, redirectURI)
	if err != nil {
		slog.Error("failed to exchange slack code", "error", err)
		settingsRedirect(w, r, "slack=error&reason=token_exchange")
		return
	}

	if !tokenResp.OK {
		slog.Error("slack oauth token response not ok", "error", tokenResp.Error)
		settingsRedirect(w, r, "slack=error&reason="+tokenResp.Error)
		return
	}

	// Check workspace allowlist
	if !isSlackTeamAllowed(tokenResp.Team.ID) {
		slog.Warn("slack workspace not in allowlist", "team_id", tokenResp.Team.ID, "team_name", tokenResp.Team.Name)
		settingsRedirect(w, r, "slack=error&reason=workspace_not_allowed")
		return
	}

	// Check if this workspace is already installed by someone else
	existing, _ := GetSlackInstallationByTeamID(r.Context(), tokenResp.Team.ID)
	if existing != nil && existing.InstalledBy != nil && *existing.InstalledBy != accountID {
		// Store as pending and ask user to confirm the takeover
		pendingID, err := CreatePendingInstallation(r.Context(), accountID, tokenResp)
		if err != nil {
			slog.Error("failed to create pending installation", "error", err)
			settingsRedirect(w, r, "slack=error&reason=storage")
			return
		}
		teamName := tokenResp.Team.Name
		if teamName == "" {
			teamName = tokenResp.Team.ID
		}
		settingsRedirect(w, r, "slack=confirm_takeover&team="+teamName+"&pending_id="+pendingID)
		return
	}

	// Store installation (upserts on team_id)
	err = UpsertSlackInstallation(
		r.Context(),
		tokenResp.Team.ID,
		tokenResp.Team.Name,
		tokenResp.AccessToken,
		tokenResp.BotUserID,
		tokenResp.Scope,
		accountID,
	)
	if err != nil {
		slog.Error("failed to store slack installation", "error", err)
		settingsRedirect(w, r, "slack=error&reason=storage")
		return
	}

	slog.Info("slack installation created", "team_id", tokenResp.Team.ID, "team_name", tokenResp.Team.Name, "account_id", accountID)
	if OnSlackInstallationChange != nil {
		OnSlackInstallationChange(tokenResp.Team.ID)
	}
	settingsRedirect(w, r, "slack=installed")
}

// slackOAuthResponse represents the response from Slack's oauth.v2.access endpoint
type slackOAuthResponse struct {
	OK          bool   `json:"ok"`
	Error       string `json:"error,omitempty"`
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	Scope       string `json:"scope"`
	BotUserID   string `json:"bot_user_id"`
	Team        struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"team"`
}

func exchangeSlackCode(ctx context.Context, clientID, clientSecret, code, redirectURI string) (*slackOAuthResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://slack.com/api/oauth.v2.access", nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	q.Set("client_id", clientID)
	q.Set("client_secret", clientSecret)
	q.Set("code", code)
	q.Set("redirect_uri", redirectURI)
	req.URL.RawQuery = q.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var tokenResp slackOAuthResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, err
	}
	return &tokenResp, nil
}

// GetSlackInstallations returns active Slack installations
func GetSlackInstallations(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	installations, err := ListSlackInstallations(r.Context(), account.ID.String())
	if err != nil {
		slog.Error("failed to list slack installations", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	if installations == nil {
		installations = []SlackInstallation{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(installations)
}

// DeleteSlackInstallation uninstalls the app from the Slack workspace and deactivates the installation
func DeleteSlackInstallation(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	teamID := chi.URLParam(r, "team_id")
	if teamID == "" {
		http.Error(w, "team_id is required", http.StatusBadRequest)
		return
	}

	// Fetch the installation to get the bot token for uninstall
	inst, err := GetSlackInstallationByTeamID(r.Context(), teamID)
	if err != nil {
		slog.Error("failed to get slack installation for uninstall", "error", err, "team_id", teamID)
		http.Error(w, "Installation not found", http.StatusNotFound)
		return
	}

	// Only the installer can disconnect
	if inst.InstalledBy == nil || *inst.InstalledBy != account.ID.String() {
		http.Error(w, "Not authorized to disconnect this installation", http.StatusForbidden)
		return
	}

	// Uninstall the app from the Slack workspace
	clientID := os.Getenv("SLACK_CLIENT_ID")
	clientSecret := os.Getenv("SLACK_CLIENT_SECRET")
	if clientID != "" && clientSecret != "" {
		if err := uninstallSlackApp(r.Context(), inst.BotToken, clientID, clientSecret); err != nil {
			slog.Warn("failed to uninstall slack app from workspace (continuing with deactivation)", "error", err, "team_id", teamID)
		}
	}

	if err := DeactivateSlackInstallation(r.Context(), teamID); err != nil {
		slog.Error("failed to deactivate slack installation", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	if OnSlackInstallationChange != nil {
		OnSlackInstallationChange(teamID)
	}
	slog.Info("slack installation removed", "team_id", teamID, "by_account", account.ID.String())
	w.WriteHeader(http.StatusNoContent)
}

// CreatePendingInstallation stores token data for a pending takeover confirmation
func CreatePendingInstallation(ctx context.Context, accountID string, tokenResp *slackOAuthResponse) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate pending id: %w", err)
	}
	pendingID := hex.EncodeToString(b)

	_, err := config.PgPool.Exec(ctx,
		`INSERT INTO slack_pending_installations (id, account_id, team_id, team_name, bot_token, bot_user_id, scope)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		pendingID, accountID, tokenResp.Team.ID, tokenResp.Team.Name, tokenResp.AccessToken, tokenResp.BotUserID, tokenResp.Scope,
	)
	if err != nil {
		return "", fmt.Errorf("failed to store pending installation: %w", err)
	}
	return pendingID, nil
}

// ConfirmSlackInstallation confirms a pending takeover installation
func ConfirmSlackInstallation(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	pendingID := chi.URLParam(r, "pending_id")
	if pendingID == "" {
		http.Error(w, "pending_id is required", http.StatusBadRequest)
		return
	}

	// Consume the pending installation (must belong to this account and not be expired)
	var teamID, teamName, botToken, botUserID, scope string
	err := config.PgPool.QueryRow(r.Context(),
		`DELETE FROM slack_pending_installations
		 WHERE id = $1 AND account_id = $2 AND expires_at > NOW()
		 RETURNING team_id, team_name, bot_token, bot_user_id, scope`,
		pendingID, account.ID.String(),
	).Scan(&teamID, &teamName, &botToken, &botUserID, &scope)
	if err != nil {
		slog.Warn("invalid or expired pending installation", "error", err, "pending_id", pendingID)
		http.Error(w, "Pending installation not found or expired", http.StatusNotFound)
		return
	}

	// Upsert the installation
	err = UpsertSlackInstallation(r.Context(), teamID, teamName, botToken, botUserID, scope, account.ID.String())
	if err != nil {
		slog.Error("failed to store slack installation from pending", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	slog.Info("slack installation confirmed (takeover)", "team_id", teamID, "team_name", teamName, "account_id", account.ID.String())
	if OnSlackInstallationChange != nil {
		OnSlackInstallationChange(teamID)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "installed", "team_id": teamID, "team_name": teamName})
}

// uninstallSlackApp calls apps.uninstall to fully remove the app from a workspace
func uninstallSlackApp(ctx context.Context, botToken, clientID, clientSecret string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://slack.com/api/apps.uninstall", nil)
	if err != nil {
		return err
	}
	q := req.URL.Query()
	q.Set("client_id", clientID)
	q.Set("client_secret", clientSecret)
	req.URL.RawQuery = q.Encode()
	req.Header.Set("Authorization", "Bearer "+botToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var result struct {
		OK    bool   `json:"ok"`
		Error string `json:"error,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}
	if !result.OK {
		return fmt.Errorf("slack apps.uninstall failed: %s", result.Error)
	}
	return nil
}
