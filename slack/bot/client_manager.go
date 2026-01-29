package bot

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const clientCacheTTL = 5 * time.Minute

// Installation represents a stored Slack installation (mirrors handlers.SlackInstallation fields needed here)
type Installation struct {
	TeamID    string
	TeamName  string
	BotToken  string
	BotUserID string
}

// InstallationStore provides access to stored Slack installations
type InstallationStore interface {
	GetSlackInstallation(ctx context.Context, teamID string) (*Installation, error)
}

type cachedClient struct {
	client   *Client
	cachedAt time.Time
}

// ClientManager manages per-workspace Slack clients
type ClientManager struct {
	clients map[string]*cachedClient // team_id -> cached client
	mu      sync.RWMutex
	store   InstallationStore
	log     *slog.Logger
}

// NewClientManager creates a new client manager
func NewClientManager(store InstallationStore, log *slog.Logger) *ClientManager {
	return &ClientManager{
		clients: make(map[string]*cachedClient),
		store:   store,
		log:     log,
	}
}

// GetClient returns a cached client for the given team, or loads one from the store
func (cm *ClientManager) GetClient(ctx context.Context, teamID string) (*Client, error) {
	cm.mu.RLock()
	if cc, ok := cm.clients[teamID]; ok && time.Since(cc.cachedAt) < clientCacheTTL {
		cm.mu.RUnlock()
		return cc.client, nil
	}
	cm.mu.RUnlock()

	// Load from store (cache miss or expired)
	inst, err := cm.store.GetSlackInstallation(ctx, teamID)
	if err != nil {
		return nil, err
	}

	client := NewClient(inst.BotToken, "", cm.log)
	client.botUserID = inst.BotUserID

	cm.mu.Lock()
	// Check again in case another goroutine loaded it
	if cc, ok := cm.clients[teamID]; ok && time.Since(cc.cachedAt) < clientCacheTTL {
		cm.mu.Unlock()
		return cc.client, nil
	}
	cm.clients[teamID] = &cachedClient{client: client, cachedAt: time.Now()}
	cm.mu.Unlock()

	cm.log.Info("loaded slack client for team", "team_id", teamID)
	return client, nil
}

// InvalidateClient removes a cached client
func (cm *ClientManager) InvalidateClient(teamID string) {
	cm.mu.Lock()
	delete(cm.clients, teamID)
	cm.mu.Unlock()
	cm.log.Info("invalidated slack client cache", "team_id", teamID)
}
