package validatorsapp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client defines the interface for fetching validator data from validators.app.
type Client interface {
	GetValidators(ctx context.Context) ([]Validator, error)
}

// apiValidator is the raw JSON representation returned by the validators.app API.
type apiValidator struct {
	Account                string   `json:"account"`
	Name                   *string  `json:"name"`
	VoteAccount            string   `json:"vote_account"`
	SoftwareVersion        string   `json:"software_version"`
	SoftwareClient         string   `json:"software_client"`
	SoftwareClientID       uint16   `json:"software_client_id"`
	Jito                   bool     `json:"jito"`
	JitoCommission         *uint32  `json:"jito_commission"`
	IsActive               bool     `json:"is_active"`
	IsDZ                   bool     `json:"is_dz"`
	ActiveStake            uint64   `json:"active_stake"`
	Commission             uint8    `json:"commission"`
	Delinquent             bool     `json:"delinquent"`
	Epoch                  uint64   `json:"epoch"`
	EpochCredits           uint64   `json:"epoch_credits"`
	SkippedSlotPercent     *string  `json:"skipped_slot_percent"`
	TotalScore             int16    `json:"total_score"`
	DataCenterKey          string   `json:"data_center_key"`
	AutonomousSystemNumber uint32   `json:"autonomous_system_number"`
	Latitude               string   `json:"latitude"`
	Longitude              string   `json:"longitude"`
	IP                     string   `json:"ip"`
	StakePoolsList         []string `json:"stake_pools_list"`
}

// toValidator converts an API response entry into our domain model.
func (a *apiValidator) toValidator() Validator {
	v := Validator{
		Account:                a.Account,
		VoteAccount:            a.VoteAccount,
		SoftwareVersion:        a.SoftwareVersion,
		SoftwareClient:         a.SoftwareClient,
		SoftwareClientID:       a.SoftwareClientID,
		Jito:                   a.Jito,
		IsActive:               a.IsActive,
		IsDZ:                   a.IsDZ,
		ActiveStake:            a.ActiveStake,
		Commission:             a.Commission,
		Delinquent:             a.Delinquent,
		Epoch:                  a.Epoch,
		EpochCredits:           a.EpochCredits,
		TotalScore:             a.TotalScore,
		DataCenterKey:          a.DataCenterKey,
		AutonomousSystemNumber: a.AutonomousSystemNumber,
		Latitude:               a.Latitude,
		Longitude:              a.Longitude,
		IP:                     a.IP,
		StakePoolsList:         a.StakePoolsList,
	}

	if a.Name != nil {
		v.Name = *a.Name
	}
	if a.JitoCommission != nil {
		v.JitoCommission = *a.JitoCommission
	}
	if a.SkippedSlotPercent != nil {
		v.SkippedSlotPercent = *a.SkippedSlotPercent
	}

	return v
}

// HTTPClient fetches validator data from the validators.app HTTP API.
type HTTPClient struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewHTTPClient creates a new HTTPClient for the validators.app API.
func NewHTTPClient(baseURL, apiKey string) *HTTPClient {
	return &HTTPClient{
		baseURL:    baseURL,
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}
}

// GetValidators fetches all active mainnet validators from the API.
func (c *HTTPClient) GetValidators(ctx context.Context) ([]Validator, error) {
	url := c.baseURL + "/api/v1/validators/mainnet.json?limit=9999&active_only=true"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Token", c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var apiValidators []apiValidator
	if err := json.NewDecoder(resp.Body).Decode(&apiValidators); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	validators := make([]Validator, 0, len(apiValidators))
	for i := range apiValidators {
		if apiValidators[i].Account == "" {
			continue
		}
		validators = append(validators, apiValidators[i].toValidator())
	}

	return validators, nil
}
