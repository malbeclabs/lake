package dztelemusage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// HTTPInfluxDBClient implements InfluxDBClient using the InfluxDB v3 HTTP Query API with CSV output.
// This is more reliable than the FlightSQL/gRPC-based SDK for bulk queries and large time windows.
type HTTPInfluxDBClient struct {
	host     string
	token    string
	database string
	client   *http.Client
}

// NewHTTPInfluxDBClient creates a new HTTP-based InfluxDB client.
func NewHTTPInfluxDBClient(host, token, database string) *HTTPInfluxDBClient {
	return &HTTPInfluxDBClient{
		host:     strings.TrimRight(host, "/"),
		token:    token,
		database: database,
		client:   &http.Client{Timeout: 10 * time.Minute},
	}
}

// QuerySQL implements InfluxDBClient by executing a SQL query via the HTTP API
// and returning the results as parsed CSV rows.
func (c *HTTPInfluxDBClient) QuerySQL(ctx context.Context, sqlQuery string) ([]map[string]any, error) {
	body, err := json.Marshal(map[string]string{
		"q":      sqlQuery,
		"db":     c.database,
		"format": "csv",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.host+"/api/v3/query_sql", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("query returned status %d: %s", resp.StatusCode, string(errBody))
	}

	return ParseInfluxCSV(resp.Body)
}

// QueryRawCSV executes a SQL query and writes the raw CSV response to w.
// If skipHeader is true, the CSV header row is omitted from the output.
// This is useful for streaming chunked exports where only the first chunk should include headers.
func (c *HTTPInfluxDBClient) QueryRawCSV(ctx context.Context, sqlQuery string, w io.Writer, skipHeader bool) error {
	body, err := json.Marshal(map[string]string{
		"q":      sqlQuery,
		"db":     c.database,
		"format": "csv",
	})
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.host+"/api/v3/query_sql", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("query returned status %d: %s", resp.StatusCode, string(errBody))
	}

	reader := bufio.NewReader(resp.Body)
	if skipHeader {
		if _, err := reader.ReadString('\n'); err != nil && err != io.EOF {
			return fmt.Errorf("failed to skip CSV header: %w", err)
		}
	}

	if _, err := io.Copy(w, reader); err != nil {
		return fmt.Errorf("failed to write CSV: %w", err)
	}

	return nil
}

// Close implements InfluxDBClient.
func (c *HTTPInfluxDBClient) Close() error {
	return nil
}

// ParseInfluxCSV parses a CSV response from InfluxDB into a slice of maps.
// Empty string values are represented as nil so downstream nil checks work correctly.
func ParseInfluxCSV(r io.Reader) ([]map[string]any, error) {
	csvReader := csv.NewReader(r)
	csvReader.TrimLeadingSpace = true

	headers, err := csvReader.Read()
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV headers: %w", err)
	}

	var results []map[string]any
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV record: %w", err)
		}

		row := make(map[string]any, len(headers))
		for i, h := range headers {
			if i < len(record) {
				if record[i] == "" {
					row[h] = nil
				} else {
					row[h] = record[i]
				}
			}
		}
		results = append(results, row)
	}

	return results, nil
}
