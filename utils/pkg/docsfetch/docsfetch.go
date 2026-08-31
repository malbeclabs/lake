// Package docsfetch loads DoubleZero documentation from the public
// malbeclabs/docs repo via raw GitHub. Lake does not host runbooks; it only reads them.
package docsfetch

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

// DefaultBase is the public docs tree on malbeclabs/docs@main.
const DefaultBase = "https://raw.githubusercontent.com/malbeclabs/docs/main/docs/"

// MaxPageBytes bounds a fetched page so an oversized document cannot flood the
// model's context; the largest docs page is ~34 KB.
const MaxPageBytes = 65536

// truncationMarker tells the model the page was cut and where the rest is, so
// it can point the user at the source instead of answering from a partial page.
func truncationMarker(url string) string {
	return fmt.Sprintf("\n\n... (truncated at %d bytes; full page: %s)", MaxPageBytes, url)
}

// validPage restricts page slugs so callers cannot traverse out of the docs tree.
var validPage = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9\-]*$`)

// ValidPage reports whether name is a safe docs slug (e.g. "setup").
func ValidPage(name string) bool {
	return validPage.MatchString(name)
}

// Client reads markdown pages from the public docs GitHub raw URL.
type Client struct {
	HTTP *http.Client
	Base string
}

// FromEnv builds a Client. DZ_DOCS_BASE_URL overrides the default GitHub raw base
// (used in tests); production uses DefaultBase.
func FromEnv() *Client {
	base := strings.TrimSpace(os.Getenv("DZ_DOCS_BASE_URL"))
	if base == "" {
		base = DefaultBase
	}
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}
	return &Client{
		HTTP: &http.Client{Timeout: 15 * time.Second},
		Base: base,
	}
}

// Read returns the markdown for page (without ".md") from GitHub raw. truncated
// reports whether the page exceeded MaxPageBytes and content is only its head.
func (c *Client) Read(ctx context.Context, page string) (content, source string, truncated bool, err error) {
	page = strings.TrimSpace(page)
	if page == "" {
		return "", "", false, fmt.Errorf("page is required")
	}
	if !ValidPage(page) {
		return "", "", false, fmt.Errorf("invalid page name: %s", page)
	}

	base := c.Base
	if base == "" {
		base = DefaultBase
	}
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	url := base + page + ".md"
	req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if reqErr != nil {
		return "", "", false, fmt.Errorf("failed to create request: %w", reqErr)
	}
	resp, doErr := httpClient.Do(req)
	if doErr != nil {
		return "", "", false, fmt.Errorf("failed to fetch docs: %w", doErr)
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, MaxPageBytes+1))
	if readErr != nil {
		return "", "", false, fmt.Errorf("failed to read response: %w", readErr)
	}
	if resp.StatusCode != http.StatusOK {
		return "", "", false, fmt.Errorf("docs page not found: %s (status %d)", page, resp.StatusCode)
	}
	if len(body) > MaxPageBytes {
		return string(body[:MaxPageBytes]) + truncationMarker(url), url, true, nil
	}
	return string(body), url, false, nil
}
