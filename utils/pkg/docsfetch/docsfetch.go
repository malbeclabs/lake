// Package docsfetch loads DoubleZero documentation from the public
// malbeclabs/docs repo (raw GitHub). An optional local checkout can override
// that for development. Lake does not host runbooks; it only reads them.
package docsfetch

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// DefaultBase is the public docs tree on malbeclabs/docs@main.
const DefaultBase = "https://raw.githubusercontent.com/malbeclabs/docs/main/docs/"

// validPage restricts page slugs so callers cannot traverse out of the docs tree.
var validPage = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9\-]*$`)

// ValidPage reports whether name is a safe docs slug (e.g. "setup").
func ValidPage(name string) bool {
	return validPage.MatchString(name)
}

// Client reads markdown pages from an optional local directory, then the public docs URL.
type Client struct {
	HTTP     *http.Client
	LocalDir string
	Base     string
}

// FromEnv builds a Client from DZ_DOCS_DIR (optional local malbeclabs/docs/docs
// checkout) and DZ_DOCS_BASE_URL (defaults to the public GitHub raw tree).
func FromEnv() *Client {
	base := strings.TrimSpace(os.Getenv("DZ_DOCS_BASE_URL"))
	if base == "" {
		base = DefaultBase
	}
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}
	return &Client{
		HTTP:     &http.Client{Timeout: 15 * time.Second},
		LocalDir: strings.TrimSpace(os.Getenv("DZ_DOCS_DIR")),
		Base:     base,
	}
}

// Read returns the markdown for page (without ".md"). LocalDir is tried first
// when set; otherwise the public docs base is fetched.
func (c *Client) Read(ctx context.Context, page string) (content, source string, err error) {
	page = strings.TrimSpace(page)
	if page == "" {
		return "", "", fmt.Errorf("page is required")
	}
	if !ValidPage(page) {
		return "", "", fmt.Errorf("invalid page name: %s", page)
	}

	if c.LocalDir != "" {
		path := filepath.Join(c.LocalDir, page+".md")
		data, readErr := os.ReadFile(path)
		if readErr == nil {
			return string(data), path, nil
		}
		if !os.IsNotExist(readErr) {
			return "", "", fmt.Errorf("local docs %s: %w", path, readErr)
		}
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
		return "", "", fmt.Errorf("failed to create request: %w", reqErr)
	}
	resp, doErr := httpClient.Do(req)
	if doErr != nil {
		return "", "", fmt.Errorf("failed to fetch docs: %w", doErr)
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return "", "", fmt.Errorf("failed to read response: %w", readErr)
	}
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("docs page not found: %s (status %d)", page, resp.StatusCode)
	}
	return string(body), url, nil
}
