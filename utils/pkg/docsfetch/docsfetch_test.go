package docsfetch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidPage(t *testing.T) {
	t.Parallel()
	assert.True(t, ValidPage("edge-runbook"))
	assert.True(t, ValidPage("index"))
	assert.False(t, ValidPage("../etc/passwd"))
	assert.False(t, ValidPage("foo/bar"))
	assert.False(t, ValidPage(""))
}

func TestRead_PublicDocs(t *testing.T) {
	t.Parallel()
	public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/edge-runbook.md" {
			_, _ = w.Write([]byte("# from public"))
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(public.Close)

	c := &Client{
		HTTP: public.Client(),
		Base: public.URL + "/",
	}
	content, source, truncated, err := c.Read(context.Background(), "edge-runbook")
	require.NoError(t, err)
	assert.Equal(t, "# from public", content)
	assert.Contains(t, source, "edge-runbook.md")
	assert.False(t, truncated)
}

func TestRead_InvalidPage(t *testing.T) {
	t.Parallel()
	c := &Client{}
	_, _, _, err := c.Read(context.Background(), "../../../etc/passwd")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page name")
}

func TestRead_TruncatesOversizedPage(t *testing.T) {
	t.Parallel()
	big := strings.Repeat("a", MaxPageBytes+5000)
	public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(big))
	}))
	t.Cleanup(public.Close)

	c := &Client{HTTP: public.Client(), Base: public.URL + "/"}
	content, source, truncated, err := c.Read(context.Background(), "huge-page")
	require.NoError(t, err)
	assert.True(t, truncated)
	assert.Equal(t, big[:MaxPageBytes], strings.TrimSuffix(content, truncationMarker(source)))
	assert.Contains(t, content, "truncated at 65536 bytes")
	assert.Contains(t, content, "huge-page.md")
}

func TestRead_LargestRealPageIsNotTruncated(t *testing.T) {
	t.Parallel()
	page := strings.Repeat("a", 34000)
	public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(page))
	}))
	t.Cleanup(public.Close)

	c := &Client{HTTP: public.Client(), Base: public.URL + "/"}
	content, _, truncated, err := c.Read(context.Background(), "contribute-provisioning")
	require.NoError(t, err)
	assert.False(t, truncated)
	assert.Equal(t, page, content)
	assert.NotContains(t, content, "truncated")
}

func TestRead_NotFound(t *testing.T) {
	t.Parallel()
	public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	t.Cleanup(public.Close)

	c := &Client{HTTP: public.Client(), Base: public.URL + "/"}
	_, _, _, err := c.Read(context.Background(), "missing-page")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "docs page not found")
}
