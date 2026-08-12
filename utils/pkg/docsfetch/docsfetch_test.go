package docsfetch

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	content, source, err := c.Read(context.Background(), "edge-runbook")
	require.NoError(t, err)
	assert.Equal(t, "# from public", content)
	assert.Contains(t, source, "edge-runbook.md")
}

func TestRead_InvalidPage(t *testing.T) {
	t.Parallel()
	c := &Client{}
	_, _, err := c.Read(context.Background(), "../../../etc/passwd")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page name")
}

func TestRead_NotFound(t *testing.T) {
	t.Parallel()
	public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	t.Cleanup(public.Close)

	c := &Client{HTTP: public.Client(), Base: public.URL + "/"}
	_, _, err := c.Read(context.Background(), "missing-page")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "docs page not found")
}
