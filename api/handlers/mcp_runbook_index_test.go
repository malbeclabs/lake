package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRunbookIndex(t *testing.T) {
	t.Parallel()

	t.Run("reads service id and page from index list", func(t *testing.T) {
		t.Parallel()
		refs := parseRunbookIndex(`
# Runbooks

` + "```markdown\n- `example` — [Ignore me](example.md)\n```" + `

## Index

- ` + "`feed-a`" + ` — [Feed A + Edge Connect](feed-a-runbook.md)
- [Plain title](other-runbook.md)
`)
		require.Len(t, refs, 2)
		assert.Equal(t, "feed-a", refs[0].Service)
		assert.Equal(t, "feed-a-runbook", refs[0].Page)
		assert.Equal(t, "Feed A + Edge Connect", refs[0].Title)
		assert.Equal(t, "other-runbook", refs[1].Service)
		assert.Equal(t, "other-runbook", refs[1].Page)
	})

	t.Run("skips the index itself, mcp, and external urls", func(t *testing.T) {
		t.Parallel()
		refs := parseRunbookIndex(`
## Index

- ` + "`self`" + ` — [This page](runbooks.md)
- ` + "`mcp`" + ` — [Connect](mcp.md)
- ` + "`web`" + ` — [Site](https://docs.doublezero.xyz/feed-a-runbook.md)
`)
		assert.Empty(t, refs)
	})
}
