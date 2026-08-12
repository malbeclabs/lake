// Package runbooks holds fallback copies of agent-oriented onboarding runbooks.
// The source of truth is public malbeclabs/docs. Lake fetches those
// live; these files exist so local/mockup installs still work when a page is
// not published yet (see get_onboarding_runbook).
package runbooks

import "embed"

//go:embed *.md
var RunbooksFS embed.FS
