package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetAllowedDomainsEnvOverride(t *testing.T) {
	// t.Setenv restores the previous value after the test.
	t.Setenv("AUTH_ALLOWED_DOMAINS", "doublezero.xyz,malbeclabs.com,doublezero.us")
	got := getAllowedDomains()
	assert.Contains(t, got, "doublezero.us")
	assert.Contains(t, got, "doublezero.xyz")
	assert.Contains(t, got, "malbeclabs.com")
}
