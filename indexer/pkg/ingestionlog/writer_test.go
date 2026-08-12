package ingestionlog

import (
	"errors"
	"testing"
	"time"
)

// TestBuildRecord_StatusMapping pins the three run outcomes, and "partial" in
// particular. Staleness alerting asks for the last *successful* run's finished_at, so
// a cycle that banked committed work and stopped must not be recorded as a success:
// doing so resets that clock while the data it covers stays behind, and a drain that
// never converges reads as healthy indefinitely. Reporting the honest source timestamp
// does not cover this on its own, because nothing reads source_max_event_ts.
func TestBuildRecord_StatusMapping(t *testing.T) {
	t.Parallel()

	start := time.Now().Add(-time.Second)

	tests := []struct {
		name       string
		result     RefreshResult
		err        error
		wantStatus string
		wantErrMsg bool
	}{
		{"clean run", RefreshResult{}, nil, "success", false},
		{"failed run", RefreshResult{}, errors.New("boom"), "error", true},
		{"stopped with work left", RefreshResult{Partial: true}, nil, "partial", false},
		{
			// An error outranks Partial: the run failed, and "error" is what carries the
			// message. Reporting it as partial would drop the cause.
			name: "failed run that also had work left", result: RefreshResult{Partial: true},
			err: errors.New("boom"), wantStatus: "error", wantErrMsg: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := buildRecord("dzingest", "RefreshPermissionEvents", "mainnet-beta", start, tt.result, tt.err)
			if rec.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q", rec.Status, tt.wantStatus)
			}
			if got := rec.ErrorMessage != nil; got != tt.wantErrMsg {
				t.Errorf("ErrorMessage present = %v, want %v", got, tt.wantErrMsg)
			}
		})
	}
}

// TestBuildRecord_PartialIsNotSuccess is the property the staleness alert depends on,
// stated directly: a partial run must not satisfy a `status = 'success'` filter.
func TestBuildRecord_PartialIsNotSuccess(t *testing.T) {
	t.Parallel()

	rec := buildRecord("dzingest", "RefreshPermissionEvents", "mainnet-beta", time.Now(),
		RefreshResult{Partial: true, RowsAffected: 42}, nil)

	if rec.Status == "success" {
		t.Fatal("a partial run recorded as success would reset the staleness clock")
	}
	if rec.RowsAffected == nil || *rec.RowsAffected != 42 {
		t.Error("committed rows must still be reported on a partial run")
	}
}
