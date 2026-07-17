package handlers

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestValidateChangeShape_AddDevice covers the payload-level shape checks the
// DB CHECK constraint cannot express: local_ref (column) is required as
// before, plus (relaxed from the old "requires contributor_pk + metro_pk")
// a device code, a contributor (contributor_code OR contributor_pk), and a
// metro (metro_pk OR new_metro with a code).
func TestValidateChangeShape_AddDevice(t *testing.T) {
	ref := "tmp_dev_1"

	tests := []struct {
		name    string
		payload string
		wantErr bool
	}{
		{
			name:    "existing contributor + existing metro (old shape) still accepted",
			payload: `{"contributor_pk":"c1","metro_pk":"m1","code":"nyc-x9"}`,
		},
		{
			name:    "new contributor + new metro accepted",
			payload: `{"contributor_code":"newco","code":"zzz-x1","new_metro":{"code":"ZZZ","latitude":10,"longitude":20}}`,
		},
		{
			name:    "new contributor + existing metro accepted",
			payload: `{"contributor_code":"newco","metro_pk":"m1","code":"nyc-x9"}`,
		},
		{
			name:    "existing contributor + new metro accepted",
			payload: `{"contributor_pk":"c1","code":"zzz-x1","new_metro":{"code":"ZZZ","latitude":10,"longitude":20}}`,
		},
		{
			name:    "missing code rejected",
			payload: `{"contributor_pk":"c1","metro_pk":"m1"}`,
			wantErr: true,
		},
		{
			name:    "missing contributor (neither code nor pk) rejected",
			payload: `{"metro_pk":"m1","code":"nyc-x9"}`,
			wantErr: true,
		},
		{
			name:    "missing metro (neither metro_pk nor new_metro) rejected",
			payload: `{"contributor_pk":"c1","code":"nyc-x9"}`,
			wantErr: true,
		},
		{
			name:    "new_metro without a code rejected",
			payload: `{"contributor_pk":"c1","code":"nyc-x9","new_metro":{"latitude":10,"longitude":20}}`,
			wantErr: true,
		},
		{
			name:    "new_metro with a code but missing coords rejected (null island)",
			payload: `{"contributor_pk":"c1","code":"nyc-x9","new_metro":{"code":"ZZZ"}}`,
			wantErr: true,
		},
		{
			name:    "new_metro with a code but explicit 0,0 coords rejected",
			payload: `{"contributor_pk":"c1","code":"nyc-x9","new_metro":{"code":"ZZZ","latitude":0,"longitude":0}}`,
			wantErr: true,
		},
		{
			name:    "new_metro with a code and valid coords accepted",
			payload: `{"contributor_pk":"c1","code":"nyc-x9","new_metro":{"code":"ZZZ","latitude":10,"longitude":20}}`,
		},
		{
			name:    "new_metro at prime meridian (long 0, lat non-zero) accepted",
			payload: `{"contributor_pk":"c1","code":"nyc-x9","new_metro":{"code":"LON","latitude":51.5,"longitude":0}}`,
		},
		{
			name:    "new_metro with out-of-range latitude rejected",
			payload: `{"contributor_pk":"c1","code":"nyc-x9","new_metro":{"code":"ZZZ","latitude":120,"longitude":20}}`,
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateChangeShape(OpAddDevice, nil, nil, &ref, json.RawMessage(tc.payload))
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	// local_ref itself is still required, independent of payload shape.
	validPayload := json.RawMessage(`{"contributor_pk":"c1","metro_pk":"m1","code":"nyc-x9"}`)
	assert.Error(t, validateChangeShape(OpAddDevice, nil, nil, nil, validPayload))
	empty := ""
	assert.Error(t, validateChangeShape(OpAddDevice, nil, nil, &empty, validPayload))
}
