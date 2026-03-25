package handlers

import "testing"

func TestIsValidatorVersionOk(t *testing.T) {
	t.Parallel()
	tests := []struct {
		client  string
		version string
		want    bool
	}{
		{"Jito", "2.2.3", true},
		{"Jito", "2.0.0", false},
		{"Agave", "2.1.0", true},
		{"Agave", "1.9.0", false},
		{"Firedancer", "0.1.0", true},
		{"unknown", "1.0.0", false},
		{"Agave", "", false},
	}

	for _, tt := range tests {
		got := isValidatorVersionOk(tt.client, tt.version)
		if got != tt.want {
			t.Errorf("isValidatorVersionOk(%q, %q) = %v, want %v",
				tt.client, tt.version, got, tt.want)
		}
	}
}
