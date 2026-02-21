package handlers

import "testing"

func TestClassifyLinkStatus(t *testing.T) {
	tests := []struct {
		name           string
		avgLatency     float64
		lossPct        float64
		committedRttUs float64
		want           string
	}{
		{
			name:           "healthy with no issues",
			avgLatency:     5000,
			lossPct:        0,
			committedRttUs: 5000,
			want:           "healthy",
		},
		{
			name:           "degraded from packet loss",
			avgLatency:     5000,
			lossPct:        1.0, // exactly at LossWarningPct
			committedRttUs: 5000,
			want:           "degraded",
		},
		{
			name:           "unhealthy from severe packet loss",
			avgLatency:     5000,
			lossPct:        10.0, // exactly at LossCriticalPct
			committedRttUs: 5000,
			want:           "unhealthy",
		},
		{
			name:           "degraded from high latency",
			avgLatency:     6200, // 24% over committed (>= 20%)
			lossPct:        0,
			committedRttUs: 5000,
			want:           "degraded",
		},
		{
			name:           "unhealthy from critical latency",
			avgLatency:     7600, // 52% over committed (>= 50%)
			lossPct:        0,
			committedRttUs: 5000,
			want:           "unhealthy",
		},
		{
			name:           "no latency check when committed RTT is zero",
			avgLatency:     50000, // way over any threshold
			lossPct:        0,
			committedRttUs: 0, // DZX or intra-metro link
			want:           "healthy",
		},
		{
			name:           "no latency check when avg latency is zero",
			avgLatency:     0,
			lossPct:        0,
			committedRttUs: 5000,
			want:           "healthy",
		},
		{
			name:           "loss still checked when committed RTT is zero",
			avgLatency:     50000,
			lossPct:        5.0,
			committedRttUs: 0,
			want:           "degraded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyLinkStatus(tt.avgLatency, tt.lossPct, tt.committedRttUs)
			if got != tt.want {
				t.Errorf("classifyLinkStatus(%v, %v, %v) = %q, want %q",
					tt.avgLatency, tt.lossPct, tt.committedRttUs, got, tt.want)
			}
		})
	}
}
