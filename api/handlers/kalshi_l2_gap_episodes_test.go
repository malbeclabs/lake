package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The run collapse is pure, so it is pinned here rather than through the query: a test that
// inserted rows a second apart would depend on which side of a second boundary each INSERT landed
// on, and would merge or split its own episodes at random.
func TestCollapseKalshiL2GapSeconds(t *testing.T) {
	tests := []struct {
		name string
		secs []uint32
		want []KalshiL2GapEpisode
	}{
		{
			name: "nothing recorded is no timeline",
			secs: nil,
			want: nil,
		},
		{
			name: "one second is an episode of one",
			secs: []uint32{100},
			want: []KalshiL2GapEpisode{{Start: 100, Seconds: 1}},
		},
		{
			// groupUniqArray returns no order. Two adjacent seconds that arrive apart are one
			// outage and must not be drawn as two.
			name: "an unsorted contiguous run folds into one episode",
			secs: []uint32{102, 100, 101},
			want: []KalshiL2GapEpisode{{Start: 100, Seconds: 3}},
		},
		{
			name: "a repeated second does not extend the run",
			secs: []uint32{100, 100, 101, 101},
			want: []KalshiL2GapEpisode{{Start: 100, Seconds: 2}},
		},
		{
			// The hole is the point: one clean second between two losses is the recovery, and
			// folding across it would report a single outage twice as long as any that happened.
			name: "a single clean second splits the run",
			secs: []uint32{100, 102},
			want: []KalshiL2GapEpisode{{Start: 100, Seconds: 1}, {Start: 102, Seconds: 1}},
		},
		{
			name: "episodes come out in time order",
			secs: []uint32{500, 100, 101, 300},
			want: []KalshiL2GapEpisode{
				{Start: 100, Seconds: 2},
				{Start: 300, Seconds: 1},
				{Start: 500, Seconds: 1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, collapseKalshiL2GapSeconds(tt.secs))
		})
	}
}
