package handlers

import "testing"

func TestSumMeasuredAlongPath(t *testing.T) {
	m := map[string]linkMeasured{
		"a:b": {AvgRttMs: 8.57, P95RttMs: 8.9, AvgJitterMs: 0.02, SampleCount: 100},
		"b:a": {AvgRttMs: 8.57, P95RttMs: 8.9, AvgJitterMs: 0.02, SampleCount: 100},
		"b:c": {AvgRttMs: 64.24, P95RttMs: 65.1, AvgJitterMs: 0.05, SampleCount: 100},
		"c:b": {AvgRttMs: 64.24, P95RttMs: 65.1, AvgJitterMs: 0.05, SampleCount: 100},
	}

	t.Run("sums every hop when all are measured", func(t *testing.T) {
		rtt, p95, jitter, partial := sumMeasuredAlongPath([]string{"a", "b", "c"}, m, 99999)
		if partial {
			t.Fatalf("expected fully measured, got partial")
		}
		if diff := rtt - 72.81; diff > 0.001 || diff < -0.001 {
			t.Errorf("rtt = %v, want 72.81", rtt)
		}
		if diff := p95 - 74.0; diff > 0.001 || diff < -0.001 {
			t.Errorf("p95 = %v, want 74.0", p95)
		}
		if diff := jitter - 0.07; diff > 0.001 || diff < -0.001 {
			t.Errorf("jitter = %v, want 0.07", jitter)
		}
	})

	// A hop with no rollup samples must fall back to the contracted figure for the
	// whole path and flag it, rather than silently reporting a short sum. MIA<->DFW
	// is a live example: committed 27.61ms with zero measured samples.
	t.Run("falls back to committed and flags when a hop is unmeasured", func(t *testing.T) {
		rtt, _, _, partial := sumMeasuredAlongPath([]string{"a", "b", "zz"}, m, 27610)
		if !partial {
			t.Fatalf("expected partial=true when a hop has no samples")
		}
		if diff := rtt - 27.61; diff > 0.001 || diff < -0.001 {
			t.Errorf("rtt = %v, want committed fallback 27.61", rtt)
		}
	})

	t.Run("single-node path is not measurable", func(t *testing.T) {
		_, _, _, partial := sumMeasuredAlongPath([]string{"a"}, m, 1000)
		if !partial {
			t.Errorf("expected partial=true for a path with no hops")
		}
	})
}

func TestPathMetroCodes(t *testing.T) {
	g := &kspGraph{Nodes: map[string]kspNodeInfo{
		"d1": {MetroCode: "tyo"},
		"d2": {MetroCode: "tyo"}, // second device in the same metro
		"d3": {MetroCode: "fra"},
		"d4": {MetroCode: "lon"},
	}}

	t.Run("collapses consecutive devices in the same metro", func(t *testing.T) {
		got := pathMetroCodes([]string{"d1", "d2", "d3", "d4"}, g)
		want := []string{"tyo", "fra", "lon"}
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("got %v, want %v", got, want)
			}
		}
	})

	t.Run("skips devices missing from the graph", func(t *testing.T) {
		got := pathMetroCodes([]string{"d1", "zz", "d4"}, g)
		if len(got) != 2 || got[0] != "tyo" || got[1] != "lon" {
			t.Errorf("got %v, want [tyo lon]", got)
		}
	})
}
