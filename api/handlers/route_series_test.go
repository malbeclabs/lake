package handlers

import "testing"

func TestParseRoutePairs(t *testing.T) {
	t.Run("parses and normalizes direction", func(t *testing.T) {
		got, err := parseRoutePairs("tyo-lon,FRA-lon")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("got %d pairs, want 2", len(got))
		}
		// Normalized so metro codes are lexicographically ordered, matching the
		// least/greatest convention the internet-latency tables use.
		if got[0] != [2]string{"lon", "tyo"} {
			t.Errorf("got[0] = %v, want [lon tyo]", got[0])
		}
		if got[1] != [2]string{"fra", "lon"} {
			t.Errorf("got[1] = %v, want [fra lon]", got[1])
		}
	})

	t.Run("rejects empty input", func(t *testing.T) {
		if _, err := parseRoutePairs(""); err == nil {
			t.Error("expected error for empty pairs")
		}
	})

	t.Run("rejects a malformed pair", func(t *testing.T) {
		if _, err := parseRoutePairs("tyolon"); err == nil {
			t.Error("expected error for pair without a separator")
		}
	})

	t.Run("rejects a self-pair", func(t *testing.T) {
		if _, err := parseRoutePairs("lon-lon"); err == nil {
			t.Error("expected error for identical metros")
		}
	})

	t.Run("caps the pair count", func(t *testing.T) {
		if _, err := parseRoutePairs("a-b,c-d,e-f,g-h,i-j,k-l,m-n,o-p,q-r,s-t,u-v"); err == nil {
			t.Error("expected error for more than 10 pairs")
		}
	})
}

func TestSumHopsAtBucket(t *testing.T) {
	t.Run("sums correctly when all hops are present", func(t *testing.T) {
		nodes := []string{"a", "b", "c"}
		links := map[string]float64{
			"a:b": 1.5,
			"b:c": 2.5,
		}
		sum, ok := sumHopsAtBucket(nodes, links)
		if !ok {
			t.Fatal("expected ok=true")
		}
		if sum != 4.0 {
			t.Errorf("sum = %v, want 4.0", sum)
		}
	})

	t.Run("reports not-ok when a hop is missing", func(t *testing.T) {
		nodes := []string{"a", "b", "c"}
		links := map[string]float64{
			"a:b": 1.5,
			// "b:c" missing
		}
		if _, ok := sumHopsAtBucket(nodes, links); ok {
			t.Error("expected ok=false for a missing hop")
		}
	})

	t.Run("reports not-ok for a path with fewer than 2 nodes", func(t *testing.T) {
		if _, ok := sumHopsAtBucket([]string{"a"}, map[string]float64{}); ok {
			t.Error("expected ok=false for a path with fewer than 2 nodes")
		}
		if _, ok := sumHopsAtBucket(nil, map[string]float64{}); ok {
			t.Error("expected ok=false for a nil path")
		}
	})
}
