package rewards

import (
	"context"
	"math"
	"os"
	"testing"
)

// twoNodeNetwork builds a minimal 2-operator network:
// city "a" and city "b", each operator contributing one link.
func twoSymmetricOperatorInput() ShapleyInput {
	return ShapleyInput{
		Devices: []Device{
			{Device: "a01-dzx-001", Operator: "alpha", Edge: 10, City: "a"},
			{Device: "b01-dzx-001", Operator: "beta", Edge: 10, City: "b"},
		},
		PrivateLinks: []PrivateLink{
			{Device1: "a01-dzx-001", Device2: "b01-dzx-001", Latency: 5, Bandwidth: 100, Uptime: 1.0, Shared: "NA"},
			{Device1: "b01-dzx-001", Device2: "a01-dzx-001", Latency: 5, Bandwidth: 100, Uptime: 1.0, Shared: "NA"},
		},
		PublicLinks: []PublicLink{
			{City1: "a", City2: "b", Latency: 50},
		},
		Demands: []Demand{
			{Start: "a", End: "b", Receivers: 1, Traffic: 1.0, Priority: 1.0, Type: 1, Multicast: "FALSE"},
		},
		OperatorUptime:   1.0,
		ContiguityBonus:  0,
		DemandMultiplier: 1.0,
	}
}

// --- CollapseSmallOperators unit tests (no binary needed) ---

func TestCollapseSmallOperators_CollapsesBelow(t *testing.T) {
	input := ShapleyInput{
		Devices: []Device{
			{Device: "d1", Operator: "big"},
			{Device: "d2", Operator: "big"},
			{Device: "d3", Operator: "big"},
			{Device: "d4", Operator: "big"},
			{Device: "d5", Operator: "big"},
			{Device: "s1", Operator: "small"},
			{Device: "s2", Operator: "small"},
		},
	}
	result := CollapseSmallOperators(input, 5)

	for _, d := range result.Devices {
		if d.Device == "s1" || d.Device == "s2" {
			if d.Operator != operatorOthers {
				t.Errorf("device %s: expected operator %q, got %q", d.Device, operatorOthers, d.Operator)
			}
		}
		if d.Device == "d1" {
			if d.Operator != "big" {
				t.Errorf("device %s: expected operator %q, got %q", d.Device, "big", d.Operator)
			}
		}
	}
}

func TestCollapseSmallOperators_NoCollapseAtThreshold(t *testing.T) {
	input := ShapleyInput{
		Devices: []Device{
			{Device: "d1", Operator: "op"},
			{Device: "d2", Operator: "op"},
			{Device: "d3", Operator: "op"},
			{Device: "d4", Operator: "op"},
			{Device: "d5", Operator: "op"},
		},
	}
	result := CollapseSmallOperators(input, 5)
	for _, d := range result.Devices {
		if d.Operator != "op" {
			t.Errorf("expected operator %q to survive threshold, got %q", "op", d.Operator)
		}
	}
}

func TestCollapseSmallOperators_NothingToCollapse(t *testing.T) {
	input := ShapleyInput{
		Devices: []Device{
			{Device: "d1", Operator: "a"},
			{Device: "d2", Operator: "a"},
			{Device: "d3", Operator: "a"},
			{Device: "d4", Operator: "a"},
			{Device: "d5", Operator: "a"},
			{Device: "d6", Operator: "b"},
			{Device: "d7", Operator: "b"},
			{Device: "d8", Operator: "b"},
			{Device: "d9", Operator: "b"},
			{Device: "d10", Operator: "b"},
		},
	}
	result := CollapseSmallOperators(input, 5)
	if len(result.Devices) != len(input.Devices) {
		t.Errorf("device count changed unexpectedly")
	}
	for _, d := range result.Devices {
		if d.Operator == operatorOthers {
			t.Errorf("unexpected collapse: device %s got operator %q", d.Device, operatorOthers)
		}
	}
}

func TestCollapseSmallOperators_PreservesLinks(t *testing.T) {
	input := ShapleyInput{
		Devices: []Device{
			{Device: "d1", Operator: "small"},
		},
		PrivateLinks: []PrivateLink{
			{Device1: "d1", Device2: "d2", Latency: 5, Bandwidth: 100},
		},
	}
	result := CollapseSmallOperators(input, 5)
	if len(result.PrivateLinks) != 1 {
		t.Errorf("expected 1 private link, got %d", len(result.PrivateLinks))
	}
}

// --- Simulate integration tests (require shapley-cli binary) ---

func requireBinary(t *testing.T) {
	t.Helper()
	path := os.Getenv("SHAPLEY_CLI_PATH")
	if path == "" {
		t.Skip("SHAPLEY_CLI_PATH not set, skipping integration test")
	}
	SetBinaryPath(path)
}

func TestSimulate_SymmetricOperatorsEqualShares(t *testing.T) {
	requireBinary(t)

	input := twoSymmetricOperatorInput()
	results, err := Simulate(context.Background(), input)
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}

	vals := make(map[string]float64)
	for _, r := range results {
		vals[r.Operator] = r.Value
	}

	alpha, beta := vals["alpha"], vals["beta"]
	if math.Abs(alpha-beta) > 1e-6 {
		t.Errorf("symmetric operators should have equal Shapley values: alpha=%v beta=%v", alpha, beta)
	}
}

func TestSimulate_ProportionsSumToOne(t *testing.T) {
	requireBinary(t)

	input := twoSymmetricOperatorInput()
	results, err := Simulate(context.Background(), input)
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}

	// Only positive values contribute to proportions
	totalPositive := 0.0
	for _, r := range results {
		if r.Value > 0 {
			totalPositive += r.Value
		}
	}
	sumProp := 0.0
	for _, r := range results {
		sumProp += r.Proportion
	}
	if totalPositive > 0 && math.Abs(sumProp-1.0) > 1e-6 {
		t.Errorf("proportions should sum to 1.0, got %v", sumProp)
	}
}

func TestSimulate_FasterLinkHigherValue(t *testing.T) {
	requireBinary(t)

	// alpha has a fast low-latency link, beta has a slow high-latency link.
	// alpha should have a higher Shapley value.
	input := ShapleyInput{
		Devices: []Device{
			{Device: "a01-dzx-001", Operator: "alpha", Edge: 10, City: "a"},
			{Device: "a01-dzx-002", Operator: "alpha", Edge: 10, City: "a"},
			{Device: "b01-dzx-001", Operator: "beta", Edge: 10, City: "b"},
			{Device: "b01-dzx-002", Operator: "beta", Edge: 10, City: "b"},
		},
		PrivateLinks: []PrivateLink{
			// alpha: fast link
			{Device1: "a01-dzx-001", Device2: "b01-dzx-001", Latency: 1, Bandwidth: 100, Uptime: 1.0, Shared: "NA"},
			{Device1: "b01-dzx-001", Device2: "a01-dzx-001", Latency: 1, Bandwidth: 100, Uptime: 1.0, Shared: "NA"},
			// beta: slow link
			{Device1: "a01-dzx-002", Device2: "b01-dzx-002", Latency: 80, Bandwidth: 10, Uptime: 1.0, Shared: "NA"},
			{Device1: "b01-dzx-002", Device2: "a01-dzx-002", Latency: 80, Bandwidth: 10, Uptime: 1.0, Shared: "NA"},
		},
		PublicLinks: []PublicLink{
			{City1: "a", City2: "b", Latency: 50},
		},
		Demands: []Demand{
			{Start: "a", End: "b", Receivers: 1, Traffic: 1.0, Priority: 1.0, Type: 1, Multicast: "FALSE"},
		},
		OperatorUptime:   1.0,
		ContiguityBonus:  0,
		DemandMultiplier: 1.0,
	}

	results, err := Simulate(context.Background(), input)
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}

	vals := make(map[string]float64)
	for _, r := range results {
		vals[r.Operator] = r.Value
	}

	if vals["alpha"] <= vals["beta"] {
		t.Errorf("expected alpha (fast link) to have higher value than beta (slow link): alpha=%v beta=%v", vals["alpha"], vals["beta"])
	}
}

func TestCompare_DeltasConsistent(t *testing.T) {
	requireBinary(t)

	baseline := twoSymmetricOperatorInput()

	// Modified: add a third operator "gamma" with an extra link
	modified := twoSymmetricOperatorInput()
	modified.Devices = append(modified.Devices,
		Device{Device: "c01-dzx-001", Operator: "gamma", Edge: 10, City: "c"},
	)

	result, err := Compare(context.Background(), baseline, modified)
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}

	for _, d := range result.Deltas {
		expected := d.ModifiedValue - d.BaselineValue
		if math.Abs(d.ValueDelta-expected) > 1e-9 {
			t.Errorf("operator %s: ValueDelta %v != ModifiedValue - BaselineValue %v", d.Operator, d.ValueDelta, expected)
		}
	}
}

// TestSimulate_CanonicalSimple is pinned against the upstream network-shapley-rs
// "simple" test case, which is itself cross-validated against the Python reference
// implementation. Tolerances match the Rust test suite (0.01 for value, 0.0001 for proportion).
func TestSimulate_CanonicalSimple(t *testing.T) {
	requireBinary(t)

	input := ShapleyInput{
		Devices: []Device{
			{Device: "SIN1", Edge: 1, Operator: "Alpha"},
			{Device: "FRA1", Edge: 1, Operator: "Alpha"},
			{Device: "AMS1", Edge: 1, Operator: "Beta"},
			{Device: "LON1", Edge: 1, Operator: "Beta"},
		},
		PrivateLinks: []PrivateLink{
			{Device1: "SIN1", Device2: "FRA1", Latency: 50.0, Bandwidth: 10.0, Uptime: 1.0, Shared: "NA"},
			{Device1: "FRA1", Device2: "AMS1", Latency: 3.0, Bandwidth: 10.0, Uptime: 1.0, Shared: "NA"},
			{Device1: "FRA1", Device2: "LON1", Latency: 5.0, Bandwidth: 10.0, Uptime: 1.0, Shared: "NA"},
		},
		PublicLinks: []PublicLink{
			{City1: "SIN", City2: "FRA", Latency: 100.0},
			{City1: "SIN", City2: "AMS", Latency: 102.0},
			{City1: "FRA", City2: "LON", Latency: 7.0},
			{City1: "FRA", City2: "AMS", Latency: 5.0},
		},
		Demands: []Demand{
			{Start: "SIN", End: "AMS", Receivers: 1, Traffic: 1.0, Priority: 1.0, Type: 1, Multicast: "TRUE"},
			{Start: "SIN", End: "LON", Receivers: 5, Traffic: 1.0, Priority: 2.0, Type: 1, Multicast: "TRUE"},
			{Start: "AMS", End: "LON", Receivers: 2, Traffic: 3.0, Priority: 1.0, Type: 2, Multicast: "FALSE"},
			{Start: "AMS", End: "FRA", Receivers: 1, Traffic: 3.0, Priority: 1.0, Type: 2, Multicast: "FALSE"},
		},
		OperatorUptime:   0.98,
		ContiguityBonus:  5.0,
		DemandMultiplier: 1.0,
	}

	results, err := Simulate(context.Background(), input)
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}

	vals := make(map[string]float64)
	props := make(map[string]float64)
	for _, r := range results {
		vals[r.Operator] = r.Value
		props[r.Operator] = r.Proportion
	}

	// Expected values from upstream Rust test suite (cross-validated against Python).
	want := map[string]struct{ value, proportion float64 }{
		"Alpha": {173.6756, 0.6702},
		"Beta":  {85.4756, 0.3298},
	}

	for op, w := range want {
		if math.Abs(vals[op]-w.value) > 0.01 {
			t.Errorf("operator %s: value = %v, want %v (±0.01)", op, vals[op], w.value)
		}
		if math.Abs(props[op]-w.proportion) > 0.0001 {
			t.Errorf("operator %s: proportion = %v, want %v (±0.0001)", op, props[op], w.proportion)
		}
	}
}
