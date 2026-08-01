package rewards

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// PrivateLink represents a direct connection between two devices.
type PrivateLink struct {
	Device1   string  `json:"device1"`
	Device2   string  `json:"device2"`
	Latency   float64 `json:"latency"`
	Bandwidth float64 `json:"bandwidth"`
	Uptime    float64 `json:"uptime"`
	Shared    string  `json:"shared"` // "NA" or numeric string
}

// Device represents a network node.
type Device struct {
	Device       string `json:"device"`
	Edge         int    `json:"edge"`
	Operator     string `json:"operator"`
	OperatorPk   string `json:"operator_pk,omitempty"`   // DB pk for linking to contributor detail page
	City         string `json:"city,omitempty"`          // 3-letter code, used by CLI and frontend
	CityName     string `json:"city_name,omitempty"`     // full name, frontend only
	OperatorName string `json:"operator_name,omitempty"` // full name, frontend only
}

// PublicLink represents a public internet connection between cities.
type PublicLink struct {
	City1   string  `json:"city1"`
	City2   string  `json:"city2"`
	Latency float64 `json:"latency"`
}

// Demand represents a traffic demand between cities.
type Demand struct {
	Start     string  `json:"start"`
	End       string  `json:"end"`
	Receivers int     `json:"receivers"`
	Traffic   float64 `json:"traffic"`
	Priority  float64 `json:"priority"`
	Type      int     `json:"type"`
	Multicast string  `json:"multicast"` // "TRUE" or "FALSE"
}

// ShapleyInput is the full input to the Shapley computation.
type ShapleyInput struct {
	PrivateLinks     []PrivateLink `json:"private_links"`
	Devices          []Device      `json:"devices"`
	Demands          []Demand      `json:"demands"`
	PublicLinks      []PublicLink  `json:"public_links"`
	OperatorUptime   float64       `json:"operator_uptime"`
	ContiguityBonus  float64       `json:"contiguity_bonus"`
	DemandMultiplier float64       `json:"demand_multiplier"`
}

// OperatorValue is the output for a single operator from the Shapley computation.
type OperatorValue struct {
	Operator   string  `json:"operator"`
	Value      float64 `json:"value"`
	Proportion float64 `json:"proportion"`
}

// CompareResult holds baseline vs modified simulation results with deltas.
type CompareResult struct {
	BaselineResults []OperatorValue `json:"baseline_results"`
	ModifiedResults []OperatorValue `json:"modified_results"`
	Deltas          []OperatorDelta `json:"deltas"`
	BaselineTotal   float64         `json:"baseline_total"`
	ModifiedTotal   float64         `json:"modified_total"`
}

// OperatorDelta shows the change between baseline and modified for an operator.
type OperatorDelta struct {
	Operator           string  `json:"operator"`
	BaselineValue      float64 `json:"baseline_value"`
	ModifiedValue      float64 `json:"modified_value"`
	ValueDelta         float64 `json:"value_delta"`
	BaselineProportion float64 `json:"baseline_proportion"`
	ModifiedProportion float64 `json:"modified_proportion"`
	ProportionDelta    float64 `json:"proportion_delta"`
}

// LinkResult is the output for a single link from the link estimate computation.
type LinkResult struct {
	Device1   string  `json:"device1"`
	Device2   string  `json:"device2"`
	Bandwidth float64 `json:"bandwidth"`
	Latency   float64 `json:"latency"`
	Value     float64 `json:"value"`
	Percent   float64 `json:"percent"`
}

// LinkEstimateResult holds per-link Shapley value breakdown for an operator.
type LinkEstimateResult struct {
	Results    []LinkResult `json:"results"`
	TotalValue float64      `json:"total_value"`
}

// shapleyBinaryPath is the path to the shapley-cli binary.
// Set via init or configuration.
var shapleyBinaryPath = "shapley-cli"

// SetBinaryPath sets the path to the shapley-cli binary.
func SetBinaryPath(path string) {
	shapleyBinaryPath = path
}

// CollapseSmallOperators merges operators with fewer than threshold devices
// into a single "Others" pseudo-operator. Reduces coalition count from 2^n
// to 2^k (where k = surviving operators + 1), making simulation much faster.
func CollapseSmallOperators(input ShapleyInput, threshold int) ShapleyInput {
	deviceCount := make(map[string]int)
	for _, d := range input.Devices {
		deviceCount[d.Operator]++
	}

	small := make(map[string]bool)
	for op, count := range deviceCount {
		if count < threshold {
			small[op] = true
		}
	}

	if len(small) == 0 {
		return input
	}

	newDevices := make([]Device, len(input.Devices))
	for i, d := range input.Devices {
		newDevices[i] = d
		if small[d.Operator] {
			newDevices[i].Operator = operatorOthers
		}
	}

	return ShapleyInput{
		PrivateLinks:     input.PrivateLinks,
		Devices:          newDevices,
		Demands:          input.Demands,
		PublicLinks:      input.PublicLinks,
		OperatorUptime:   input.OperatorUptime,
		ContiguityBonus:  input.ContiguityBonus,
		DemandMultiplier: input.DemandMultiplier,
	}
}

// Simulate runs the Shapley computation on the given input.
func Simulate(ctx context.Context, input ShapleyInput) ([]OperatorValue, error) {
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshal input: %w", err)
	}

	cmd := exec.CommandContext(ctx, shapleyBinaryPath)
	cmd.Stdin = bytes.NewReader(inputJSON)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("shapley-cli: %w: %s", err, stderr.String())
	}

	var results []OperatorValue
	if err := json.Unmarshal(stdout.Bytes(), &results); err != nil {
		return nil, fmt.Errorf("parse output: %w", err)
	}

	return results, nil
}

// SimulatePerCity runs per-source-city Shapley computations and aggregates with
// stake-weighted city weights, matching the official doublezero-offchain system.
func SimulatePerCity(ctx context.Context, baseNetwork ShapleyInput, cityDemands []CityDemandGroup, totalSlots int64, maxConcurrency int) ([]OperatorValue, error) {
	if len(cityDemands) == 0 || totalSlots == 0 {
		return nil, fmt.Errorf("no city demands or zero total slots")
	}

	type cityResult struct {
		results []OperatorValue
		weight  float64
	}

	total := len(cityDemands)
	var done int32
	log.Printf("rewards: starting per-city Shapley for %d cities (concurrency=%d)", total, maxConcurrency)

	results := make([]cityResult, total)
	var mu sync.Mutex
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup
	var firstErr error

	for i, cg := range cityDemands {
		wg.Add(1)
		go func(idx int, group CityDemandGroup) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			// Build input with full topology but only this city's demands.
			cityInput := ShapleyInput{
				PrivateLinks:     baseNetwork.PrivateLinks,
				Devices:          baseNetwork.Devices,
				Demands:          group.Demands,
				PublicLinks:      baseNetwork.PublicLinks,
				OperatorUptime:   baseNetwork.OperatorUptime,
				ContiguityBonus:  baseNetwork.ContiguityBonus,
				DemandMultiplier: baseNetwork.DemandMultiplier,
			}

			res, err := Simulate(ctx, cityInput)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("city %s: %w", group.SourceCity, err)
				}
				mu.Unlock()
				return
			}

			weight := float64(group.TotalSlots) / float64(totalSlots)
			mu.Lock()
			n := int(atomic.AddInt32(&done, 1))
			results[idx] = cityResult{results: res, weight: weight}
			mu.Unlock()

			log.Printf("rewards: per-city Shapley [%d/%d] done: %s (weight=%.4f)", n, total, group.SourceCity, weight)
		}(i, cg)
	}

	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}

	// Aggregate: final[op] = sum(per_city_value[op] * city_weight)
	aggregated := make(map[string]float64)
	for _, cr := range results {
		for _, ov := range cr.results {
			aggregated[ov.Operator] += ov.Value * cr.weight
		}
	}

	// Normalize to proportions.
	var totalValue float64
	for _, v := range aggregated {
		totalValue += v
	}

	var out []OperatorValue
	for op, val := range aggregated {
		prop := 0.0
		if totalValue > 0 {
			prop = val / totalValue
		}
		out = append(out, OperatorValue{
			Operator:   op,
			Value:      val,
			Proportion: prop,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Value > out[j].Value
	})

	return out, nil
}

// GroupDemandsByCity groups demands by their Start (source city) field.
// Used by the Compare endpoint to build CityDemandGroups from a ShapleyInput.
func GroupDemandsByCity(input ShapleyInput) []CityDemandGroup {
	cityMap := make(map[string][]Demand)
	for _, d := range input.Demands {
		cityMap[d.Start] = append(cityMap[d.Start], d)
	}

	var groups []CityDemandGroup
	for city, demands := range cityMap {
		groups = append(groups, CityDemandGroup{
			SourceCity: city,
			Demands:    demands,
			TotalSlots: 1, // equal weight when slot data is unavailable
		})
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].SourceCity < groups[j].SourceCity
	})
	return groups
}

// BuildCompareResult computes deltas between baseline and modified simulation results.
func BuildCompareResult(baselineResults, modifiedResults []OperatorValue) *CompareResult {
	allOps := make(map[string]bool)
	baseMap := make(map[string]OperatorValue)
	var baseTotal float64
	for _, r := range baselineResults {
		baseMap[r.Operator] = r
		allOps[r.Operator] = true
		baseTotal += r.Value
	}
	modMap := make(map[string]OperatorValue)
	var modTotal float64
	for _, r := range modifiedResults {
		modMap[r.Operator] = r
		allOps[r.Operator] = true
		modTotal += r.Value
	}

	sortedOps := make([]string, 0, len(allOps))
	for op := range allOps {
		sortedOps = append(sortedOps, op)
	}
	sort.Strings(sortedOps)

	var deltas []OperatorDelta
	for _, op := range sortedOps {
		bl := baseMap[op]
		md := modMap[op]
		deltas = append(deltas, OperatorDelta{
			Operator:           op,
			BaselineValue:      bl.Value,
			ModifiedValue:      md.Value,
			ValueDelta:         md.Value - bl.Value,
			BaselineProportion: bl.Proportion,
			ModifiedProportion: md.Proportion,
			ProportionDelta:    md.Proportion - bl.Proportion,
		})
	}

	return &CompareResult{
		BaselineResults: baselineResults,
		ModifiedResults: modifiedResults,
		Deltas:          deltas,
		BaselineTotal:   baseTotal,
		ModifiedTotal:   modTotal,
	}
}

// LinkEstimate computes per-link Shapley values for a specific operator.
// cachedBaselineResults, if non-nil, skips the baseline simulation in the approx path.
// This ports the Python network_linkestimate.py logic to Go.
func LinkEstimate(ctx context.Context, operatorFocus string, input ShapleyInput, cachedBaselineResults []OperatorValue) (*LinkEstimateResult, error) {
	// Build device-to-operator lookup
	deviceOperator := make(map[string]string)
	for _, d := range input.Devices {
		deviceOperator[d.Device] = d.Operator
	}

	// Build consolidated link table with operator info, collapsing non-focus operators,
	// and count focus operator links — all in one pass.
	type taggedLink struct {
		Device1   string
		Device2   string
		Latency   float64
		Bandwidth float64
		Uptime    float64
		Shared    string
		Operator1 string
		Operator2 string
		Tagged    bool
	}

	opLinkCount := 0
	var links []taggedLink
	for _, pl := range input.PrivateLinks {
		op1 := deviceOperator[pl.Device1]
		op2 := deviceOperator[pl.Device2]
		if op1 == operatorFocus || op2 == operatorFocus {
			opLinkCount++
		}
		if op1 != operatorFocus && op1 != operatorPublic {
			op1 = operatorOthers
		}
		if op2 != operatorFocus && op2 != operatorPublic {
			op2 = operatorOthers
		}
		links = append(links, taggedLink{
			Device1:   pl.Device1,
			Device2:   pl.Device2,
			Latency:   pl.Latency,
			Bandwidth: pl.Bandwidth,
			Uptime:    pl.Uptime,
			Shared:    pl.Shared,
			Operator1: op1,
			Operator2: op2,
		})
	}

	// Retag links: each link of the focus operator becomes a pseudo-operator (numbered "1", "2", ...),
	// non-focus operators are collapsed, and we set operator_uptime=1.0.
	if opLinkCount > 15 {
		return linkEstimateApprox(ctx, operatorFocus, input, deviceOperator, cachedBaselineResults)
	}

	// isRealDevice: excludes metro aggregate devices, which end with "00".
	// The digit check is intentionally omitted — some real devices have non-numeric names (e.g. "cherydam").
	isRealDevice := func(code string) bool {
		return !strings.HasSuffix(code, "00")
	}

	// Build index for O(1) symmetric link lookup: (d2,d1,bw,latency) -> index
	type linkKey struct {
		d1, d2      string
		bw, latency float64
	}
	symIndex := make(map[linkKey]int, len(links))
	for i, l := range links {
		symIndex[linkKey{l.Device1, l.Device2, l.Bandwidth, l.Latency}] = i
	}

	// Tag focus operator links as pseudo-operators
	counter := 0
	for idx := range links {
		if links[idx].Tagged || (links[idx].Operator1 != operatorFocus && links[idx].Operator2 != operatorFocus) {
			continue
		}

		d1 := links[idx].Device1
		d2 := links[idx].Device2

		if isRealDevice(d1) && isRealDevice(d2) {
			// Find symmetric link via index
			symIdx, hasSym := symIndex[linkKey{d2, d1, links[idx].Bandwidth, links[idx].Latency}]

			counter++
			tag := fmt.Sprintf("%d", counter)

			if links[idx].Operator1 == operatorFocus {
				links[idx].Operator1 = tag
				if hasSym {
					links[symIdx].Operator2 = tag
				}
			}
			if links[idx].Operator2 == operatorFocus {
				links[idx].Operator2 = tag
				if hasSym {
					links[symIdx].Operator1 = tag
				}
			}

			links[idx].Tagged = true
			if hasSym {
				links[symIdx].Tagged = true
			}
		} else {
			// Edge connections — not real inter-metro links
			links[idx].Operator1 = operatorPrivate
			links[idx].Operator2 = operatorPrivate
			links[idx].Tagged = true
		}
	}

	// Now rebuild the input with retagged devices.
	// Each pseudo-operator (numbered tag) needs its devices assigned to it.
	var newDevices []Device
	newDeviceSet := make(map[string]bool)

	for _, l := range links {
		if !newDeviceSet[l.Device1] {
			newDeviceSet[l.Device1] = true
			newDevices = append(newDevices, Device{
				Device:   l.Device1,
				Edge:     100,
				Operator: l.Operator1,
			})
		}
		if !newDeviceSet[l.Device2] {
			newDeviceSet[l.Device2] = true
			op := l.Operator2
			newDevices = append(newDevices, Device{
				Device:   l.Device2,
				Edge:     100,
				Operator: op,
			})
		}
	}

	var newLinks []PrivateLink
	for _, l := range links {
		newLinks = append(newLinks, PrivateLink{
			Device1:   l.Device1,
			Device2:   l.Device2,
			Latency:   l.Latency,
			Bandwidth: l.Bandwidth,
			Uptime:    l.Uptime,
			Shared:    l.Shared,
		})
	}

	retaggedInput := ShapleyInput{
		PrivateLinks:     newLinks,
		Devices:          newDevices,
		Demands:          input.Demands,
		PublicLinks:      input.PublicLinks,
		OperatorUptime:   1.0, // forced for link estimate
		ContiguityBonus:  input.ContiguityBonus,
		DemandMultiplier: input.DemandMultiplier,
	}

	results, err := Simulate(ctx, retaggedInput)
	if err != nil {
		return nil, fmt.Errorf("link estimate simulation: %w", err)
	}

	// Map pseudo-operator results back to links
	dropTags := map[string]bool{operatorPublic: true, operatorPrivate: true, operatorOthers: true}

	// Build schedule: for each link involving the focus operator, find its pseudo-operator value
	type linkInfo struct {
		Device1   string
		Device2   string
		Bandwidth float64
		Latency   float64
		Tag       string
	}

	// Deduplicate by sorted device pair so bidirectional entries appear once,
	// and single-direction entries (some links are stored only one way in the DB) are still included.
	seenPair := make(map[[2]string]bool)
	var schedule []linkInfo
	for _, l := range links {
		if dropTags[l.Operator1] && dropTags[l.Operator2] {
			continue
		}
		d1, d2 := l.Device1, l.Device2
		if d1 > d2 {
			d1, d2 = d2, d1
		}
		key := [2]string{d1, d2}
		if seenPair[key] {
			continue
		}
		seenPair[key] = true
		tag := l.Operator1
		if dropTags[tag] {
			tag = l.Operator2
		}
		schedule = append(schedule, linkInfo{
			Device1:   l.Device1,
			Device2:   l.Device2,
			Bandwidth: l.Bandwidth,
			Latency:   l.Latency,
			Tag:       tag,
		})
	}

	// Build result-value lookup
	valMap := make(map[string]float64)
	for _, r := range results {
		valMap[r.Operator] = r.Value
	}

	var linkResults []LinkResult
	totalValue := 0.0
	for _, s := range schedule {
		v := valMap[s.Tag]
		linkResults = append(linkResults, LinkResult{
			Device1:   s.Device1,
			Device2:   s.Device2,
			Bandwidth: s.Bandwidth,
			Latency:   s.Latency,
			Value:     v,
			Percent:   0, // computed below
		})
		totalValue += math.Max(v, 0)
	}

	// Compute percentages
	for i := range linkResults {
		if totalValue > 0 {
			linkResults[i].Percent = math.Max(linkResults[i].Value, 0) / totalValue
		}
	}

	return &LinkEstimateResult{
		Results:    linkResults,
		TotalValue: totalValue,
	}, nil
}

// linkEstimateApprox uses leave-one-out approximation for operators with >15 links.
// For each link, it removes that link and re-runs the full simulation. The marginal
// value of each link is the difference between the baseline and the reduced network.
// cachedBaseline, if non-nil, skips the initial baseline simulation.
func linkEstimateApprox(ctx context.Context, operatorFocus string, input ShapleyInput, deviceOperator map[string]string, cachedBaseline []OperatorValue) (*LinkEstimateResult, error) {
	// Identify which links belong to the focus operator, deduplicating bidirectional pairs
	type opLink struct {
		index int
		link  PrivateLink
	}
	seen := make(map[string]bool)
	var focusLinks []opLink
	for i, link := range input.PrivateLinks {
		op1 := deviceOperator[link.Device1]
		op2 := deviceOperator[link.Device2]
		if op1 == operatorFocus || op2 == operatorFocus {
			// Deduplicate: use sorted device pair as key
			d1, d2 := link.Device1, link.Device2
			if d1 > d2 {
				d1, d2 = d2, d1
			}
			key := d1 + "|" + d2
			if seen[key] {
				continue
			}
			seen[key] = true
			focusLinks = append(focusLinks, opLink{index: i, link: link})
		}
	}

	// Use cached baseline if available, otherwise run baseline simulation
	baselineResults := cachedBaseline
	if baselineResults == nil {
		var err error
		baselineResults, err = Simulate(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("baseline simulation: %w", err)
		}
	}

	// Find baseline value for the focus operator
	baselineValue := 0.0
	for _, r := range baselineResults {
		if r.Operator == operatorFocus {
			baselineValue = r.Value
			break
		}
	}

	// For each focus link, remove it and re-simulate
	var linkResults []LinkResult
	totalMarginal := 0.0

	for _, fl := range focusLinks {
		// Build input without this link
		reduced := ShapleyInput{
			Devices:          input.Devices,
			Demands:          input.Demands,
			PublicLinks:      input.PublicLinks,
			OperatorUptime:   input.OperatorUptime,
			ContiguityBonus:  input.ContiguityBonus,
			DemandMultiplier: input.DemandMultiplier,
		}
		for i, link := range input.PrivateLinks {
			if i != fl.index {
				reduced.PrivateLinks = append(reduced.PrivateLinks, link)
			}
		}

		reducedResults, err := Simulate(ctx, reduced)
		if err != nil {
			// If removing this link breaks the simulation, assign zero value
			linkResults = append(linkResults, LinkResult{
				Device1:   fl.link.Device1,
				Device2:   fl.link.Device2,
				Bandwidth: fl.link.Bandwidth,
				Latency:   fl.link.Latency,
				Value:     0,
			})
			continue
		}

		reducedValue := 0.0
		for _, r := range reducedResults {
			if r.Operator == operatorFocus {
				reducedValue = r.Value
				break
			}
		}

		marginal := math.Max(baselineValue-reducedValue, 0)
		totalMarginal += marginal

		linkResults = append(linkResults, LinkResult{
			Device1:   fl.link.Device1,
			Device2:   fl.link.Device2,
			Bandwidth: fl.link.Bandwidth,
			Latency:   fl.link.Latency,
			Value:     marginal,
		})
	}

	// Compute percentages
	for i := range linkResults {
		if totalMarginal > 0 {
			linkResults[i].Percent = linkResults[i].Value / totalMarginal
		}
	}

	return &LinkEstimateResult{
		Results:    linkResults,
		TotalValue: totalMarginal,
	}, nil
}
