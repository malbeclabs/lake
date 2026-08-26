package handlers

import "time"

// EdgeMulticastGroupForTest describes one group to EdgeMulticastPathParityForTest.
type EdgeMulticastGroupForTest struct {
	PK          string
	Code        string
	MulticastIP string
}

// EdgeMulticastPathParityForTest exposes the path-parity measurement to the external test package,
// keyed "<group pk>|<publisher address>". The measurement is pure and worth testing without a
// database standing in the way; the production caller reads the same function through the cached
// observations payload.
func EdgeMulticastPathParityForTest(groups []EdgeMulticastGroupForTest, series []EdgeMulticastObservationSeries) map[string]*EdgeMulticastPathParity {
	catalog := make([]MulticastDeliveryGroup, 0, len(groups))
	for _, g := range groups {
		catalog = append(catalog, MulticastDeliveryGroup{PK: g.PK, Code: g.Code, MulticastIP: g.MulticastIP})
	}
	out := map[string]*EdgeMulticastPathParity{}
	for key, parity := range edgeMulticastPathParity(series, newEdgeMulticastCaptureSourceMap(catalog)) {
		out[key.groupPK+"|"+key.ip] = parity
	}
	return out
}

// EdgeMulticastPathRatesForTest exposes the recorded message rate to the external test package,
// keyed "<group pk>|<publisher address>".
func EdgeMulticastPathRatesForTest(groups []EdgeMulticastGroupForTest, series []EdgeMulticastObservationSeries, windowMinutes int) map[string]float64 {
	catalog := make([]MulticastDeliveryGroup, 0, len(groups))
	for _, g := range groups {
		catalog = append(catalog, MulticastDeliveryGroup{PK: g.PK, Code: g.Code, MulticastIP: g.MulticastIP})
	}
	out := map[string]float64{}
	for key, rate := range edgeMulticastPathRates(series, newEdgeMulticastCaptureSourceMap(catalog), windowMinutes) {
		out[key.groupPK+"|"+key.ip] = rate
	}
	return out
}

// EdgeMulticastPublisherHealthForTest exposes the per-publisher verdict to the external test
// package. The verdict is pure — a line plus whether the group has any recorded series at all —
// and its ranking is the page's whole contract, so it is worth pinning without a database in the
// way.
func EdgeMulticastPublisherHealthForTest(line EdgeMulticastPublisher, groupHasSeries bool) string {
	// Subscribed by default: every case this shim is used for is a group somebody records, which
	// is the ordinary state. The unsubscribed case has its own shim below so it cannot be reached
	// by accident.
	return edgeMulticastPublisherHealth(line, groupHasSeries, true)
}

// EdgeMulticastPublisherHealthUnsubscribedForTest grades a line on a group with NO subscriber,
// where a shared tunnel counter is the only evidence there is and nothing can ever settle it.
func EdgeMulticastPublisherHealthUnsubscribedForTest(line EdgeMulticastPublisher, groupHasSeries bool) string {
	return edgeMulticastPublisherHealth(line, groupHasSeries, false)
}

// EdgeMulticastPublisherStatus values, for tests that need to build a line in a given counter
// state without importing the constants.
const (
	EdgeMulticastPubPublishingForTest = edgeMulticastPubPublishing
	EdgeMulticastPubThinForTest       = edgeMulticastPubThin
	EdgeMulticastPubIdleForTest       = edgeMulticastPubIdle
	EdgeMulticastPubUnknownForTest    = edgeMulticastPubUnknown
)

// EdgeMulticastSequenceHealthForTest grades a set of channel instances the way the fold does:
// each instance's status from its own last-seen against the payload's clock, then the quiet-
// capture-source demotion, then the tally. Exposed because those three steps are one contract —
// the demotion only means anything through what the tally does with it — and because both are
// pure, so pinning them needs no database.
func EdgeMulticastSequenceHealthForTest(instances []EdgeMulticastChannelInstance, asOf time.Time) *EdgeMulticastSequenceHealth {
	health := &EdgeMulticastSequenceHealth{}
	for _, inst := range instances {
		inst.Status = edgeMulticastSequenceStatus(inst.GapBooks, inst.LastSeen, asOf)
		health.Instances = append(health.Instances, inst)
	}
	demoteEdgeMulticastQuietCaptureSources(health)
	finishEdgeMulticastSequenceHealth(health)
	return health
}

// EdgeMulticastNodeCoverageForTest exposes the recorder-side check to the external test package,
// keyed by group pk. Same shape as EdgeMulticastPathParityForTest and for the same reason: the
// measurement is pure, and the pair of checks only means anything if each is pinned against the
// case the other is meant to catch.
func EdgeMulticastNodeCoverageForTest(groups []EdgeMulticastGroupForTest, series []EdgeMulticastObservationSeries) map[string]*EdgeMulticastRecorderCoverage {
	catalog := make([]MulticastDeliveryGroup, 0, len(groups))
	for _, g := range groups {
		catalog = append(catalog, MulticastDeliveryGroup{PK: g.PK, Code: g.Code, MulticastIP: g.MulticastIP})
	}
	return edgeMulticastNodeCoverage(series, newEdgeMulticastCaptureSourceMap(catalog))
}

// EdgeMulticastRecorderLossFoldForTest exposes the recorder-loss fold to the external test
// package. The fold is pure and carries the rule the page is judged on — a loss at one recorder is
// its branch, a loss at two or more is not — so it is worth testing without a database in the way.
func EdgeMulticastRecorderLossFoldForTest(series []EdgeMulticastRecorderLossSeries) (map[string][]EdgeMulticastRecorderLoss, map[string][]KalshiL2GapEpisode) {
	return edgeMulticastRecorderLossFold(series)
}

// EdgeMulticastRecorderLossLineKeyForTest builds the key the fold's maps are addressed by, so a
// test does not have to hardcode the separator.
func EdgeMulticastRecorderLossLineKeyForTest(multicastGroup, publisherSourceIP string) string {
	return edgeMulticastRecorderLossLineKey(multicastGroup, publisherSourceIP)
}

// EdgeMulticastAllPathsGappedForTest exposes the "every path lost at once" intersection to the
// external test package. It is the only sequence finding that belongs to the group rather than to a
// line, so its keying is worth pinning without a database in the way.
func EdgeMulticastAllPathsGappedForTest(instances []EdgeMulticastChannelInstance) []KalshiL2GapEpisode {
	return edgeMulticastAllPathsGapped(instances)
}

// EdgeMulticastFamilyOfForTest exposes the group-code family used to key a section for a group no
// feed row claims.
func EdgeMulticastFamilyOfForTest(code string) string {
	return edgeMulticastFamilyOf(code)
}
