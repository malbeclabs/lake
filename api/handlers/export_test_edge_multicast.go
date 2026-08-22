package handlers

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
