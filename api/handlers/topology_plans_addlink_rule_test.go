package handlers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Unit tests for the backend WAN/DZX defense-in-depth rule (validateAddLinkEndpointsRule
// + resolveAddLinkEndpointRule), mirroring the frontend's link-type.ts deriveLinkType.
// No HTTP: these exercise the pure helper against fake baselines/changes.

func TestAddLinkEndpointsRule_WANAccepted(t *testing.T) {
	baseline := &TopologyResponse{
		Metros: []Metro{{PK: "m-nyc", Code: "NYC"}, {PK: "m-lon", Code: "LON"}},
		Devices: []Device{
			{PK: "d-a", Code: "nyc-a", ContributorPK: "c-jump", ContributorCode: "jump_", MetroPK: "m-nyc"},
			{PK: "d-z", Code: "lon-b", ContributorPK: "c-jump", ContributorCode: "jump_", MetroPK: "m-lon"},
		},
	}
	idx := newBaselineIndex(baseline, nil)
	a := idx.resolveAddLinkEndpointRule("d-a", "")
	z := idx.resolveAddLinkEndpointRule("d-z", "")
	require.NoError(t, validateAddLinkEndpointsRule(a, z))
}

func TestAddLinkEndpointsRule_DZXAccepted(t *testing.T) {
	baseline := &TopologyResponse{
		Metros: []Metro{{PK: "m-nyc", Code: "NYC"}},
		Devices: []Device{
			{PK: "d-a", Code: "nyc-a", ContributorPK: "c-jump", ContributorCode: "jump_", MetroPK: "m-nyc"},
			{PK: "d-z", Code: "nyc-b", ContributorPK: "c-other", ContributorCode: "other", MetroPK: "m-nyc"},
		},
	}
	idx := newBaselineIndex(baseline, nil)
	a := idx.resolveAddLinkEndpointRule("d-a", "")
	z := idx.resolveAddLinkEndpointRule("d-z", "")
	require.NoError(t, validateAddLinkEndpointsRule(a, z))
}

func TestAddLinkEndpointsRule_SameContributorSameMetroAccepted(t *testing.T) {
	baseline := &TopologyResponse{
		Devices: []Device{
			{PK: "d-a", Code: "nyc-a", ContributorPK: "c-jump", ContributorCode: "jump_", MetroPK: "m-nyc"},
			{PK: "d-z", Code: "nyc-b", ContributorPK: "c-jump", ContributorCode: "jump_", MetroPK: "m-nyc"},
		},
	}
	idx := newBaselineIndex(baseline, nil)
	a := idx.resolveAddLinkEndpointRule("d-a", "")
	z := idx.resolveAddLinkEndpointRule("d-z", "")
	require.NoError(t, validateAddLinkEndpointsRule(a, z))
}

func TestAddLinkEndpointsRule_CrossContributorCrossMetroRejected(t *testing.T) {
	baseline := &TopologyResponse{
		Devices: []Device{
			{PK: "d-a", Code: "nyc-a", ContributorPK: "c-jump", ContributorCode: "jump_", MetroPK: "m-nyc"},
			{PK: "d-z", Code: "lon-b", ContributorPK: "c-other", ContributorCode: "other", MetroPK: "m-lon"},
		},
	}
	idx := newBaselineIndex(baseline, nil)
	a := idx.resolveAddLinkEndpointRule("d-a", "")
	z := idx.resolveAddLinkEndpointRule("d-z", "")
	require.Error(t, validateAddLinkEndpointsRule(a, z))
}

func TestAddLinkEndpointsRule_UnresolvedEndpointSkipped(t *testing.T) {
	baseline := &TopologyResponse{
		Devices: []Device{
			{PK: "d-a", Code: "nyc-a", ContributorPK: "c-jump", ContributorCode: "jump_", MetroPK: "m-nyc"},
		},
	}
	idx := newBaselineIndex(baseline, nil)
	a := idx.resolveAddLinkEndpointRule("d-a", "")
	// "d-unknown" is neither an existing device pk nor a sibling add_device ref.
	z := idx.resolveAddLinkEndpointRule("d-unknown", "")
	require.NoError(t, validateAddLinkEndpointsRule(a, z))
}

// A sibling add_device change (referenced by local_ref, not yet a real device pk)
// resolves the same way an existing device would: same EXISTING contributor
// (linked by contributor_pk, as the payload does when picked from the known
// list) as the baseline device but a brand-new metro -> WAN, accepted.
func TestAddLinkEndpointsRule_SiblingAddDeviceRefResolvesWAN(t *testing.T) {
	baseline := &TopologyResponse{
		Devices: []Device{
			{PK: "d-a", Code: "nyc-a", ContributorPK: "c-jump", ContributorCode: "jump_", MetroPK: "m-nyc"},
		},
	}
	changes := []PlanChange{
		{
			OpType:   OpAddDevice,
			LocalRef: "tmp_dev_1",
			Payload: rawJSON(t, map[string]any{
				"contributor_pk":   "c-jump",
				"contributor_code": "jump_",
				"new_metro":        map[string]any{"code": "LON", "latitude": 51.5, "longitude": -0.1},
			}),
		},
	}
	idx := newBaselineIndex(baseline, changes)
	a := idx.resolveAddLinkEndpointRule("d-a", "")
	z := idx.resolveAddLinkEndpointRule("", "tmp_dev_1")
	require.NoError(t, validateAddLinkEndpointsRule(a, z))
}

// Same sibling add_device pattern, but a different EXISTING contributor in a
// brand-new metro -> cross-contributor AND cross-metro, rejected.
func TestAddLinkEndpointsRule_SiblingAddDeviceRefRejectsCrossCross(t *testing.T) {
	baseline := &TopologyResponse{
		Devices: []Device{
			{PK: "d-a", Code: "nyc-a", ContributorPK: "c-jump", ContributorCode: "jump_", MetroPK: "m-nyc"},
		},
	}
	changes := []PlanChange{
		{
			OpType:   OpAddDevice,
			LocalRef: "tmp_dev_1",
			Payload: rawJSON(t, map[string]any{
				"contributor_pk":   "c-other",
				"contributor_code": "other",
				"new_metro":        map[string]any{"code": "LON", "latitude": 51.5, "longitude": -0.1},
			}),
		},
	}
	idx := newBaselineIndex(baseline, changes)
	a := idx.resolveAddLinkEndpointRule("d-a", "")
	z := idx.resolveAddLinkEndpointRule("", "tmp_dev_1")
	require.Error(t, validateAddLinkEndpointsRule(a, z))
}
