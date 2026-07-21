package handlers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBuildPlannedIssues_DecomAndFilteredContributorSpecs exercises the full
// spec builder against decomFixtureBaseline (shared with the E1 decom-issue
// tests): dev-b is removed (owned solely by contrib-x), link-2 is removed
// (cross-contributor: contrib-x <-> contrib-y), and contrib-x also brings a
// new device online. The action list is built the same way production does
// (deriveActionListFromBaseline from the same plan + baseline) so tasks line
// up exactly with what collectDecomTargets resolves.
func TestBuildPlannedIssues_DecomAndFilteredContributorSpecs(t *testing.T) {
	baseline := decomFixtureBaseline()
	plan := &Plan{
		Name: "Q3 decom",
		Changes: []PlanChange{
			{Seq: 10, OpType: OpRemoveDevice, RefDevicePK: "dev-b-pk", State: StatePending},
			{Seq: 20, OpType: OpRemoveLink, RefLinkPK: "link-2-pk", State: StatePending},
			{
				Seq: 30, OpType: OpAddDevice, LocalRef: "new-dev-ref", State: StatePending,
				Payload: rawJSON(t, map[string]any{
					"contributor_pk": "contrib-x-pk", "contributor_code": "contrib-x",
					"code": "dev-new", "metro_pk": "mtr-a-pk",
				}),
			},
		},
	}
	al := deriveActionListFromBaseline(plan, plan.Changes, baseline)

	specs := buildPlannedIssues(plan, al, baseline, "https://data.malbeclabs.com/topology/planner?plan=x", nil)

	var deviceDecoms, linkDecoms, contributors []plannedIssue
	for _, s := range specs {
		switch s.Kind {
		case kindDeviceDecom:
			deviceDecoms = append(deviceDecoms, s)
		case kindLinkDecom:
			linkDecoms = append(linkDecoms, s)
		case kindContributor:
			contributors = append(contributors, s)
		}
	}

	// Exactly one device decom, keyed on the removed device's pk.
	require.Len(t, deviceDecoms, 1)
	require.Equal(t, "dev-b-pk", deviceDecoms[0].EntityPK)
	require.Contains(t, deviceDecoms[0].Labels, "contributor-decommission")

	// Exactly one link decom, keyed on the removed link's pk.
	require.Len(t, linkDecoms, 1)
	require.Equal(t, "link-2-pk", linkDecoms[0].EntityPK)
	require.Contains(t, linkDecoms[0].Labels, "contributor-decommission")

	// contrib-x has non-removal work (the add_device) alongside its removals,
	// so it gets exactly one contributor issue whose body contains the add
	// task but not either removal task.
	require.Len(t, contributors, 1)
	cx := contributors[0]
	require.Equal(t, "contrib-x-pk", cx.ContributorPK)
	require.Equal(t, "contrib-x", cx.ContributorCode)
	require.Contains(t, cx.Body, "dev-new")
	require.NotContains(t, cx.Body, "Decommission device dev-b")
	require.NotContains(t, cx.Body, "Remove link link-2")

	// contrib-y's only change is the removal (the far end of link-2), so it
	// gets no per-contributor issue at all: its work is fully covered by the
	// link decom issue.
	for _, c := range contributors {
		require.NotEqual(t, "contrib-y", c.ContributorCode)
	}
}

// TestBuildPlannedIssues_NoChanges proves an empty plan (no changes at all)
// produces no decom and no contributor specs.
func TestBuildPlannedIssues_NoChanges(t *testing.T) {
	baseline := decomFixtureBaseline()
	plan := &Plan{Name: "Empty plan"}
	al := deriveActionListFromBaseline(plan, plan.Changes, baseline)

	specs := buildPlannedIssues(plan, al, baseline, "https://data.malbeclabs.com/topology/planner?plan=x", nil)
	require.Empty(t, specs)
}
