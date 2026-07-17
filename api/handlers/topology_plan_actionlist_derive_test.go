package handlers

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func rawJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// groupTitles indexes an ActionList's groups by contributor code -> task titles.
func groupTitles(al *ActionList) map[string][]string {
	out := map[string][]string{}
	for _, g := range al.Groups {
		for _, task := range g.Tasks {
			out[g.ContributorCode] = append(out[g.ContributorCode], task.Title)
		}
	}
	return out
}

func TestDeriveActionList_SingleOwnerOps(t *testing.T) {
	baseline := &TopologyResponse{
		Metros: []Metro{{PK: "m-lax", Code: "LAX", Name: "Los Angeles"}},
		Devices: []Device{{
			PK: "d-old", Code: "lax001-dz001", Status: "activated",
			MetroPK: "m-lax", ContributorPK: "c-jump", ContributorCode: "jump_",
			UserCount: 5, StakeSol: 1000.0, StakeShare: 2.5,
		}},
	}
	date := "2026-08-01"
	changes := []PlanChange{
		{
			Seq: 20, OpType: OpRemoveDevice, RefDevicePK: "d-old",
			TargetDate: &date, AssigneeNote: "notify NOC", State: StatePending,
		},
		{
			Seq: 10, OpType: OpAddDevice, LocalRef: "tmp_dev_1", State: StatePending,
			Payload: rawJSON(t, map[string]any{
				"contributor_pk": "c-jump", "metro_pk": "m-lax",
				"code": "lax002-dz001", "device_type": "switch",
			}),
		},
	}

	al := deriveActionListFromBaseline(&Plan{Name: "Q3 cleanup"}, changes, baseline)

	require.Len(t, al.Groups, 1)
	g := al.Groups[0]
	require.Equal(t, "jump_", g.ContributorCode)
	require.Equal(t, "#ext-doublezero-jump_", g.SlackChannel)

	// Tasks are emitted in seq order: add_device (10) then remove_device (20).
	require.Len(t, g.Tasks, 2)
	require.Equal(t, "Bring device lax002-dz001 online in LAX", g.Tasks[0].Title)

	rm := g.Tasks[1]
	require.Equal(t, "Decommission device lax001-dz001", rm.Title)
	require.NotNil(t, rm.CurrentUsers)
	require.Equal(t, 5, *rm.CurrentUsers)
	require.NotNil(t, rm.StakeSol)
	require.InDelta(t, 1000.0, *rm.StakeSol, 0.001)
	require.NotNil(t, rm.StakeShare)
	require.InDelta(t, 2.5, *rm.StakeShare, 0.001)
	require.NotNil(t, rm.TargetDate)
	require.Equal(t, "2026-08-01", *rm.TargetDate)
	require.Equal(t, "notify NOC", rm.Note)
}

// TestDeriveActionList_AddDevice_NewContributorAndMetro covers the canonical
// add_device shape for a contributor and metro that don't exist onchain yet:
// no contributor_pk to look up in the baseline, no metro_pk either, so both
// must resolve straight from the payload (contributor_code, new_metro.code).
func TestDeriveActionList_AddDevice_NewContributorAndMetro(t *testing.T) {
	baseline := &TopologyResponse{
		Metros:  []Metro{{PK: "m-lax", Code: "LAX", Name: "Los Angeles"}},
		Devices: []Device{},
	}
	changes := []PlanChange{
		{
			Seq: 10, OpType: OpAddDevice, LocalRef: "tmp_dev_1", State: StatePending,
			Payload: rawJSON(t, map[string]any{
				"contributor_code": "newco", "code": "zzz001-dz001",
				"new_metro": map[string]any{"code": "ZZZ", "latitude": 10.0, "longitude": 20.0},
			}),
		},
	}

	al := deriveActionListFromBaseline(&Plan{Name: "New region"}, changes, baseline)

	require.Len(t, al.Groups, 1)
	g := al.Groups[0]
	require.Equal(t, "newco", g.ContributorCode)
	require.Empty(t, g.ContributorPK, "brand-new contributor has no pk yet")
	require.Equal(t, "#ext-doublezero-newco", g.SlackChannel)
	require.Len(t, g.Tasks, 1)
	require.Equal(t, "Bring device zzz001-dz001 online in ZZZ", g.Tasks[0].Title)
}

// TestDeriveActionList_AddLink_SiblingNewContributor covers an add_link that
// resolves one endpoint to a sibling add_device carrying a brand-new
// contributor (contributor_code only, no pk): the link task must attribute
// that endpoint to the new contributor's code, not leave it blank.
func TestDeriveActionList_AddLink_SiblingNewContributor(t *testing.T) {
	baseline := &TopologyResponse{
		Metros: []Metro{{PK: "m-nyc", Code: "NYC", Name: "New York"}},
		Devices: []Device{
			{PK: "d-z", Code: "nyc001-dz001", Status: "activated", MetroPK: "m-nyc", ContributorPK: "c-tele", ContributorCode: "teleport"},
		},
	}
	changes := []PlanChange{
		{
			Seq: 5, OpType: OpAddDevice, LocalRef: "tmp_dev_1", State: StatePending,
			Payload: rawJSON(t, map[string]any{
				"contributor_code": "newco", "code": "nyc003-dz001",
				"new_metro": map[string]any{"code": "NYC", "latitude": 40.7, "longitude": -74.0},
			}),
		},
		{
			Seq: 10, OpType: OpAddLink, State: StatePending,
			Payload: rawJSON(t, map[string]any{
				"side_a_ref": "tmp_dev_1", "side_z_device_pk": "d-z",
				"link_type": "WAN", "bandwidth_bps": 10000000000, "latency_ns": 5000000,
			}),
		},
	}

	al := deriveActionListFromBaseline(&Plan{Name: "n"}, changes, baseline)

	titles := groupTitles(al)
	require.Contains(t, titles["newco"], "Provision WAN link nyc003-dz001 <-> nyc001-dz001")
	require.Contains(t, titles["teleport"], "Provision WAN link nyc003-dz001 <-> nyc001-dz001")
}

func TestDeriveActionList_DualEndpointOps(t *testing.T) {
	baseline := &TopologyResponse{
		Metros: []Metro{{PK: "m-nyc", Code: "NYC", Name: "New York"}},
		Devices: []Device{
			{PK: "d-z", Code: "nyc001-dz001", Status: "activated", MetroPK: "m-nyc", ContributorPK: "c-tele", ContributorCode: "teleport"},
			// A latitude-owned device so contributor code resolves for the sibling add_device.
			{PK: "d-lat", Code: "nyc009-dz001", Status: "activated", MetroPK: "m-nyc", ContributorPK: "c-lat", ContributorCode: "latitude"},
		},
		Links: []Link{{
			PK: "l-1", Code: "lax001-dz001:nyc001-dz001", Status: "activated", LinkType: "DZX",
			SideAPK: "d-a", SideAContributorPK: "c-jump", SideAContributorCode: "jump_",
			SideZPK: "d-z", SideZContributorPK: "c-tele", SideZContributorCode: "teleport",
		}},
	}

	t.Run("remove_link hits both endpoint contributors", func(t *testing.T) {
		changes := []PlanChange{{Seq: 10, OpType: OpRemoveLink, RefLinkPK: "l-1", State: StatePending}}
		al := deriveActionListFromBaseline(&Plan{Name: "n"}, changes, baseline)

		titles := groupTitles(al)
		require.Equal(t, []string{"Remove link lax001-dz001:nyc001-dz001"}, titles["jump_"])
		require.Equal(t, []string{"Remove link lax001-dz001:nyc001-dz001"}, titles["teleport"])

		for _, g := range al.Groups {
			require.Equal(t, []string{"jump_", "teleport"}, g.Tasks[0].InvolvedContributors)
		}
	})

	t.Run("add_link resolves a sibling create-op via local_ref", func(t *testing.T) {
		changes := []PlanChange{
			{
				Seq: 5, OpType: OpAddDevice, LocalRef: "tmp_dev_1", State: StatePending,
				Payload: rawJSON(t, map[string]any{
					"contributor_pk": "c-lat", "metro_pk": "m-nyc",
					"code": "nyc003-dz001", "device_type": "switch",
				}),
			},
			{
				Seq: 10, OpType: OpAddLink, State: StatePending,
				Payload: rawJSON(t, map[string]any{
					"side_a_ref": "tmp_dev_1", "side_z_device_pk": "d-z",
					"link_type": "WAN", "bandwidth_bps": 10000000000, "latency_ns": 5000000,
				}),
			},
		}
		al := deriveActionListFromBaseline(&Plan{Name: "n"}, changes, baseline)

		titles := groupTitles(al)
		require.Contains(t, titles["latitude"], "Provision WAN link nyc003-dz001 <-> nyc001-dz001")
		require.Contains(t, titles["teleport"], "Provision WAN link nyc003-dz001 <-> nyc001-dz001")
	})
}

func TestDeriveActionList_MoveLinkEndCrossContributor(t *testing.T) {
	// DZX link l-1: side A owned by jump_, side Z owned by teleport.
	// Move side A onto device d-c, which is owned by latitude.
	baseline := &TopologyResponse{
		Metros: []Metro{
			{PK: "m-lax", Code: "LAX", Name: "Los Angeles"},
			{PK: "m-nyc", Code: "NYC", Name: "New York"},
		},
		Devices: []Device{
			{PK: "d-a", Code: "lax001-dz001", Status: "activated", MetroPK: "m-lax", ContributorPK: "c-jump", ContributorCode: "jump_"},
			{PK: "d-z", Code: "nyc001-dz001", Status: "activated", MetroPK: "m-nyc", ContributorPK: "c-tele", ContributorCode: "teleport"},
			{PK: "d-c", Code: "nyc002-dz001", Status: "activated", MetroPK: "m-nyc", ContributorPK: "c-lat", ContributorCode: "latitude"},
		},
		Links: []Link{{
			PK: "l-1", Code: "lax001-dz001:nyc001-dz001", Status: "activated", LinkType: "DZX",
			SideAPK: "d-a", SideAContributorPK: "c-jump", SideAContributorCode: "jump_",
			SideZPK: "d-z", SideZContributorPK: "c-tele", SideZContributorCode: "teleport",
		}},
	}
	changes := []PlanChange{{
		Seq: 10, OpType: OpMoveLinkEnd, RefLinkPK: "l-1", NewDevicePK: "d-c", State: StatePending,
		Payload: rawJSON(t, map[string]any{
			"side": "a", "new_iface_name": "Ethernet1",
			"latency_ns": 5000000, "bandwidth_bps": 10000000000,
		}),
	}}

	al := deriveActionListFromBaseline(&Plan{Name: "n"}, changes, baseline)

	// Groups are sorted by contributor code: jump_, latitude, teleport.
	require.Len(t, al.Groups, 3)
	require.Equal(t, "jump_", al.Groups[0].ContributorCode)
	require.Equal(t, "latitude", al.Groups[1].ContributorCode)
	require.Equal(t, "teleport", al.Groups[2].ContributorCode)

	wantTitle := "jump_ ↔ latitude: coordinate moving DZX link lax001-dz001:nyc001-dz001 to device nyc002-dz001"
	for _, g := range al.Groups {
		require.Len(t, g.Tasks, 1, "each involved contributor gets exactly the coordination task")
		require.Equal(t, wantTitle, g.Tasks[0].Title)
		require.Equal(t, []string{"jump_", "latitude", "teleport"}, g.Tasks[0].InvolvedContributors)
	}
	// The other-side owner (teleport) also carries the coordination task.
	require.Equal(t, "#ext-doublezero-teleport", al.Groups[2].SlackChannel)
}

func TestRenderActionListMarkdown(t *testing.T) {
	baseline := &TopologyResponse{
		Metros: []Metro{{PK: "m-lax", Code: "LAX", Name: "Los Angeles"}},
		Devices: []Device{{
			PK: "d-old", Code: "lax001-dz001", Status: "activated",
			MetroPK: "m-lax", ContributorPK: "c-jump", ContributorCode: "jump_",
			UserCount: 5, StakeSol: 1000.0, StakeShare: 2.5,
		}},
	}
	date := "2026-08-01"
	changes := []PlanChange{{
		Seq: 10, OpType: OpRemoveDevice, RefDevicePK: "d-old",
		TargetDate: &date, AssigneeNote: "notify NOC", State: StatePending,
	}}

	al := deriveActionListFromBaseline(&Plan{Name: "Q3 cleanup"}, changes, baseline)

	wantGroup := "## jump_ (#ext-doublezero-jump_)\n\n" +
		"- [ ] **Decommission device lax001-dz001**\n" +
		"  - Current users: 5, stake: 1000.0 SOL (2.50%)\n" +
		"  - Target date: 2026-08-01\n" +
		"  - Note: notify NOC\n"
	require.Equal(t, wantGroup, al.Groups[0].Markdown)

	wantFull := "# Topology plan: Q3 cleanup\n\n" + wantGroup
	require.Equal(t, wantFull, al.Markdown)
}

func TestRenderActionListMarkdown_SanitizesUserText(t *testing.T) {
	baseline := &TopologyResponse{
		Metros: []Metro{{PK: "m-lax", Code: "LAX", Name: "Los Angeles"}},
		Devices: []Device{{
			PK: "d-old", Code: "lax001-dz001", Status: "activated",
			MetroPK: "m-lax", ContributorPK: "c-jump", ContributorCode: "jump_",
			UserCount: 5, StakeSol: 1000.0, StakeShare: 2.5,
		}},
	}
	date := "2026-08-01"
	// A malicious note: raw newlines, a code fence, and lines that would look like
	// a new heading and a new list item if the newlines survived.
	changes := []PlanChange{
		{
			Seq: 10, OpType: OpRemoveDevice, RefDevicePK: "d-old",
			TargetDate:   &date,
			AssigneeNote: "notify NOC\n```\n# not a heading\n- not an item",
			State:        StatePending,
		},
		{
			Seq: 20, OpType: OpAddDevice, LocalRef: "tmp_1", State: StatePending,
			Payload: rawJSON(t, map[string]any{
				"contributor_pk": "c-jump", "metro_pk": "m-lax",
				"code": "lax002-dz001", "device_type": "switch",
			}),
		},
	}

	al := deriveActionListFromBaseline(&Plan{Name: "Q3 cleanup"}, changes, baseline)

	// The note collapses to one physical line (backticks escaped), so the code
	// fence is neutralized and the following task stays a separate, clean list item.
	wantGroup := "## jump_ (#ext-doublezero-jump_)\n\n" +
		"- [ ] **Decommission device lax001-dz001**\n" +
		"  - Current users: 5, stake: 1000.0 SOL (2.50%)\n" +
		"  - Target date: 2026-08-01\n" +
		"  - Note: notify NOC \\`\\`\\` # not a heading - not an item\n" +
		"- [ ] **Bring device lax002-dz001 online in LAX**\n"
	require.Equal(t, wantGroup, al.Groups[0].Markdown)

	// No raw newline from the note leaks into the document, and no unescaped
	// triple-backtick can open a code fence.
	require.NotContains(t, al.Markdown, "notify NOC\n")
	require.NotContains(t, al.Markdown, "```")
	require.Contains(t, al.Markdown, wantGroup)
}
