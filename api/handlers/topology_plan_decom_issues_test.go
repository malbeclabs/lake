package handlers

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// decomFixtureBaseline builds a small, obviously-fake baseline shared by the
// decom-issue tests:
//
//	dev-a (contrib-x, mtr-a) --link-1 (WAN)--> dev-b (contrib-x, mtr-b)
//	dev-a (contrib-x, mtr-a) --link-2 (DZX)--> dev-c (contrib-y, mtr-c)
//
// link-1 is same-contributor on both ends; link-2 is cross-contributor and a
// DZX link, so it exercises both the cross-contributor and DZX-exposure paths.
func decomFixtureBaseline() *TopologyResponse {
	return &TopologyResponse{
		Metros: []Metro{
			{PK: "mtr-a-pk", Code: "mtr-a", Name: "Metro Alpha"},
			{PK: "mtr-b-pk", Code: "mtr-b", Name: "mtr-b"}, // name == code
			{PK: "mtr-c-pk", Code: "mtr-c", Name: "Metro Charlie"},
		},
		Devices: []Device{
			{
				PK: "dev-a-pk", Code: "dev-a", MetroPK: "mtr-a-pk",
				ContributorPK: "contrib-x-pk", ContributorCode: "contrib-x",
				UserCount:  12,
				Interfaces: []DeviceInterface{{Name: "Ethernet2"}, {Name: "Ethernet1"}},
			},
			{
				PK: "dev-b-pk", Code: "dev-b", MetroPK: "mtr-b-pk",
				ContributorPK: "contrib-x-pk", ContributorCode: "contrib-x",
				UserCount: 3,
			},
			{
				PK: "dev-c-pk", Code: "dev-c", MetroPK: "mtr-c-pk",
				ContributorPK: "contrib-y-pk", ContributorCode: "contrib-y",
				UserCount: 7,
			},
		},
		Links: []Link{
			{
				PK: "link-1-pk", Code: "link-1", LinkType: "WAN", BandwidthBps: 100_000_000_000,
				SideAPK: "dev-a-pk", SideACode: "dev-a", SideAIfaceName: "Ethernet1",
				SideZPK: "dev-b-pk", SideZCode: "dev-b", SideZIfaceName: "Ethernet1",
				ContributorPK: "contrib-x-pk", ContributorCode: "contrib-x",
				SideAContributorPK: "contrib-x-pk", SideAContributorCode: "contrib-x",
				SideZContributorPK: "contrib-x-pk", SideZContributorCode: "contrib-x",
			},
			{
				PK: "link-2-pk", Code: "link-2", LinkType: "DZX", BandwidthBps: 10_000_000_000,
				SideAPK: "dev-a-pk", SideACode: "dev-a", SideAIfaceName: "Ethernet2",
				SideZPK: "dev-c-pk", SideZCode: "dev-c", SideZIfaceName: "Ethernet1",
				ContributorPK: "contrib-x-pk", ContributorCode: "contrib-x",
				SideAContributorPK: "contrib-x-pk", SideAContributorCode: "contrib-x",
				SideZContributorPK: "contrib-y-pk", SideZContributorCode: "contrib-y",
			},
		},
	}
}

// decomFixtureNames is a fake contributor code->name map: contrib-x resolves
// to a display name, contrib-y deliberately has no entry so tests can assert
// the code-only fallback.
func decomFixtureNames() map[string]string {
	return map[string]string{"contrib-x": "Contributor X"}
}

func TestCollectDecomTargets_Device_ResolvesAttachedLinksAndCounts(t *testing.T) {
	baseline := decomFixtureBaseline()
	plan := &Plan{
		Name: "Q3 decom",
		Changes: []PlanChange{
			{Seq: 10, OpType: OpRemoveDevice, RefDevicePK: "dev-a-pk", State: StatePending},
		},
	}

	devices, links := collectDecomTargets(plan, baseline, decomFixtureNames())
	require.Empty(t, links)
	require.Len(t, devices, 1)

	d := devices[0]
	require.True(t, d.Resolved)
	require.Equal(t, "dev-a-pk", d.DevicePK)
	require.Equal(t, "dev-a", d.DeviceCode)
	require.Equal(t, "contrib-x", d.ContributorCode)
	require.Equal(t, "Contributor X", d.ContributorName)
	require.Equal(t, "Metro Alpha", d.MetroCity)
	require.Equal(t, []string{"Ethernet1", "Ethernet2"}, d.Interfaces)

	require.Len(t, d.AttachedLinks, 2)
	require.Equal(t, "link-1", d.AttachedLinks[0].LinkCode)
	require.EqualValues(t, 100_000_000_000, d.AttachedLinks[0].BandwidthBps)
	require.Equal(t, "WAN", d.AttachedLinks[0].LinkType)
	require.False(t, d.AttachedLinks[0].CrossContributor)
	require.Equal(t, "contrib-x", d.AttachedLinks[0].OtherContribCode)

	require.Equal(t, "link-2", d.AttachedLinks[1].LinkCode)
	require.Equal(t, "DZX", d.AttachedLinks[1].LinkType)
	require.True(t, d.AttachedLinks[1].CrossContributor)
	require.Equal(t, "contrib-y", d.AttachedLinks[1].OtherContribCode)

	require.Equal(t, []string{"contrib-y"}, d.CrossContribCodes)
}

func TestCollectDecomTargets_Device_ContributorNameFallsBackToCodeWhenUnknown(t *testing.T) {
	baseline := decomFixtureBaseline()
	plan := &Plan{
		Changes: []PlanChange{
			{Seq: 10, OpType: OpRemoveDevice, RefDevicePK: "dev-c-pk", State: StatePending},
		},
	}

	// dev-c's contributor (contrib-y) has no entry in decomFixtureNames.
	devices, _ := collectDecomTargets(plan, baseline, decomFixtureNames())
	require.Len(t, devices, 1)
	require.Equal(t, "contrib-y", devices[0].ContributorCode)
	require.Equal(t, "contrib-y", devices[0].ContributorName)
}

func TestCollectDecomTargets_NilNamesMapDegradesToCodeOnly(t *testing.T) {
	baseline := decomFixtureBaseline()
	plan := &Plan{
		Changes: []PlanChange{
			{Seq: 10, OpType: OpRemoveDevice, RefDevicePK: "dev-a-pk", State: StatePending},
		},
	}

	devices, _ := collectDecomTargets(plan, baseline, nil)
	require.Len(t, devices, 1)
	require.Equal(t, "contrib-x", devices[0].ContributorName)
}

func TestCollectDecomTargets_Link_ResolvesCrossContributorEndpoints(t *testing.T) {
	baseline := decomFixtureBaseline()
	plan := &Plan{
		Name: "Q3 decom",
		Changes: []PlanChange{
			{Seq: 10, OpType: OpRemoveLink, RefLinkPK: "link-2-pk", State: StatePending},
		},
	}

	devices, links := collectDecomTargets(plan, baseline, decomFixtureNames())
	require.Empty(t, devices)
	require.Len(t, links, 1)

	l := links[0]
	require.True(t, l.Resolved)
	require.Equal(t, "link-2-pk", l.LinkPK)
	require.Equal(t, "link-2", l.LinkCode)
	require.Equal(t, "DZX", l.LinkType)
	require.EqualValues(t, 10_000_000_000, l.BandwidthBps)
	require.Equal(t, "contrib-x", l.OwnerContribCode)
	require.Equal(t, "Contributor X", l.OwnerContribName)

	require.Equal(t, "dev-a", l.SideADeviceCode)
	require.Equal(t, "Metro Alpha", l.SideAMetroCity)
	require.Equal(t, "contrib-x", l.SideAContribCode)
	require.Equal(t, "Ethernet2", l.SideAIface)
	require.EqualValues(t, 12, l.SideAUsers)

	require.Equal(t, "dev-c", l.SideZDeviceCode)
	require.Equal(t, "Metro Charlie", l.SideZMetroCity)
	require.Equal(t, "contrib-y", l.SideZContribCode)
	require.Equal(t, "Ethernet1", l.SideZIface)
	require.EqualValues(t, 7, l.SideZUsers)

	require.True(t, l.CrossContributor)
}

func TestCollectDecomTargets_SkipsSkippedAndSupersededAndResolvesDrift(t *testing.T) {
	baseline := decomFixtureBaseline()
	plan := &Plan{
		Name: "Q3 decom",
		Changes: []PlanChange{
			{Seq: 10, OpType: OpRemoveDevice, RefDevicePK: "dev-skip-pk", State: StateSkipped},
			{Seq: 20, OpType: OpRemoveDevice, RefDevicePK: "dev-superseded-pk", State: StateSuperseded},
			{
				Seq: 30, OpType: OpRemoveDevice, RefDevicePK: "dev-ghost-pk", State: StatePending,
				RefSnapshot: rawJSON(t, map[string]any{
					"device_code": "dev-ghost", "contributor_code": "contrib-z",
				}),
			},
		},
	}

	devices, links := collectDecomTargets(plan, baseline, decomFixtureNames())
	require.Empty(t, links)
	require.Len(t, devices, 1)

	d := devices[0]
	require.False(t, d.Resolved)
	require.Equal(t, "dev-ghost-pk", d.DevicePK)
	require.Equal(t, "dev-ghost", d.DeviceCode)
	require.Equal(t, "contrib-z", d.ContributorCode)
	require.Equal(t, "contrib-z", d.ContributorName) // no name known for contrib-z: falls back to code
	require.Empty(t, d.AttachedLinks)
	require.Empty(t, d.Interfaces)
}

func TestRenderDeviceDecomTitle(t *testing.T) {
	target := DeviceDecomTarget{
		ContributorCode: "contrib-x",
		ContributorName: "Contributor X",
		MetroCity:       "Metro Alpha",
		DeviceCode:      "dev-a",
	}

	title := renderDeviceDecomTitle(target)
	require.Equal(t, "[Decom] Contributor X — Metro Alpha (dev-a) — TBD", title)
	require.True(t, strings.Contains(title, " — "), "decom titles deliberately use an em dash to match the infra convention")

	date := "2026-09-01"
	target.Change = PlanChange{TargetDate: &date}
	title = renderDeviceDecomTitle(target)
	require.Equal(t, "[Decom] Contributor X — Metro Alpha (dev-a) — 2026-09-01", title)
	require.True(t, strings.Contains(title, " — "))
}

func TestRenderLinkDecomTitle(t *testing.T) {
	target := LinkDecomTarget{
		OwnerContribCode: "contrib-x",
		OwnerContribName: "Contributor X",
		SideADeviceCode:  "dev-a",
		SideZDeviceCode:  "dev-c",
	}

	title := renderLinkDecomTitle(target)
	require.Equal(t, "[Decom-Link] Contributor X — dev-a:dev-c — TBD", title)
	require.True(t, strings.Contains(title, " — "))

	date := "2026-09-01"
	target.Change = PlanChange{TargetDate: &date}
	title = renderLinkDecomTitle(target)
	require.Equal(t, "[Decom-Link] Contributor X — dev-a:dev-c — 2026-09-01", title)
	require.True(t, strings.Contains(title, " — "))
}

func TestRenderDeviceDecomBody_Resolved(t *testing.T) {
	baseline := decomFixtureBaseline()
	date := "2026-09-01"
	plan := &Plan{
		Name: "Q3 decom",
		Changes: []PlanChange{
			{
				Seq: 10, OpType: OpRemoveDevice, RefDevicePK: "dev-a-pk", State: StatePending,
				TargetDate: &date,
			},
		},
	}
	devices, _ := collectDecomTargets(plan, baseline, decomFixtureNames())
	require.Len(t, devices, 1)
	devices[0].Change.TargetDate = &date

	body := renderDeviceDecomBody(devices[0])

	// Section structure, matching the real infra issue exactly.
	require.Contains(t, body, "## Summary")
	require.Contains(t, body, "## Pre-decom investigation")
	require.Contains(t, body, "## Timeline (decom date: 2026-09-01)")
	require.Contains(t, body, "## Post-decom verification (DZ)")

	require.Contains(t, body, "- Contributor: Contributor X (`contrib-x`)")
	require.Contains(t, body, "- Device: `dev-a` (`dev-a-pk`)")
	require.Contains(t, body, "- Type: Device decommission")
	require.Contains(t, body, "- Decommission date: 2026-09-01")

	require.Contains(t, body, "- Links on the device: `link-1` (100G WAN), `link-2` (10G DZX); peer devices stay")
	require.Contains(t, body, "- Interfaces: Ethernet1, Ethernet2")
	require.Contains(t, body, "- Cross-contributor / DZX exposure: contributors: `contrib-y`; includes DZX link(s)")
	require.Contains(t, body, "- Physical switch disposition: (confirm)")
	require.Contains(t, body, "- [ ] Maintenance event created in OPS portal for the decom date")

	require.Contains(t, body, "### T-31 days: Cap (Contributor)")
	require.Contains(t, body, "- [ ] `device update --max-users 0`")
	require.Contains(t, body, "### T-14 days: Notice (User team)")
	require.Contains(t, body, "- [ ] User team contacts connected users to migrate to another DZD")
	require.Contains(t, body, "### T-1 day (2026-08-31): DZ prep")
	require.Contains(t, body, "- [ ] Engineering disables the device in the shred program (no active seats, safe)")
	require.Contains(t, body, "- [ ] Remove any straggler users")
	require.Contains(t, body, "### Decom day (2026-09-01): Teardown (Contributor, in order)")
	require.Contains(t, body, "- Soft-drain then hard-drain each link")
	require.Contains(t, body, "- Delete each link")
	require.Contains(t, body, "- Delete each interface")
	require.Contains(t, body, "- Drain the device")
	require.Contains(t, body, "- Delete the device")

	require.Contains(t, body, "- [ ] Device gone from `device list`; links gone from `link list`; interfaces removed")
	require.Contains(t, body, "- [ ] Shred oracle healthy and settling (no wedge)")
	require.Contains(t, body, "- [ ] Physical switch return handled")

	require.False(t, strings.Contains(body, "—"), "em dash is reserved for the title, never the body")
}

func TestRenderDeviceDecomBody_NoFabricatedUserCounts(t *testing.T) {
	baseline := decomFixtureBaseline() // dev-a has 12 users
	plan := &Plan{
		Changes: []PlanChange{
			{Seq: 10, OpType: OpRemoveDevice, RefDevicePK: "dev-a-pk", State: StatePending},
		},
	}
	devices, _ := collectDecomTargets(plan, baseline, decomFixtureNames())
	require.Len(t, devices, 1)

	body := renderDeviceDecomBody(devices[0])

	require.Contains(t, body, "- [ ] User team contacts connected users to migrate to another DZD")
	require.NotContains(t, body, "12")
	require.NotContains(t, body, "connected)")
}

func TestRenderDeviceDecomBody_Drift(t *testing.T) {
	target := DeviceDecomTarget{
		Resolved:        false,
		DevicePK:        "dev-ghost-pk",
		DeviceCode:      "dev-ghost",
		ContributorCode: "contrib-z",
	}

	body := renderDeviceDecomBody(target)

	require.Contains(t, body, "- Links on the device: unknown (drift)")
	require.Contains(t, body, "- Interfaces: unknown (drift)")
	require.Contains(t, body, "- Cross-contributor / DZX exposure: unknown (drift)")
	require.Contains(t, body, "- Physical switch disposition: (confirm)")
	require.Contains(t, body, "## Timeline")
	require.Contains(t, body, "## Post-decom verification (DZ)")
	require.Contains(t, body, "- [ ] `device update --max-users 0`")
	require.Contains(t, body, "- Contributor: contrib-z (`contrib-z`)")
	require.False(t, strings.Contains(body, "—"))
}

func TestCollectDecomTargets_Device_DriftKeepsCapturedMetroCode(t *testing.T) {
	baseline := decomFixtureBaseline()
	plan := &Plan{
		Name: "Q3 decom",
		Changes: []PlanChange{
			{
				Seq: 10, OpType: OpRemoveDevice, RefDevicePK: "dev-ghost-pk", State: StatePending,
				RefSnapshot: rawJSON(t, map[string]any{
					"device_code": "dev-ghost", "contributor_code": "contrib-z", "metro_code": "mtr-ghost",
				}),
			},
		},
	}

	devices, _ := collectDecomTargets(plan, baseline, decomFixtureNames())
	require.Len(t, devices, 1)

	d := devices[0]
	require.False(t, d.Resolved)
	require.Equal(t, "mtr-ghost", d.MetroCity)

	title := renderDeviceDecomTitle(d)
	require.Contains(t, title, "mtr-ghost")
	require.NotContains(t, title, "metro unknown")
}

func TestRenderLinkDecomTitle_Drift(t *testing.T) {
	target := LinkDecomTarget{
		Resolved:         false,
		LinkPK:           "link-ghost-pk",
		LinkCode:         "link-ghost",
		OwnerContribCode: "contrib-z",
	}

	title := renderLinkDecomTitle(target)
	require.Contains(t, title, "link-ghost")
	require.NotContains(t, title, " - : - ")
	require.Equal(t, "[Decom-Link] contrib-z — link-ghost — TBD", title)
}

func TestRenderLinkDecomBody_Drift(t *testing.T) {
	target := LinkDecomTarget{
		Resolved:         false,
		LinkPK:           "link-ghost-pk",
		LinkCode:         "link-ghost",
		OwnerContribCode: "contrib-z",
	}

	body := renderLinkDecomBody(target)

	require.Contains(t, body, "- Endpoints: unknown (drift)")
	require.Contains(t, body, "- [ ] Both endpoint devices stay (unknown (drift))")
	require.Contains(t, body, "- [ ] Cross-contributor: unknown (drift)")
	require.Contains(t, body, "- [ ] Maintenance event in OPS portal (if user-impacting)")
	require.Contains(t, body, "- Delete the freed interfaces on both endpoints (unknown (drift))")
	require.Contains(t, body, "- [ ] Link gone from `link list`; freed interfaces removed")
	require.Contains(t, body, "- [ ] No path/latency regressions")
	require.False(t, strings.Contains(body, "—"))
}

func TestRenderDeviceDecomTitle_SanitizesDriftedFields(t *testing.T) {
	target := DeviceDecomTarget{
		ContributorCode: "contrib`x",
		MetroCity:       "mtr-a\nline2",
		DeviceCode:      "dev-a`bad",
	}

	title := renderDeviceDecomTitle(target)
	require.NotContains(t, title, "\n")
	require.NotContains(t, title, "mtr-a\nline2")
	require.Contains(t, title, "\\`")
}

func TestRenderLinkDecomBody_EndpointsAndOrderedChecklist(t *testing.T) {
	baseline := decomFixtureBaseline()
	plan := &Plan{
		Name: "Q3 decom",
		Changes: []PlanChange{
			{Seq: 10, OpType: OpRemoveLink, RefLinkPK: "link-2-pk", State: StatePending},
		},
	}
	_, links := collectDecomTargets(plan, baseline, decomFixtureNames())
	require.Len(t, links, 1)

	body := renderLinkDecomBody(links[0])

	require.Contains(t, body, "- Contributor: Contributor X (`contrib-x`)")
	require.Contains(t, body, "- Link: `link-2` (`link-2-pk`)")
	require.Contains(t, body, "- Endpoints: `dev-a` (Metro Alpha) to `dev-c` (Metro Charlie), 10Gbps DZX")
	require.Contains(t, body, "- Target date: TBD")

	require.Contains(t, body, "- [ ] Both endpoint devices stay (Metro Alpha 12 users, Metro Charlie 7 users)")
	require.Contains(t, body, "- [ ] Impact: does removing this link reroute or degrade any paths? (watch after soft-drain)")
	require.Contains(t, body, "- [ ] Cross-contributor: yes (`contrib-x` and `contrib-y`)")

	softIdx := strings.Index(body, "- Soft-drain the link")
	confirmIdx := strings.Index(body, "- Confirm the network reroutes cleanly")
	hardIdx := strings.Index(body, "- Hard-drain the link")
	deleteIdx := strings.Index(body, "- Delete the link")
	freedIdx := strings.Index(body, "- Delete the freed interfaces on both endpoints")
	require.True(t, softIdx >= 0 && softIdx < confirmIdx && confirmIdx < hardIdx && hardIdx < deleteIdx && deleteIdx < freedIdx,
		"expected soft-drain, confirm, hard-drain, delete, freed-interfaces in order")

	require.Contains(t, body, "- Delete the freed interfaces on both endpoints (Metro Alpha Ethernet2, Metro Charlie Ethernet1)")
	require.False(t, strings.Contains(body, "—"))
}

func TestRenderLinkDecomBody_OmitsEmptySideIface(t *testing.T) {
	target := LinkDecomTarget{
		Resolved:         true,
		LinkCode:         "link-9",
		SideADeviceCode:  "dev-a",
		SideAIface:       "",
		SideZDeviceCode:  "dev-z",
		SideZIface:       "Ethernet3",
		OwnerContribCode: "contrib-x",
	}

	body := renderLinkDecomBody(target)

	require.Contains(t, body, "- Delete the freed interfaces on both endpoints (unknown, unknown Ethernet3)")
	require.Equal(t, 1, strings.Count(body, "Delete the freed interfaces"))
}

func TestBwShort_Decom(t *testing.T) {
	require.Equal(t, "100G", bwShort(100_000_000_000))
	require.Equal(t, "10G", bwShort(10_000_000_000))
	require.Equal(t, "400M", bwShort(400_000_000))
	require.Equal(t, "1.5G", bwShort(1_500_000_000))
}

func TestBwGbps_Decom(t *testing.T) {
	require.Equal(t, "100Gbps", bwGbps(100_000_000_000))
	require.Equal(t, "50Gbps", bwGbps(50_000_000_000))
	require.Equal(t, "", bwGbps(0))
	require.Equal(t, "", bwGbps(-1))
}

func TestBothEndpointsStayNote_Decom(t *testing.T) {
	require.Equal(t, "(only the link is removed)", bothEndpointsStayNote(LinkDecomTarget{}))
	require.Equal(t, "(Oslo 1 user, unknown 0 users)",
		bothEndpointsStayNote(LinkDecomTarget{SideAMetroCity: "Oslo", SideAUsers: 1}))
	require.Equal(t, "(Tokyo 15 users, Seattle 1 user)",
		bothEndpointsStayNote(LinkDecomTarget{SideAMetroCity: "Tokyo", SideAUsers: 15, SideZMetroCity: "Seattle", SideZUsers: 1}))
}

func TestCollectDecomTargets_Device_SelfLoopLinkListedOnce(t *testing.T) {
	baseline := decomFixtureBaseline()
	baseline.Links = append(baseline.Links, Link{
		PK: "link-loop-pk", Code: "link-loop", LinkType: "WAN", BandwidthBps: 1_000_000_000,
		SideAPK: "dev-a-pk", SideACode: "dev-a", SideAIfaceName: "Ethernet3",
		SideZPK: "dev-a-pk", SideZCode: "dev-a", SideZIfaceName: "Ethernet3",
		ContributorPK: "contrib-x-pk", ContributorCode: "contrib-x",
		SideAContributorPK: "contrib-x-pk", SideAContributorCode: "contrib-x",
		SideZContributorPK: "contrib-x-pk", SideZContributorCode: "contrib-x",
	})
	plan := &Plan{
		Name: "Q3 decom",
		Changes: []PlanChange{
			{Seq: 10, OpType: OpRemoveDevice, RefDevicePK: "dev-a-pk", State: StatePending},
		},
	}

	devices, _ := collectDecomTargets(plan, baseline, decomFixtureNames())
	require.Len(t, devices, 1)

	count := 0
	for _, al := range devices[0].AttachedLinks {
		if al.LinkCode == "link-loop" {
			count++
		}
	}
	require.Equal(t, 1, count)
}

// TestLoadContributorNamesNilDBForDecom proves loadContributorNames degrades
// to an empty map (never errors/panics) when ClickHouse is unavailable, so a
// name-lookup failure can never break decom issue rendering.
func TestLoadContributorNamesNilDBForDecom(t *testing.T) {
	api := &API{}
	names := api.loadContributorNames(context.Background())
	require.NotNil(t, names)
	require.Empty(t, names)
}
