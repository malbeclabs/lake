package handlers

import (
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
			{PK: "mtr-b-pk", Code: "mtr-b", Name: "mtr-b"}, // name == code: no parenthetical
			{PK: "mtr-c-pk", Code: "mtr-c", Name: "Metro Charlie"},
		},
		Devices: []Device{
			{
				PK: "dev-a-pk", Code: "dev-a", MetroPK: "mtr-a-pk",
				ContributorPK: "contrib-x-pk", ContributorCode: "contrib-x",
				UserCount: 12, UnicastUsersCount: 10, MulticastSubscribersCount: 1, MulticastPublishersCount: 1,
				StakeSol: 250.5, StakeShare: 3.25,
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

func TestCollectDecomTargets_Device_ResolvesAttachedLinksAndCounts(t *testing.T) {
	baseline := decomFixtureBaseline()
	plan := &Plan{
		Name: "Q3 decom",
		Changes: []PlanChange{
			{Seq: 10, OpType: OpRemoveDevice, RefDevicePK: "dev-a-pk", State: StatePending},
		},
	}

	devices, links := collectDecomTargets(plan, baseline)
	require.Empty(t, links)
	require.Len(t, devices, 1)

	d := devices[0]
	require.True(t, d.Resolved)
	require.Equal(t, "dev-a-pk", d.DevicePK)
	require.Equal(t, "dev-a", d.DeviceCode)
	require.Equal(t, "contrib-x", d.ContributorCode)
	require.Equal(t, "mtr-a (Metro Alpha)", d.MetroLabel)
	require.EqualValues(t, 12, d.Users)
	require.EqualValues(t, 10, d.UnicastUsers)
	require.EqualValues(t, 1, d.MulticastSubs)
	require.EqualValues(t, 1, d.MulticastPubs)
	require.InDelta(t, 250.5, d.StakeSol, 0.001)
	require.InDelta(t, 3.25, d.StakeShare, 0.001)
	require.Equal(t, []string{"Ethernet1", "Ethernet2"}, d.Interfaces)

	require.Len(t, d.AttachedLinks, 2)
	require.Equal(t, "link-1", d.AttachedLinks[0].LinkCode)
	require.Equal(t, "dev-b", d.AttachedLinks[0].OtherDeviceCode)
	require.Equal(t, "mtr-b", d.AttachedLinks[0].OtherMetroLabel)
	require.False(t, d.AttachedLinks[0].CrossContributor)

	require.Equal(t, "link-2", d.AttachedLinks[1].LinkCode)
	require.Equal(t, "dev-c", d.AttachedLinks[1].OtherDeviceCode)
	require.Equal(t, "mtr-c (Metro Charlie)", d.AttachedLinks[1].OtherMetroLabel)
	require.True(t, d.AttachedLinks[1].CrossContributor)
	require.Equal(t, "contrib-y", d.AttachedLinks[1].OtherContribCode)

	require.Equal(t, []string{"contrib-y"}, d.CrossContribCodes)
}

func TestCollectDecomTargets_Link_ResolvesCrossContributorEndpoints(t *testing.T) {
	baseline := decomFixtureBaseline()
	plan := &Plan{
		Name: "Q3 decom",
		Changes: []PlanChange{
			{Seq: 10, OpType: OpRemoveLink, RefLinkPK: "link-2-pk", State: StatePending},
		},
	}

	devices, links := collectDecomTargets(plan, baseline)
	require.Empty(t, devices)
	require.Len(t, links, 1)

	l := links[0]
	require.True(t, l.Resolved)
	require.Equal(t, "link-2-pk", l.LinkPK)
	require.Equal(t, "link-2", l.LinkCode)
	require.Equal(t, "DZX", l.LinkType)
	require.EqualValues(t, 10_000_000_000, l.BandwidthBps)
	require.Equal(t, "contrib-x", l.OwnerContribCode)

	require.Equal(t, "dev-a", l.SideADeviceCode)
	require.Equal(t, "mtr-a (Metro Alpha)", l.SideAMetroLabel)
	require.Equal(t, "contrib-x", l.SideAContribCode)
	require.Equal(t, "Ethernet2", l.SideAIface)
	require.EqualValues(t, 12, l.SideAUsers)

	require.Equal(t, "dev-c", l.SideZDeviceCode)
	require.Equal(t, "mtr-c (Metro Charlie)", l.SideZMetroLabel)
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

	devices, links := collectDecomTargets(plan, baseline)
	require.Empty(t, links)
	require.Len(t, devices, 1)

	d := devices[0]
	require.False(t, d.Resolved)
	require.Equal(t, "dev-ghost-pk", d.DevicePK)
	require.Equal(t, "dev-ghost", d.DeviceCode)
	require.Equal(t, "contrib-z", d.ContributorCode)
	require.Empty(t, d.AttachedLinks)
	require.Empty(t, d.Interfaces)
}

func TestRenderDeviceDecomTitle(t *testing.T) {
	target := DeviceDecomTarget{
		ContributorCode: "contrib-x",
		MetroLabel:      "mtr-a (Metro Alpha)",
		DeviceCode:      "dev-a",
	}

	title := renderDeviceDecomTitle(target)
	require.Equal(t, "[Decom] contrib-x - mtr-a (Metro Alpha) (dev-a) - TBD", title)
	require.False(t, strings.Contains(title, "—"))

	date := "2026-09-01"
	target.Change = PlanChange{TargetDate: &date}
	title = renderDeviceDecomTitle(target)
	require.Equal(t, "[Decom] contrib-x - mtr-a (Metro Alpha) (dev-a) - 2026-09-01", title)
	require.False(t, strings.Contains(title, "—"))
}

func TestRenderLinkDecomTitle(t *testing.T) {
	target := LinkDecomTarget{
		OwnerContribCode: "contrib-x",
		SideADeviceCode:  "dev-a",
		SideZDeviceCode:  "dev-c",
	}

	title := renderLinkDecomTitle(target)
	require.Equal(t, "[Decom-Link] contrib-x - dev-a:dev-c - TBD", title)
	require.False(t, strings.Contains(title, "—"))

	date := "2026-09-01"
	target.Change = PlanChange{TargetDate: &date}
	title = renderLinkDecomTitle(target)
	require.Equal(t, "[Decom-Link] contrib-x - dev-a:dev-c - 2026-09-01", title)
	require.False(t, strings.Contains(title, "—"))
}

func TestRenderDeviceDecomBody_Resolved(t *testing.T) {
	baseline := decomFixtureBaseline()
	date := "2026-09-01"
	plan := &Plan{
		Name: "Q3 decom",
		Changes: []PlanChange{
			{
				Seq: 10, OpType: OpRemoveDevice, RefDevicePK: "dev-a-pk", State: StatePending,
				TargetDate: &date, AssigneeNote: "coordinate with NOC",
			},
		},
	}
	devices, _ := collectDecomTargets(plan, baseline)
	require.Len(t, devices, 1)
	devices[0].Change.TargetDate = &date
	devices[0].Change.AssigneeNote = "coordinate with NOC"

	body := renderDeviceDecomBody(devices[0])

	require.Contains(t, body, "## Summary")
	require.Contains(t, body, "## Timeline")
	require.Contains(t, body, "## Post-decom verification (DZ)")
	require.Contains(t, body, "Current load: 12 users (10 unicast), 1 multicast subscribers, 1 multicast publishers")
	require.Contains(t, body, "- [ ] Soft-drain link `link-1`")
	require.Contains(t, body, "- [ ] Soft-drain link `link-2`")
	require.Contains(t, body, "- [ ] `device update --max-users 0` on `dev-a`")
	require.Contains(t, body, "Cross-contributor / DZX exposure: contributors: `contrib-y`; includes DZX link(s)")
	require.Contains(t, body, "## Notes")
	require.Contains(t, body, "coordinate with NOC")
	require.False(t, strings.Contains(body, "—"))
}

func TestRenderDeviceDecomBody_Drift(t *testing.T) {
	target := DeviceDecomTarget{
		Resolved:        false,
		DevicePK:        "dev-ghost-pk",
		DeviceCode:      "dev-ghost",
		ContributorCode: "contrib-z",
	}

	body := renderDeviceDecomBody(target)

	require.Contains(t, body, deviceDriftNote)
	require.NotContains(t, body, "Current load:")
	require.Contains(t, body, "## Timeline")
	require.Contains(t, body, "## Post-decom verification (DZ)")
	require.Contains(t, body, "- [ ] `device update --max-users 0` on `dev-ghost`")
	require.False(t, strings.Contains(body, "—"))
}

func TestRenderLinkDecomBody_EndpointsAndOrderedChecklist(t *testing.T) {
	baseline := decomFixtureBaseline()
	plan := &Plan{
		Name: "Q3 decom",
		Changes: []PlanChange{
			{Seq: 10, OpType: OpRemoveLink, RefLinkPK: "link-2-pk", State: StatePending},
		},
	}
	_, links := collectDecomTargets(plan, baseline)
	require.Len(t, links, 1)

	body := renderLinkDecomBody(links[0])

	require.Contains(t, body, "Endpoints: `dev-a` (mtr-a (Metro Alpha)) to `dev-c` (mtr-c (Metro Charlie))")

	softIdx := strings.Index(body, "- [ ] Soft-drain link `link-2`")
	confirmIdx := strings.Index(body, "- [ ] Confirm reroute and no traffic on `link-2`")
	hardIdx := strings.Index(body, "- [ ] Hard-drain link `link-2`")
	deleteIdx := strings.Index(body, "- [ ] Delete link `link-2`")
	require.True(t, softIdx >= 0 && softIdx < confirmIdx && confirmIdx < hardIdx && hardIdx < deleteIdx,
		"expected soft-drain, confirm, hard-drain, delete in order")

	require.Contains(t, body, "- [ ] Delete freed interface `Ethernet2` on `dev-a`")
	require.Contains(t, body, "- [ ] Delete freed interface `Ethernet1` on `dev-c`")
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

	require.NotContains(t, body, "Delete freed interface `` on `dev-a`")
	require.NotContains(t, body, "Delete freed interface ` on `dev-a`")
	require.Contains(t, body, "- [ ] Delete freed interface `Ethernet3` on `dev-z`")

	count := strings.Count(body, "Delete freed interface")
	require.Equal(t, 1, count)
}

func TestFormatBandwidthBps_Decom(t *testing.T) {
	require.Equal(t, "100 Gbps", formatBandwidthBps(100_000_000_000))
	require.Equal(t, "10 Gbps", formatBandwidthBps(10_000_000_000))
	require.Equal(t, "400 Mbps", formatBandwidthBps(400_000_000))
	require.Equal(t, "1.5 Gbps", formatBandwidthBps(1_500_000_000))
	require.Equal(t, "0 bps", formatBandwidthBps(0))
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

	devices, _ := collectDecomTargets(plan, baseline)
	require.Len(t, devices, 1)

	d := devices[0]
	require.False(t, d.Resolved)
	require.Equal(t, "mtr-ghost", d.MetroLabel)

	title := renderDeviceDecomTitle(d)
	require.Contains(t, title, "mtr-ghost")
	require.NotContains(t, title, "metro unknown")

	body := renderDeviceDecomBody(d)
	require.Contains(t, body, "- Metro: mtr-ghost\n")
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
	require.Equal(t, "[Decom-Link] contrib-z - link-ghost - TBD", title)
}

func TestRenderLinkDecomBody_Drift(t *testing.T) {
	target := LinkDecomTarget{
		Resolved:         false,
		LinkPK:           "link-ghost-pk",
		LinkCode:         "link-ghost",
		OwnerContribCode: "contrib-z",
	}

	body := renderLinkDecomBody(target)

	require.Contains(t, body, linkDriftNote)
	require.NotContains(t, body, "Cross-contributor: no")
	require.Contains(t, body, "Endpoint details unavailable (drift); confirm endpoints, user counts, and cross-contributor exposure manually.")
	require.Contains(t, body, "- [ ] Maintenance event created in OPS portal")
	require.Contains(t, body, "- [ ] Metro-pair latency still within target")
	require.NotContains(t, body, "- Endpoints:")
	require.False(t, strings.Contains(body, "—"))
}

func TestRenderDeviceDecomTitle_SanitizesDriftedFields(t *testing.T) {
	target := DeviceDecomTarget{
		ContributorCode: "contrib`x",
		MetroLabel:      "mtr-a\nline2",
		DeviceCode:      "dev-a`bad",
	}

	title := renderDeviceDecomTitle(target)
	require.NotContains(t, title, "\n")
	require.NotContains(t, title, "mtr-a\nline2")
	require.Contains(t, title, "\\`")
}

func TestRenderDeviceDecomBody_Drift_OmitsFabricatedConnectedCount(t *testing.T) {
	target := DeviceDecomTarget{
		Resolved:        false,
		DevicePK:        "dev-ghost-pk",
		DeviceCode:      "dev-ghost",
		ContributorCode: "contrib-z",
		Users:           0,
	}

	body := renderDeviceDecomBody(target)

	require.Contains(t, body, "- [ ] Notify affected users to migrate\n")
	require.NotContains(t, body, "connected")
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

	devices, _ := collectDecomTargets(plan, baseline)
	require.Len(t, devices, 1)

	count := 0
	for _, al := range devices[0].AttachedLinks {
		if al.LinkCode == "link-loop" {
			count++
		}
	}
	require.Equal(t, 1, count)
}
