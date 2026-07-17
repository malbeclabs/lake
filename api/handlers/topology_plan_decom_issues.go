package handlers

import (
	"fmt"
	"sort"
	"strings"
)

// This file is the pure rendering layer for decommission ("decom") GitHub
// issues: when a topology plan removes a device or a link, the operator wants
// an issue in the private malbeclabs/infra repo (label contributor-decommission)
// pre-filled with everything this app already knows. collectDecomTargets
// resolves a plan's remove_device / remove_link changes against the live
// baseline topology into render-ready targets; the four render* functions turn
// a target into an issue title/body. Nothing here touches a database or HTTP,
// and nothing here is wired into the existing issue sync (topology_plan_issues.go)
// -- that wiring is a separate follow-up task.
//
// House rule: no em dashes anywhere in rendered text. Use a plain hyphen.

// DeviceDecomTarget is one remove_device change resolved against the live
// baseline into everything a decom issue needs. When the device is absent
// from the baseline (drift), the rich fields stay zero/empty and Resolved is
// false, so the renderer emits a drift note instead of fabricated data.
type DeviceDecomTarget struct {
	Change            PlanChange
	Resolved          bool
	DevicePK          string
	DeviceCode        string
	ContributorCode   string
	MetroLabel        string // "<code>" or "<code> (<name>)"; "" if unknown
	Users             uint64
	UnicastUsers      uint16
	MulticastSubs     uint16
	MulticastPubs     uint16
	StakeSol          float64
	StakeShare        float64
	Interfaces        []string // interface names, sorted
	AttachedLinks     []DecomAttachedLink
	CrossContribCodes []string // distinct other-contributor codes with links terminating here, sorted
}

// DecomAttachedLink is one baseline link terminating on a device targeted for
// decommission, from that device's point of view (OtherDeviceCode/OtherMetroLabel
// describe the far end).
type DecomAttachedLink struct {
	LinkCode         string
	OtherDeviceCode  string
	OtherMetroLabel  string
	BandwidthBps     int64
	LinkType         string
	CrossContributor bool
	OtherContribCode string
}

// LinkDecomTarget is one remove_link change resolved against the live baseline.
type LinkDecomTarget struct {
	Change           PlanChange
	Resolved         bool
	LinkPK           string
	LinkCode         string
	LinkType         string
	BandwidthBps     int64
	OwnerContribCode string
	SideADeviceCode  string
	SideAMetroLabel  string
	SideAContribCode string
	SideAIface       string
	SideAUsers       uint64
	SideZDeviceCode  string
	SideZMetroLabel  string
	SideZContribCode string
	SideZIface       string
	SideZUsers       uint64
	CrossContributor bool
}

// decomBaselineIndex holds fast lookups over the live topology for resolving
// decom targets: devices and links by pk, metros by pk, and every link
// attached to a device pk (grouped once so collectDecomTargets doesn't rescan
// the whole link list per device).
type decomBaselineIndex struct {
	deviceByPK      map[string]Device
	linkByPK        map[string]Link
	metroByPK       map[string]Metro
	linksByDevicePK map[string][]Link
}

func newDecomBaselineIndex(b *TopologyResponse) *decomBaselineIndex {
	idx := &decomBaselineIndex{
		deviceByPK:      map[string]Device{},
		linkByPK:        map[string]Link{},
		metroByPK:       map[string]Metro{},
		linksByDevicePK: map[string][]Link{},
	}
	if b == nil {
		return idx
	}
	for _, d := range b.Devices {
		idx.deviceByPK[d.PK] = d
	}
	for _, l := range b.Links {
		idx.linkByPK[l.PK] = l
		if l.SideAPK != "" {
			idx.linksByDevicePK[l.SideAPK] = append(idx.linksByDevicePK[l.SideAPK], l)
		}
		if l.SideZPK != "" && l.SideZPK != l.SideAPK {
			idx.linksByDevicePK[l.SideZPK] = append(idx.linksByDevicePK[l.SideZPK], l)
		}
	}
	for _, m := range b.Metros {
		idx.metroByPK[m.PK] = m
	}
	return idx
}

// metroLabelFor renders a metro as "<code>" or "<code> (<name>)" when the name
// differs from the code and is non-empty. Returns "" for a zero-value Metro
// (i.e. the metro pk was not found in the baseline).
func metroLabelFor(m Metro) string {
	if m.Code == "" {
		return ""
	}
	if m.Name != "" && m.Name != m.Code {
		return fmt.Sprintf("%s (%s)", m.Code, m.Name)
	}
	return m.Code
}

// decomDate renders a change's target date, or "TBD" when unset.
func decomDate(targetDate *string) string {
	if targetDate == nil || *targetDate == "" {
		return "TBD"
	}
	return *targetDate
}

// formatBandwidthBps renders a bps value as a human-readable rate (e.g.
// "100 Gbps", "400 Mbps"), 1 decimal place with a trailing ".0" trimmed.
func formatBandwidthBps(bps int64) string {
	switch {
	case bps >= 1_000_000_000_000:
		return trimBandwidthUnit(float64(bps)/1_000_000_000_000, "Tbps")
	case bps >= 1_000_000_000:
		return trimBandwidthUnit(float64(bps)/1_000_000_000, "Gbps")
	case bps >= 1_000_000:
		return trimBandwidthUnit(float64(bps)/1_000_000, "Mbps")
	case bps >= 1_000:
		return trimBandwidthUnit(float64(bps)/1_000, "Kbps")
	default:
		return fmt.Sprintf("%d bps", bps)
	}
}

func trimBandwidthUnit(v float64, unit string) string {
	s := fmt.Sprintf("%.1f", v)
	s = strings.TrimSuffix(s, ".0")
	return s + " " + unit
}

// resolveDeviceTarget resolves one remove_device change against the baseline
// index. When the device pk is not present in the baseline (drift), only
// identity fields (from ref_snapshot) are filled and Resolved stays false.
func resolveDeviceTarget(idx *decomBaselineIndex, ch PlanChange) DeviceDecomTarget {
	t := DeviceDecomTarget{Change: ch, DevicePK: ch.RefDevicePK}
	snap := parseSnapshot(ch.RefSnapshot)

	d, ok := idx.deviceByPK[ch.RefDevicePK]
	if !ok {
		t.DeviceCode = orFallback(snap.DeviceCode, ch.RefDevicePK)
		t.ContributorCode = snap.ContributorCode
		t.MetroLabel = snap.MetroCode
		return t
	}

	t.Resolved = true
	t.DevicePK = d.PK
	t.DeviceCode = d.Code
	t.ContributorCode = d.ContributorCode
	t.MetroLabel = metroLabelFor(idx.metroByPK[d.MetroPK])
	t.Users = d.UserCount
	t.UnicastUsers = d.UnicastUsersCount
	t.MulticastSubs = d.MulticastSubscribersCount
	t.MulticastPubs = d.MulticastPublishersCount
	t.StakeSol = d.StakeSol
	t.StakeShare = d.StakeShare

	for _, iface := range d.Interfaces {
		if iface.Name != "" {
			t.Interfaces = append(t.Interfaces, iface.Name)
		}
	}
	sort.Strings(t.Interfaces)

	crossCodes := map[string]bool{}
	for _, l := range idx.linksByDevicePK[d.PK] {
		var otherPK, otherCode, otherContribPK, otherContribCode string
		if l.SideAPK == d.PK {
			otherPK, otherCode = l.SideZPK, l.SideZCode
			otherContribPK, otherContribCode = l.SideZContributorPK, l.SideZContributorCode
		} else {
			otherPK, otherCode = l.SideAPK, l.SideACode
			otherContribPK, otherContribCode = l.SideAContributorPK, l.SideAContributorCode
		}
		otherMetroLabel := ""
		if otherDev, ok := idx.deviceByPK[otherPK]; ok {
			otherMetroLabel = metroLabelFor(idx.metroByPK[otherDev.MetroPK])
		}
		cross := otherContribPK != "" && d.ContributorPK != "" && otherContribPK != d.ContributorPK
		t.AttachedLinks = append(t.AttachedLinks, DecomAttachedLink{
			LinkCode:         l.Code,
			OtherDeviceCode:  otherCode,
			OtherMetroLabel:  otherMetroLabel,
			BandwidthBps:     l.BandwidthBps,
			LinkType:         l.LinkType,
			CrossContributor: cross,
			OtherContribCode: otherContribCode,
		})
		if cross && otherContribCode != "" {
			crossCodes[otherContribCode] = true
		}
	}
	sort.Slice(t.AttachedLinks, func(i, j int) bool { return t.AttachedLinks[i].LinkCode < t.AttachedLinks[j].LinkCode })

	for code := range crossCodes {
		t.CrossContribCodes = append(t.CrossContribCodes, code)
	}
	sort.Strings(t.CrossContribCodes)

	return t
}

// resolveLinkTarget resolves one remove_link change against the baseline
// index. When the link pk is not present in the baseline (drift), only
// identity fields (from ref_snapshot) are filled and Resolved stays false.
func resolveLinkTarget(idx *decomBaselineIndex, ch PlanChange) LinkDecomTarget {
	t := LinkDecomTarget{Change: ch, LinkPK: ch.RefLinkPK}
	snap := parseSnapshot(ch.RefSnapshot)

	l, ok := idx.linkByPK[ch.RefLinkPK]
	if !ok {
		t.LinkCode = orFallback(snap.LinkCode, ch.RefLinkPK)
		t.LinkType = snap.LinkType
		t.BandwidthBps = snap.BandwidthBps
		t.OwnerContribCode = snap.ContributorCode
		t.SideAContribCode = snap.SideAContributorCode
		t.SideZContribCode = snap.SideZContributorCode
		t.CrossContributor = snap.SideAContributorPK != "" && snap.SideZContributorPK != "" &&
			snap.SideAContributorPK != snap.SideZContributorPK
		return t
	}

	t.Resolved = true
	t.LinkPK = l.PK
	t.LinkCode = l.Code
	t.LinkType = l.LinkType
	t.BandwidthBps = l.BandwidthBps
	t.OwnerContribCode = l.ContributorCode
	t.SideADeviceCode = l.SideACode
	t.SideAContribCode = l.SideAContributorCode
	t.SideAIface = l.SideAIfaceName
	t.SideZDeviceCode = l.SideZCode
	t.SideZContribCode = l.SideZContributorCode
	t.SideZIface = l.SideZIfaceName

	if da, ok := idx.deviceByPK[l.SideAPK]; ok {
		t.SideAMetroLabel = metroLabelFor(idx.metroByPK[da.MetroPK])
		t.SideAUsers = da.UserCount
	}
	if dz, ok := idx.deviceByPK[l.SideZPK]; ok {
		t.SideZMetroLabel = metroLabelFor(idx.metroByPK[dz.MetroPK])
		t.SideZUsers = dz.UserCount
	}

	t.CrossContributor = l.SideAContributorPK != "" && l.SideZContributorPK != "" &&
		l.SideAContributorPK != l.SideZContributorPK

	return t
}

// collectDecomTargets walks a plan's pending changes and resolves each
// remove_device / remove_link into a render-ready target using the live
// baseline. skipped/superseded changes are ignored (mirrors
// deriveActionListFromBaseline's filter). Devices/links missing from the
// baseline fall back to ref_snapshot for identity and are marked Resolved=false.
func collectDecomTargets(plan *Plan, baseline *TopologyResponse) ([]DeviceDecomTarget, []LinkDecomTarget) {
	idx := newDecomBaselineIndex(baseline)

	var changes []PlanChange
	if plan != nil {
		changes = append(changes, plan.Changes...)
	}
	sort.SliceStable(changes, func(i, j int) bool { return changes[i].Seq < changes[j].Seq })

	var devices []DeviceDecomTarget
	var links []LinkDecomTarget
	for _, ch := range changes {
		if ch.State == StateSkipped || ch.State == StateSuperseded {
			continue
		}
		switch ch.OpType {
		case OpRemoveDevice:
			devices = append(devices, resolveDeviceTarget(idx, ch))
		case OpRemoveLink:
			links = append(links, resolveLinkTarget(idx, ch))
		}
	}
	return devices, links
}

// deviceDriftNote and linkDriftNote are the exact strings shown in place of
// baseline-derived data when a decom target has drifted out of the live
// topology (the referenced device/link no longer exists there).
const (
	deviceDriftNote = "> Device not found in the live baseline (drift); fill in load, links, and interfaces manually."
	linkDriftNote   = "> Link not found in the live baseline (drift); fill in endpoint load and interface details manually."
)

// crossContribExposureLine renders the "Cross-contributor / DZX exposure" fact
// line for a resolved device target: the distinct other-contributor codes with
// links terminating here, and/or a note that a DZX link is attached.
func crossContribExposureLine(t DeviceDecomTarget) string {
	var parts []string
	if len(t.CrossContribCodes) > 0 {
		codes := make([]string, len(t.CrossContribCodes))
		for i, c := range t.CrossContribCodes {
			codes[i] = "`" + sanitizeInline(c) + "`"
		}
		parts = append(parts, "contributors: "+strings.Join(codes, ", "))
	}
	hasDZX := false
	for _, al := range t.AttachedLinks {
		if strings.EqualFold(al.LinkType, "DZX") {
			hasDZX = true
			break
		}
	}
	if hasDZX {
		parts = append(parts, "includes DZX link(s)")
	}
	if len(parts) == 0 {
		return "None detected"
	}
	return strings.Join(parts, "; ")
}

// renderDeviceDecomTitle builds the device decom issue title, matching the
// infra contributor-decommission format but with a plain hyphen separator (no
// em dash). Every interpolated field is sanitized: ref_snapshot is unconstrained
// JSON, so a drifted device's code or metro can carry a backtick or newline.
func renderDeviceDecomTitle(t DeviceDecomTarget) string {
	metro := orFallback(t.MetroLabel, "metro unknown")
	return fmt.Sprintf("[Decom] %s - %s (%s) - %s",
		sanitizeInline(orFallback(t.ContributorCode, "unknown contributor")),
		sanitizeInline(metro),
		sanitizeInline(t.DeviceCode),
		sanitizeInline(decomDate(t.Change.TargetDate)))
}

// renderLinkDecomTitle builds the link decom issue title. The endpoints
// segment is only built from the side device codes when at least one is
// non-empty; on a drifted link (both sides empty) it falls back to the
// sanitized link code so the title never reads a bare "unknown - : - TBD".
func renderLinkDecomTitle(t LinkDecomTarget) string {
	endpoints := sanitizeInline(t.LinkCode)
	if t.SideADeviceCode != "" || t.SideZDeviceCode != "" {
		endpoints = sanitizeInline(t.SideADeviceCode) + ":" + sanitizeInline(t.SideZDeviceCode)
	}
	return fmt.Sprintf("[Decom-Link] %s - %s - %s",
		sanitizeInline(orFallback(t.OwnerContribCode, "unknown contributor")),
		endpoints,
		sanitizeInline(decomDate(t.Change.TargetDate)))
}

// renderDeviceDecomBody renders the full device decom issue body: a summary,
// a pre-decom investigation checklist, a dated teardown timeline, and a
// post-decom verification checklist. When the target has drifted out of the
// live baseline (Resolved == false), the data-derived load/links/interfaces
// facts are replaced by a single drift note; the checklists stay intact since
// they are still valid actions regardless of drift.
func renderDeviceDecomBody(t DeviceDecomTarget) string {
	var b strings.Builder
	contribCode := orFallback(t.ContributorCode, "unknown contributor")
	metro := orFallback(t.MetroLabel, "unknown")
	date := sanitizeInline(decomDate(t.Change.TargetDate))
	devCode := t.DeviceCode

	b.WriteString("## Summary\n")
	fmt.Fprintf(&b, "- Contributor: `%s`\n", sanitizeInline(contribCode))
	fmt.Fprintf(&b, "- Device: `%s` (`%s`)\n", sanitizeInline(devCode), sanitizeInline(t.DevicePK))
	b.WriteString("- Type: Device decommission\n")
	fmt.Fprintf(&b, "- Metro: %s\n", sanitizeInline(metro))
	fmt.Fprintf(&b, "- Decommission date: %s\n\n", date)

	if t.Resolved {
		fmt.Fprintf(&b, "Current load: %d users (%d unicast), %d multicast subscribers, %d multicast publishers; stake %.1f SOL (%.2f%%).\n\n",
			t.Users, t.UnicastUsers, t.MulticastSubs, t.MulticastPubs, t.StakeSol, t.StakeShare)
	} else {
		b.WriteString(deviceDriftNote + "\n\n")
	}

	b.WriteString("## Pre-decom investigation\n")
	if t.Resolved {
		b.WriteString("- Links on the device:\n")
		if len(t.AttachedLinks) == 0 {
			b.WriteString("  - None\n")
		} else {
			for _, al := range t.AttachedLinks {
				otherMetro := orFallback(al.OtherMetroLabel, "unknown")
				fmt.Fprintf(&b, "  - `%s` -> `%s` (%s), %s", sanitizeInline(al.LinkCode), sanitizeInline(al.OtherDeviceCode), sanitizeInline(otherMetro), formatBandwidthBps(al.BandwidthBps))
				if al.CrossContributor {
					fmt.Fprintf(&b, " [cross-contributor: `%s`]", sanitizeInline(al.OtherContribCode))
				}
				b.WriteString("\n")
			}
		}
		b.WriteString("- Interfaces:\n")
		if len(t.Interfaces) == 0 {
			b.WriteString("  - None recorded\n")
		} else {
			for _, iface := range t.Interfaces {
				fmt.Fprintf(&b, "  - `%s`\n", sanitizeInline(iface))
			}
		}
		fmt.Fprintf(&b, "- Cross-contributor / DZX exposure: %s\n", crossContribExposureLine(t))
	}
	b.WriteString("- [ ] Confirm shred-seat subscribers and multicast subscribers to notify\n")
	b.WriteString("- [ ] Physical switch disposition (return / repurpose / dispose)\n")
	b.WriteString("- [ ] Maintenance event created in OPS portal\n\n")

	fmt.Fprintf(&b, "## Timeline (decom date: %s)\n", date)
	fmt.Fprintf(&b, "### T-31 days: Cap (%s)\n", sanitizeInline(contribCode))
	fmt.Fprintf(&b, "- [ ] `device update --max-users 0` on `%s` (stops new users; existing users keep working)\n", sanitizeInline(devCode))
	b.WriteString("### T-14 days: Notice (User team)\n")
	if t.Resolved {
		fmt.Fprintf(&b, "- [ ] Notify affected users (%d connected) to migrate\n", t.Users)
	} else {
		b.WriteString("- [ ] Notify affected users to migrate\n")
	}
	b.WriteString("### T-1 day: DZ prep\n")
	b.WriteString("- [ ] Confirm user count has drained\n")
	b.WriteString("- [ ] Final go / no-go\n")
	fmt.Fprintf(&b, "### Decom day: Teardown (%s, in order)\n", sanitizeInline(contribCode))
	for _, al := range t.AttachedLinks {
		fmt.Fprintf(&b, "- [ ] Soft-drain link `%s`\n", sanitizeInline(al.LinkCode))
		fmt.Fprintf(&b, "- [ ] Confirm reroute and no traffic on `%s`\n", sanitizeInline(al.LinkCode))
		fmt.Fprintf(&b, "- [ ] Hard-drain link `%s`\n", sanitizeInline(al.LinkCode))
		fmt.Fprintf(&b, "- [ ] Delete link `%s`\n", sanitizeInline(al.LinkCode))
	}
	fmt.Fprintf(&b, "- [ ] Delete interfaces on `%s`\n", sanitizeInline(devCode))
	fmt.Fprintf(&b, "- [ ] Drain device `%s`\n", sanitizeInline(devCode))
	fmt.Fprintf(&b, "- [ ] Delete device `%s`\n\n", sanitizeInline(devCode))

	b.WriteString("## Post-decom verification (DZ)\n")
	fmt.Fprintf(&b, "- [ ] Device `%s` no longer onchain\n", sanitizeInline(devCode))
	fmt.Fprintf(&b, "- [ ] No orphaned links reference `%s`\n", sanitizeInline(devCode))
	b.WriteString("- [ ] Monitoring and alerts updated\n")

	if t.Change.AssigneeNote != "" {
		b.WriteString("\n## Notes\n")
		fmt.Fprintf(&b, "%s\n", sanitizeInline(t.Change.AssigneeNote))
	}

	return b.String()
}

// renderLinkDecomBody renders the full link decom issue body: a summary,
// pre-decom notes on both endpoints, an ordered contributor action checklist,
// and a post-decom verification checklist. When the target has drifted out of
// the live baseline (Resolved == false), a drift note is added after the
// summary; the checklists stay intact since they are still valid actions.
func renderLinkDecomBody(t LinkDecomTarget) string {
	var b strings.Builder
	owner := orFallback(t.OwnerContribCode, "unknown contributor")
	sideAMetro := orFallback(t.SideAMetroLabel, "unknown")
	sideZMetro := orFallback(t.SideZMetroLabel, "unknown")
	date := sanitizeInline(decomDate(t.Change.TargetDate))
	hasEndpoints := t.SideADeviceCode != "" || t.SideZDeviceCode != ""

	b.WriteString("## Summary\n")
	fmt.Fprintf(&b, "- Contributor: `%s`\n", sanitizeInline(owner))
	fmt.Fprintf(&b, "- Link: `%s` (`%s`)\n", sanitizeInline(t.LinkCode), sanitizeInline(t.LinkPK))
	if hasEndpoints {
		fmt.Fprintf(&b, "- Endpoints: `%s` (%s) to `%s` (%s)\n",
			sanitizeInline(t.SideADeviceCode), sanitizeInline(sideAMetro), sanitizeInline(t.SideZDeviceCode), sanitizeInline(sideZMetro))
	}
	fmt.Fprintf(&b, "- Bandwidth: %s %s\n", formatBandwidthBps(t.BandwidthBps), sanitizeInline(t.LinkType))
	fmt.Fprintf(&b, "- Target date: %s\n\n", date)

	if !t.Resolved {
		b.WriteString(linkDriftNote + "\n\n")
	}

	b.WriteString("## Pre-decom notes\n")
	if t.Resolved {
		fmt.Fprintf(&b, "- Side A `%s` (%s, `%s`): %d users\n",
			sanitizeInline(t.SideADeviceCode), sanitizeInline(sideAMetro), sanitizeInline(t.SideAContribCode), t.SideAUsers)
		fmt.Fprintf(&b, "- Side Z `%s` (%s, `%s`): %d users\n",
			sanitizeInline(t.SideZDeviceCode), sanitizeInline(sideZMetro), sanitizeInline(t.SideZContribCode), t.SideZUsers)
		if t.CrossContributor {
			fmt.Fprintf(&b, "- Cross-contributor: yes (`%s` and `%s`)\n", sanitizeInline(t.SideAContribCode), sanitizeInline(t.SideZContribCode))
		} else {
			b.WriteString("- Cross-contributor: no\n")
		}
		fmt.Fprintf(&b, "- Impact: removing this link reroutes traffic between %s and %s; confirm alternate-path capacity.\n",
			sanitizeInline(sideAMetro), sanitizeInline(sideZMetro))
	} else {
		b.WriteString("- Endpoint details unavailable (drift); confirm endpoints, user counts, and cross-contributor exposure manually.\n")
	}
	b.WriteString("- [ ] Maintenance event created in OPS portal\n\n")

	b.WriteString("## Contributor actions (in order)\n")
	fmt.Fprintf(&b, "- [ ] Soft-drain link `%s`\n", sanitizeInline(t.LinkCode))
	fmt.Fprintf(&b, "- [ ] Confirm reroute and no traffic on `%s`\n", sanitizeInline(t.LinkCode))
	fmt.Fprintf(&b, "- [ ] Hard-drain link `%s`\n", sanitizeInline(t.LinkCode))
	fmt.Fprintf(&b, "- [ ] Delete link `%s`\n", sanitizeInline(t.LinkCode))
	if t.SideAIface != "" {
		fmt.Fprintf(&b, "- [ ] Delete freed interface `%s` on `%s`\n", sanitizeInline(t.SideAIface), sanitizeInline(t.SideADeviceCode))
	}
	if t.SideZIface != "" {
		fmt.Fprintf(&b, "- [ ] Delete freed interface `%s` on `%s`\n", sanitizeInline(t.SideZIface), sanitizeInline(t.SideZDeviceCode))
	}
	b.WriteString("\n")

	b.WriteString("## Post-decom verification (DZ)\n")
	fmt.Fprintf(&b, "- [ ] Link `%s` no longer onchain\n", sanitizeInline(t.LinkCode))
	b.WriteString("- [ ] Freed interfaces confirmed removed\n")
	if t.Resolved {
		fmt.Fprintf(&b, "- [ ] Metro-pair latency %s to %s still within target\n", sanitizeInline(sideAMetro), sanitizeInline(sideZMetro))
	} else {
		b.WriteString("- [ ] Metro-pair latency still within target\n")
	}

	if t.Change.AssigneeNote != "" {
		b.WriteString("\n## Notes\n")
		fmt.Fprintf(&b, "%s\n", sanitizeInline(t.Change.AssigneeNote))
	}

	return b.String()
}
