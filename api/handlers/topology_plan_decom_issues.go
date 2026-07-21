package handlers

import (
	"fmt"
	"sort"
	"strings"
	"time"
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
// House rule: no em dashes anywhere in rendered text, EXCEPT the device/link
// decom issue TITLES, which use an em dash separator to match the real
// malbeclabs/infra contributor-decommission issue convention exactly. This is
// a deliberate, explicit exception scoped to those two title formats only.

// DeviceDecomTarget is one remove_device change resolved against the live
// baseline into everything a decom issue needs. When the device is absent
// from the baseline (drift), the rich fields stay zero/empty and Resolved is
// false, so the renderer emits "unknown (drift)" placeholders instead of
// fabricated data.
type DeviceDecomTarget struct {
	Change            PlanChange
	Resolved          bool
	DevicePK          string
	DeviceCode        string
	ContributorCode   string
	ContributorName   string   // resolved display name; falls back to ContributorCode
	MetroCity         string   // metro city name for the issue title; "" if unknown
	Interfaces        []string // interface names, sorted
	AttachedLinks     []DecomAttachedLink
	CrossContribCodes []string // distinct other-contributor codes with links terminating here, sorted
}

// DecomAttachedLink is one baseline link terminating on a device targeted for
// decommission, from that device's point of view.
type DecomAttachedLink struct {
	LinkCode         string
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
	OwnerContribName string // resolved display name; falls back to OwnerContribCode
	SideADeviceCode  string
	SideAMetroCity   string
	SideAContribCode string
	SideAIface       string
	SideAUsers       uint64
	SideZDeviceCode  string
	SideZMetroCity   string
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

// metroCityFor returns a metro's display city name (its Name, falling back to
// Code) for interpolation into decom titles and endpoint lines, matching the
// infra convention of naming the metro by city rather than code. Returns ""
// for a zero-value Metro (i.e. the metro pk was not found in the baseline).
func metroCityFor(m Metro) string {
	if m.Name != "" {
		return m.Name
	}
	return m.Code
}

// contributorDisplayName resolves a contributor code to its display name via
// the code->name map (see (a *API) loadContributorNames), falling back to the
// code itself when the name is unknown or the map lookup failed. Returns ""
// when code is itself empty, so callers can apply their own "unknown
// contributor" fallback.
func contributorDisplayName(code string, names map[string]string) string {
	if code == "" {
		return ""
	}
	if name, ok := names[code]; ok && name != "" {
		return name
	}
	return code
}

// decomDate renders a change's target date, or "TBD" when unset.
func decomDate(targetDate *string) string {
	if targetDate == nil || *targetDate == "" {
		return "TBD"
	}
	return *targetDate
}

// parseDecomDate parses a change's target date as YYYY-MM-DD, reporting
// ok=false for an unset or unparseable date (e.g. "TBD").
func parseDecomDate(targetDate *string) (time.Time, bool) {
	if targetDate == nil || *targetDate == "" {
		return time.Time{}, false
	}
	d, err := time.Parse("2006-01-02", *targetDate)
	if err != nil {
		return time.Time{}, false
	}
	return d, true
}

// t1DayParenthetical renders " (<date - 1 day>)" for the T-1 day timeline
// header, or "" when the target date is unset/unparseable (the header then
// carries no parenthetical at all, matching the infra convention of only
// dating the header when a real date is known).
func t1DayParenthetical(targetDate *string) string {
	d, ok := parseDecomDate(targetDate)
	if !ok {
		return ""
	}
	return fmt.Sprintf(" (%s)", d.AddDate(0, 0, -1).Format("2006-01-02"))
}

// decomDayParenthetical renders " (<date>)" for the decom-day timeline header,
// or "" when the target date is unset/unparseable.
func decomDayParenthetical(targetDate *string) string {
	d, ok := parseDecomDate(targetDate)
	if !ok {
		return ""
	}
	return fmt.Sprintf(" (%s)", d.Format("2006-01-02"))
}

// bwShort renders a bps value as a compact rate for the device "Links on the
// device" line (e.g. "100G", "10G", "400M"): bps/1e9 + "G" once it reaches
// gigabit scale, else bps/1e6 + "M", with a trailing ".0" trimmed.
func bwShort(bps int64) string {
	if bps >= 1_000_000_000 {
		return trimTrailingZero(float64(bps)/1_000_000_000) + "G"
	}
	return trimTrailingZero(float64(bps)/1_000_000) + "M"
}

// bwGbps renders a bps value as a Gbps rate for the link "Endpoints" line
// (e.g. "50Gbps", "100Gbps"), or "" when bandwidth is 0/unknown so the caller
// can omit the token entirely rather than print a fabricated "0Gbps".
func bwGbps(bps int64) string {
	if bps <= 0 {
		return ""
	}
	return trimTrailingZero(float64(bps)/1_000_000_000) + "Gbps"
}

func trimTrailingZero(v float64) string {
	s := fmt.Sprintf("%.1f", v)
	return strings.TrimSuffix(s, ".0")
}

// resolveDeviceTarget resolves one remove_device change against the baseline
// index. When the device pk is not present in the baseline (drift), only
// identity fields (from ref_snapshot) are filled and Resolved stays false.
func resolveDeviceTarget(idx *decomBaselineIndex, ch PlanChange, names map[string]string) DeviceDecomTarget {
	t := DeviceDecomTarget{Change: ch, DevicePK: ch.RefDevicePK}
	snap := parseSnapshot(ch.RefSnapshot)

	d, ok := idx.deviceByPK[ch.RefDevicePK]
	if !ok {
		t.DeviceCode = orFallback(snap.DeviceCode, ch.RefDevicePK)
		t.ContributorCode = snap.ContributorCode
		t.ContributorName = contributorDisplayName(t.ContributorCode, names)
		t.MetroCity = snap.MetroCode
		return t
	}

	t.Resolved = true
	t.DevicePK = d.PK
	t.DeviceCode = d.Code
	t.ContributorCode = d.ContributorCode
	t.ContributorName = contributorDisplayName(d.ContributorCode, names)
	t.MetroCity = metroCityFor(idx.metroByPK[d.MetroPK])

	for _, iface := range d.Interfaces {
		if iface.Name != "" {
			t.Interfaces = append(t.Interfaces, iface.Name)
		}
	}
	sort.Strings(t.Interfaces)

	crossCodes := map[string]bool{}
	for _, l := range idx.linksByDevicePK[d.PK] {
		var otherContribPK, otherContribCode string
		if l.SideAPK == d.PK {
			otherContribPK, otherContribCode = l.SideZContributorPK, l.SideZContributorCode
		} else {
			otherContribPK, otherContribCode = l.SideAContributorPK, l.SideAContributorCode
		}
		cross := otherContribPK != "" && d.ContributorPK != "" && otherContribPK != d.ContributorPK
		t.AttachedLinks = append(t.AttachedLinks, DecomAttachedLink{
			LinkCode:         l.Code,
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
func resolveLinkTarget(idx *decomBaselineIndex, ch PlanChange, names map[string]string) LinkDecomTarget {
	t := LinkDecomTarget{Change: ch, LinkPK: ch.RefLinkPK}
	snap := parseSnapshot(ch.RefSnapshot)

	l, ok := idx.linkByPK[ch.RefLinkPK]
	if !ok {
		t.LinkCode = orFallback(snap.LinkCode, ch.RefLinkPK)
		t.LinkType = snap.LinkType
		t.BandwidthBps = snap.BandwidthBps
		t.OwnerContribCode = snap.ContributorCode
		t.OwnerContribName = contributorDisplayName(t.OwnerContribCode, names)
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
	t.OwnerContribName = contributorDisplayName(l.ContributorCode, names)
	t.SideADeviceCode = l.SideACode
	t.SideAContribCode = l.SideAContributorCode
	t.SideAIface = l.SideAIfaceName
	t.SideZDeviceCode = l.SideZCode
	t.SideZContribCode = l.SideZContributorCode
	t.SideZIface = l.SideZIfaceName

	if da, ok := idx.deviceByPK[l.SideAPK]; ok {
		t.SideAMetroCity = metroCityFor(idx.metroByPK[da.MetroPK])
		t.SideAUsers = da.UserCount
	}
	if dz, ok := idx.deviceByPK[l.SideZPK]; ok {
		t.SideZMetroCity = metroCityFor(idx.metroByPK[dz.MetroPK])
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
// names is the contributor code->display-name map (see (a *API)
// loadContributorNames); pass nil when the caller has no resolution available
// (targets then fall back to the contributor code everywhere).
func collectDecomTargets(plan *Plan, baseline *TopologyResponse, names map[string]string) ([]DeviceDecomTarget, []LinkDecomTarget) {
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
			devices = append(devices, resolveDeviceTarget(idx, ch, names))
		case OpRemoveLink:
			links = append(links, resolveLinkTarget(idx, ch, names))
		}
	}
	return devices, links
}

// driftPlaceholder is the short, deterministic stand-in used everywhere a fact
// is unknowable because the target has drifted out of the live baseline
// (Resolved == false). Counts and identifiers are never fabricated; this
// placeholder is used in their place instead.
const driftPlaceholder = "unknown (drift)"

// contributorSummaryLine renders the "Contributor: Name (`code`)" fact line
// value, matching the real infra issue format. When no code is known at all,
// it falls back to a plain "unknown contributor" (no empty backtick pair).
func contributorSummaryLine(name, code string) string {
	if code == "" {
		return sanitizeInline(orFallback(name, "unknown contributor"))
	}
	return fmt.Sprintf("%s (`%s`)", sanitizeInline(orFallback(name, code)), sanitizeInline(code))
}

// deviceLinksLine renders the device decom "Links on the device" fact line:
// every attached link as `code` (bwShort type), comma-separated, followed by
// the deterministic "; peer devices stay" suffix (the real issues carry
// hand-written prose here that this app cannot fabricate).
func deviceLinksLine(t DeviceDecomTarget) string {
	if len(t.AttachedLinks) == 0 {
		return "none"
	}
	parts := make([]string, len(t.AttachedLinks))
	for i, al := range t.AttachedLinks {
		parts[i] = fmt.Sprintf("`%s` (%s %s)", sanitizeInline(al.LinkCode), bwShort(al.BandwidthBps), sanitizeInline(al.LinkType))
	}
	return strings.Join(parts, ", ") + "; peer devices stay"
}

// interfacesLine renders the device decom "Interfaces" fact line: a sorted,
// comma-separated list of interface names, or "none recorded" when empty.
func interfacesLine(ifaces []string) string {
	if len(ifaces) == 0 {
		return "none recorded"
	}
	parts := make([]string, len(ifaces))
	for i, iface := range ifaces {
		parts[i] = sanitizeInline(iface)
	}
	return strings.Join(parts, ", ")
}

// crossContribExposureLine renders the "Cross-contributor / DZX exposure" fact
// line for a resolved device target: the distinct other-contributor codes with
// links terminating here, and/or a note that a DZX link is attached, or the
// deterministic "none (only <ContribName> links terminate here)" when neither
// applies.
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
		name := orFallback(t.ContributorName, orFallback(t.ContributorCode, "unknown contributor"))
		return fmt.Sprintf("none (only %s links terminate here)", sanitizeInline(name))
	}
	return strings.Join(parts, "; ")
}

// renderDeviceDecomTitle builds the device decom issue title, matching the
// real malbeclabs/infra contributor-decommission issue format exactly:
// "[Decom] <ContributorName> — <MetroCity> (<DeviceCode>) — <date>". The em
// dash separator and display-name/city fields are a deliberate, explicit
// match to the infra convention (see the house rule at the top of this file).
// Every interpolated field is sanitized: ref_snapshot is unconstrained JSON,
// so a drifted device's code or metro can carry a backtick or newline.
func renderDeviceDecomTitle(t DeviceDecomTarget) string {
	name := orFallback(t.ContributorName, orFallback(t.ContributorCode, "unknown contributor"))
	city := orFallback(t.MetroCity, "metro unknown")
	return fmt.Sprintf("[Decom] %s — %s (%s) — %s",
		sanitizeInline(name),
		sanitizeInline(city),
		sanitizeInline(t.DeviceCode),
		sanitizeInline(decomDate(t.Change.TargetDate)))
}

// renderLinkDecomTitle builds the link decom issue title, matching the real
// infra format: "[Decom-Link] <ContributorName> — <sideA>:<sideZ> — <date>".
// The endpoints segment falls back to the sanitized link code on a drifted
// link (both sides empty), so the title never reads a bare "unknown — : —
// TBD".
func renderLinkDecomTitle(t LinkDecomTarget) string {
	name := orFallback(t.OwnerContribName, orFallback(t.OwnerContribCode, "unknown contributor"))
	endpoints := sanitizeInline(t.LinkCode)
	if t.SideADeviceCode != "" || t.SideZDeviceCode != "" {
		endpoints = sanitizeInline(t.SideADeviceCode) + ":" + sanitizeInline(t.SideZDeviceCode)
	}
	return fmt.Sprintf("[Decom-Link] %s — %s — %s",
		sanitizeInline(name),
		endpoints,
		sanitizeInline(decomDate(t.Change.TargetDate)))
}

// renderDeviceDecomBody renders the full device decom issue body, matching the
// real infra issue's section structure exactly: a summary, a pre-decom
// investigation checklist, a dated teardown timeline, and a post-decom
// verification checklist. When the target has drifted out of the live
// baseline (Resolved == false), the data-derived links/interfaces/exposure
// facts are replaced by the driftPlaceholder; every checklist / plain-bullet
// step stays intact since it is still a valid action regardless of drift.
func renderDeviceDecomBody(t DeviceDecomTarget) string {
	var b strings.Builder
	date := sanitizeInline(decomDate(t.Change.TargetDate))
	devCode := t.DeviceCode

	b.WriteString("## Summary\n")
	fmt.Fprintf(&b, "- Contributor: %s\n", contributorSummaryLine(t.ContributorName, t.ContributorCode))
	fmt.Fprintf(&b, "- Device: `%s` (`%s`)\n", sanitizeInline(devCode), sanitizeInline(t.DevicePK))
	b.WriteString("- Type: Device decommission\n")
	fmt.Fprintf(&b, "- Decommission date: %s\n\n", date)

	b.WriteString("## Pre-decom investigation\n")
	if t.Resolved {
		fmt.Fprintf(&b, "- Links on the device: %s\n", deviceLinksLine(t))
		fmt.Fprintf(&b, "- Interfaces: %s\n", interfacesLine(t.Interfaces))
		fmt.Fprintf(&b, "- Cross-contributor / DZX exposure: %s\n", crossContribExposureLine(t))
	} else {
		fmt.Fprintf(&b, "- Links on the device: %s\n", driftPlaceholder)
		fmt.Fprintf(&b, "- Interfaces: %s\n", driftPlaceholder)
		fmt.Fprintf(&b, "- Cross-contributor / DZX exposure: %s\n", driftPlaceholder)
	}
	b.WriteString("- Physical switch disposition: (confirm)\n")
	b.WriteString("- [ ] Maintenance event created in OPS portal for the decom date\n\n")

	fmt.Fprintf(&b, "## Timeline (decom date: %s)\n", date)
	b.WriteString("### T-31 days: Cap (Contributor)\n")
	b.WriteString("- [ ] `device update --max-users 0`\n")
	b.WriteString("### T-14 days: Notice (User team)\n")
	b.WriteString("- [ ] User team contacts connected users to migrate to another DZD\n")
	fmt.Fprintf(&b, "### T-1 day%s: DZ prep\n", t1DayParenthetical(t.Change.TargetDate))
	b.WriteString("- [ ] Engineering disables the device in the shred program (no active seats, safe)\n")
	b.WriteString("- [ ] Remove any straggler users\n")
	fmt.Fprintf(&b, "### Decom day%s: Teardown (Contributor, in order)\n", decomDayParenthetical(t.Change.TargetDate))
	b.WriteString("- Soft-drain then hard-drain each link\n")
	b.WriteString("- Delete each link\n")
	b.WriteString("- Delete each interface\n")
	b.WriteString("- Drain the device\n")
	b.WriteString("- Delete the device\n\n")

	b.WriteString("## Post-decom verification (DZ)\n")
	b.WriteString("- [ ] Device gone from `device list`; links gone from `link list`; interfaces removed\n")
	b.WriteString("- [ ] Shred oracle healthy and settling (no wedge)\n")
	b.WriteString("- [ ] Physical switch return handled\n")

	return b.String()
}

// linkEndpointsLine renders the link decom "Endpoints" fact line: both side
// device codes and metro cities, plus the bandwidth+type token (bandwidth
// omitted entirely when 0/unknown).
func linkEndpointsLine(t LinkDecomTarget) string {
	sideACity := orFallback(t.SideAMetroCity, "unknown")
	sideZCity := orFallback(t.SideZMetroCity, "unknown")
	tail := sanitizeInline(t.LinkType)
	if bw := bwGbps(t.BandwidthBps); bw != "" {
		tail = bw + " " + tail
	}
	return fmt.Sprintf("`%s` (%s) to `%s` (%s), %s",
		sanitizeInline(t.SideADeviceCode), sanitizeInline(sideACity),
		sanitizeInline(t.SideZDeviceCode), sanitizeInline(sideZCity),
		tail)
}

// userCountPhrase renders a user count as "N users", using the singular
// "1 user" for exactly one.
func userCountPhrase(n uint64) string {
	if n == 1 {
		return "1 user"
	}
	return fmt.Sprintf("%d users", n)
}

// bothEndpointsStayNote renders the parenthetical on the link decom "Both
// endpoint devices stay" line: per-side user counts when either endpoint has
// users, else the deterministic "(only the link is removed)".
func bothEndpointsStayNote(t LinkDecomTarget) string {
	if t.SideAUsers == 0 && t.SideZUsers == 0 {
		return "(only the link is removed)"
	}
	return fmt.Sprintf("(%s %s, %s %s)",
		sanitizeInline(orFallback(t.SideAMetroCity, "unknown")), userCountPhrase(t.SideAUsers),
		sanitizeInline(orFallback(t.SideZMetroCity, "unknown")), userCountPhrase(t.SideZUsers))
}

// linkCrossContributorLine renders the link decom "Cross-contributor" fact
// line: "yes (`a` and `z`)" when the two endpoints belong to different
// contributors, else "none (both endpoints <code>)".
func linkCrossContributorLine(t LinkDecomTarget) string {
	if t.CrossContributor {
		return fmt.Sprintf("yes (`%s` and `%s`)", sanitizeInline(t.SideAContribCode), sanitizeInline(t.SideZContribCode))
	}
	code := orFallback(t.SideAContribCode, orFallback(t.OwnerContribCode, "unknown"))
	return fmt.Sprintf("none (both endpoints %s)", sanitizeInline(code))
}

// freedInterfacesNote renders the parenthetical on the link decom "Delete the
// freed interfaces on both endpoints" line: "<cityA> <ifaceA>, <cityZ>
// <ifaceZ>", dropping an unknown side's interface name (city only), or the
// deterministic "both endpoints" when neither is known.
func freedInterfacesNote(t LinkDecomTarget) string {
	if !t.Resolved {
		return driftPlaceholder
	}
	aKnown := t.SideAIface != ""
	zKnown := t.SideZIface != ""
	if !aKnown && !zKnown {
		return "both endpoints"
	}
	aCity := sanitizeInline(orFallback(t.SideAMetroCity, "unknown"))
	zCity := sanitizeInline(orFallback(t.SideZMetroCity, "unknown"))
	sideA := aCity
	if aKnown {
		sideA = aCity + " " + sanitizeInline(t.SideAIface)
	}
	sideZ := zCity
	if zKnown {
		sideZ = zCity + " " + sanitizeInline(t.SideZIface)
	}
	return sideA + ", " + sideZ
}

// renderLinkDecomBody renders the full link decom issue body, matching the
// real infra issue's section structure exactly: a summary, pre-decom notes on
// both endpoints, an ordered contributor action list, and a post-decom
// verification checklist. When the target has drifted out of the live
// baseline (Resolved == false), the data-derived facts are replaced by the
// driftPlaceholder; every checklist / plain-bullet step stays intact since it
// is still a valid action regardless of drift.
func renderLinkDecomBody(t LinkDecomTarget) string {
	var b strings.Builder
	date := sanitizeInline(decomDate(t.Change.TargetDate))
	hasEndpoints := t.Resolved && (t.SideADeviceCode != "" || t.SideZDeviceCode != "")

	b.WriteString("## Summary\n")
	fmt.Fprintf(&b, "- Contributor: %s\n", contributorSummaryLine(t.OwnerContribName, t.OwnerContribCode))
	fmt.Fprintf(&b, "- Link: `%s` (`%s`)\n", sanitizeInline(t.LinkCode), sanitizeInline(t.LinkPK))
	if hasEndpoints {
		fmt.Fprintf(&b, "- Endpoints: %s\n", linkEndpointsLine(t))
	} else {
		fmt.Fprintf(&b, "- Endpoints: %s\n", driftPlaceholder)
	}
	fmt.Fprintf(&b, "- Target date: %s\n\n", date)

	b.WriteString("## Pre-decom notes\n")
	if t.Resolved {
		fmt.Fprintf(&b, "- [ ] Both endpoint devices stay %s\n", bothEndpointsStayNote(t))
		b.WriteString("- [ ] Impact: does removing this link reroute or degrade any paths? (watch after soft-drain)\n")
		fmt.Fprintf(&b, "- [ ] Cross-contributor: %s\n", linkCrossContributorLine(t))
	} else {
		fmt.Fprintf(&b, "- [ ] Both endpoint devices stay (%s)\n", driftPlaceholder)
		b.WriteString("- [ ] Impact: does removing this link reroute or degrade any paths? (watch after soft-drain)\n")
		fmt.Fprintf(&b, "- [ ] Cross-contributor: %s\n", driftPlaceholder)
	}
	b.WriteString("- [ ] Maintenance event in OPS portal (if user-impacting)\n\n")

	b.WriteString("## Contributor actions (in order)\n")
	b.WriteString("- Soft-drain the link\n")
	b.WriteString("- Confirm the network reroutes cleanly\n")
	b.WriteString("- Hard-drain the link\n")
	b.WriteString("- Delete the link\n")
	fmt.Fprintf(&b, "- Delete the freed interfaces on both endpoints (%s)\n\n", freedInterfacesNote(t))

	b.WriteString("## Post-decom verification (DZ)\n")
	b.WriteString("- [ ] Link gone from `link list`; freed interfaces removed\n")
	b.WriteString("- [ ] No path/latency regressions\n")

	return b.String()
}
