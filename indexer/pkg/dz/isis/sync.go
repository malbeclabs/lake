package isis

import (
	"context"
	"fmt"
	"net"

	dzsvc "github.com/malbeclabs/lake/indexer/pkg/dz/serviceability"
)

// Sync parses LSPs, correlates them with current link state from ClickHouse,
// and writes ISIS adjacency and device data to ClickHouse. This is fully
// independent of Neo4j.
func (s *Store) Sync(ctx context.Context, lsps []LSP) error {
	// Query current links from ClickHouse for tunnel map resolution.
	links, err := dzsvc.QueryCurrentLinks(ctx, s.log, s.cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to query links: %w", err)
	}

	tMaps := buildTunnelMapFromLinks(links)
	s.log.Debug("isis: built tunnel map", "primary_mappings", len(tMaps.primary), "by_link_code_mappings", len(tMaps.byLinkCode))

	// Build device lookup: system_id -> device_pk (from tunnel map matches)
	devicePKBySystemID := make(map[string]string)
	for _, lsp := range lsps {
		for _, neighbor := range lsp.Neighbors {
			if mapping, found, _ := tMaps.resolve(lsp.Hostname, neighbor.NeighborAddr); found {
				devicePKBySystemID[lsp.SystemID] = mapping.localPK
			}
		}
	}

	// Build adjacency slice
	var adjacencies []Adjacency
	for _, lsp := range lsps {
		for _, neighbor := range lsp.Neighbors {
			adj := Adjacency{
				SystemID:         lsp.SystemID,
				NeighborSystemID: neighbor.SystemID,
				NeighborAddr:     neighbor.NeighborAddr,
				Hostname:         lsp.Hostname,
				RouterID:         lsp.RouterID,
				LocalAddr:        neighbor.LocalAddr,
				Metric:           int64(neighbor.Metric),
				AdjSIDs:          AdjSIDsToJSON(neighbor.AdjSIDs),
			}
			if mapping, found, _ := tMaps.resolve(lsp.Hostname, neighbor.NeighborAddr); found {
				adj.DevicePK = mapping.localPK
				adj.LinkPK = mapping.linkPK
			}
			adjacencies = append(adjacencies, adj)
		}
	}

	// Build device slice
	var devices []Device
	for _, lsp := range lsps {
		var overload, nodeUnreachable uint8
		if lsp.Overload {
			overload = 1
		}
		if lsp.NodeUnreachable {
			nodeUnreachable = 1
		}
		devices = append(devices, Device{
			SystemID:        lsp.SystemID,
			DevicePK:        devicePKBySystemID[lsp.SystemID],
			Hostname:        lsp.Hostname,
			RouterID:        lsp.RouterID,
			Overload:        overload,
			NodeUnreachable: nodeUnreachable,
			Sequence:        lsp.Sequence,
		})
	}

	if err := s.ReplaceAdjacencies(ctx, adjacencies); err != nil {
		return fmt.Errorf("failed to replace adjacencies: %w", err)
	}
	if err := s.ReplaceDevices(ctx, devices); err != nil {
		return fmt.Errorf("failed to replace devices: %w", err)
	}

	s.log.Info("isis: synced to ClickHouse", "adjacencies", len(adjacencies), "devices", len(devices))
	return nil
}

// tunnelMapping maps a tunnel IP address to Link and Device information.
type tunnelMapping struct {
	linkPK     string
	linkCode   string
	neighborPK string
	localPK    string
	bandwidth  int64
	isDrained  bool
}

// tunnelMaps holds the primary tunnel map and an override map for duplicate tunnel nets.
type tunnelMaps struct {
	primary      map[string]tunnelMapping
	byLinkCode   map[string]tunnelMapping
	duplicateIPs map[string]bool
}

// resolve looks up the correct tunnelMapping for a given hostname and neighbor IP.
func (t *tunnelMaps) resolve(hostname, neighborAddr string) (tunnelMapping, bool, bool) {
	if linkCodes, ok := duplicateTunnelHostnameToLinkCodes[hostname]; ok {
		for _, code := range linkCodes {
			if m, found := t.byLinkCode[code+":"+neighborAddr]; found {
				return m, true, false
			}
		}
	}
	m, found := t.primary[neighborAddr]
	ambiguous := found && t.duplicateIPs[neighborAddr]
	return m, found, ambiguous
}

// buildTunnelMapFromLinks builds tunnel maps from ClickHouse link data.
func buildTunnelMapFromLinks(links []dzsvc.Link) *tunnelMaps {
	maps := &tunnelMaps{
		primary:      make(map[string]tunnelMapping),
		byLinkCode:   make(map[string]tunnelMapping),
		duplicateIPs: make(map[string]bool),
	}

	for _, link := range links {
		if link.TunnelNet == "" {
			continue
		}

		ip1, ip2, err := parseTunnelNet31(link.TunnelNet)
		if err != nil {
			continue
		}

		isDrained := link.Status == "soft-drained" || link.Status == "hard-drained"

		mappingA := tunnelMapping{
			linkPK:     link.PK,
			linkCode:   link.Code,
			neighborPK: link.SideAPK,
			localPK:    link.SideZPK,
			bandwidth:  int64(link.Bandwidth),
			isDrained:  isDrained,
		}
		mappingZ := tunnelMapping{
			linkPK:     link.PK,
			linkCode:   link.Code,
			neighborPK: link.SideZPK,
			localPK:    link.SideAPK,
			bandwidth:  int64(link.Bandwidth),
			isDrained:  isDrained,
		}

		// Track duplicates
		if _, exists := maps.primary[ip1]; exists {
			maps.duplicateIPs[ip1] = true
		}
		if _, exists := maps.primary[ip2]; exists {
			maps.duplicateIPs[ip2] = true
		}

		maps.primary[ip1] = mappingA
		maps.primary[ip2] = mappingZ
		maps.byLinkCode[link.Code+":"+ip1] = mappingA
		maps.byLinkCode[link.Code+":"+ip2] = mappingZ
	}

	return maps
}

// parseTunnelNet31 parses a /31 CIDR and returns both IP addresses.
func parseTunnelNet31(cidr string) (string, string, error) {
	_, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return "", "", fmt.Errorf("invalid CIDR: %w", err)
	}

	ones, bits := ipnet.Mask.Size()
	if ones != 31 || bits != 32 {
		return "", "", fmt.Errorf("expected /31, got /%d", ones)
	}

	ip := ipnet.IP.To4()
	if ip == nil {
		return "", "", fmt.Errorf("not an IPv4 address")
	}

	ip1 := make(net.IP, 4)
	copy(ip1, ip)

	ip2 := make(net.IP, 4)
	copy(ip2, ip)
	ip2[3]++

	return ip1.String(), ip2.String(), nil
}

// duplicateTunnelHostnameToLinkCodes disambiguates ISIS hostnames when multiple
// links share the same tunnel_net CIDR.
// TODO: Remove this once the duplicate tunnel_net values are fixed in the network.
var duplicateTunnelHostnameToLinkCodes = map[string][]string{
	"frankfurt":  {"dz-fra-01:dzd-fra-01"},
	"dzd-fra-01": {"dz-fra-01:dzd-fra-01", "dzd-fra-01:dzd-tok-01"},
	"la2-dz002":  {"lax001-dz002:tyo002-dz002"},
	"ty2-dz002":  {"lax001-dz002:tyo002-dz002"},

	"dz100a.lax1.teraswitch.com": {"dz100a-lax1-tsw:dz-lax-sw01"},
	"dz-lax-sw01":                {"dz100a-lax1-tsw:dz-lax-sw01", "dz-slc-sw01:dz-lax-sw01"},
	"cc1t-dz002":                 {"tyo001-dz002:sin001-dz002"},
	"sg1t1-dz002":                {"tyo001-dz002:sin001-dz002"},

	"AU1C-NSWP-DZ01":  {"au1c-dz01:la2r-dz01", "au1c-dz01:dz-ch2-sw01"},
	"LA2R-NSWP-DZ01":  {"au1c-dz01:la2r-dz01"},
	"DGT-DZD-DUB-DB2": {"dub001-dz001:dgt-dzd-dub-db2"},
	"dub2t1-dz001":    {"dub001-dz001:dgt-dzd-dub-db2"},

	"dz100a.sea1.teraswitch.com": {"dz100a-sea1-tsw:dz115a-tyo2-tsw"},
	"dz115a.tyo2.teraswitch.com": {"dz100a-sea1-tsw:dz115a-tyo2-tsw"},
	"DZ-FR5-SW01":                {"allnodes-fra1:dz-fr5-sw01"},
	"dz-1":                       {"allnodes-fra1:dz-fr5-sw01"},

	"dzd-tok-01":                 {"dzd-fra-01:dzd-tok-01"},
	"dz100a.fra2.teraswitch.com": {"dz100a-sgp1-tsw:dz100a-fra2-tsw"},
	"dz100a.sgp1.teraswitch.com": {"dz100a-sgp1-tsw:dz100a-fra2-tsw"},

	"dz-slc-sw01": {"dz-slc-sw01:dz-lax-sw01"},
	"DZ-CH2-SW01": {"au1c-dz01:dz-ch2-sw01"},
}
