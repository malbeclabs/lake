package graph

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/dz/isis"
	dzsvc "github.com/malbeclabs/lake/indexer/pkg/dz/serviceability"
	"github.com/malbeclabs/lake/indexer/pkg/neo4j"
)

// StoreConfig holds configuration for the Store.
type StoreConfig struct {
	Logger     *slog.Logger
	Neo4j      neo4j.Client
	ClickHouse clickhouse.Client
}

func (cfg *StoreConfig) Validate() error {
	if cfg.Logger == nil {
		return errors.New("logger is required")
	}
	if cfg.Neo4j == nil {
		return errors.New("neo4j client is required")
	}
	if cfg.ClickHouse == nil {
		return errors.New("clickhouse client is required")
	}
	return nil
}

// Store manages the Neo4j graph representation of the network topology.
// It syncs data from ClickHouse (source of truth) to Neo4j for graph algorithms.
type Store struct {
	log *slog.Logger
	cfg StoreConfig
}

// NewStore creates a new Store.
func NewStore(cfg StoreConfig) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Store{
		log: cfg.Logger,
		cfg: cfg,
	}, nil
}

// Sync reads current state from ClickHouse and replaces the Neo4j graph.
// This performs a full sync atomically within a single transaction.
// Readers see either the old state or the new state, never an empty/partial state.
func (s *Store) Sync(ctx context.Context) error {
	s.log.Debug("graph: starting sync")

	// Read current data from ClickHouse
	devices, err := dzsvc.QueryCurrentDevices(ctx, s.log, s.cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to query devices: %w", err)
	}

	links, err := dzsvc.QueryCurrentLinks(ctx, s.log, s.cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to query links: %w", err)
	}

	metros, err := dzsvc.QueryCurrentMetros(ctx, s.log, s.cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to query metros: %w", err)
	}

	users, err := dzsvc.QueryCurrentUsers(ctx, s.log, s.cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to query users: %w", err)
	}

	contributors, err := dzsvc.QueryCurrentContributors(ctx, s.log, s.cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to query contributors: %w", err)
	}

	s.log.Debug("graph: fetched data from ClickHouse",
		"devices", len(devices),
		"links", len(links),
		"metros", len(metros),
		"users", len(users),
		"contributors", len(contributors))

	session, err := s.cfg.Neo4j.Session(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Neo4j session: %w", err)
	}
	defer session.Close(ctx)

	// Perform atomic sync within a single write transaction
	_, err = session.ExecuteWrite(ctx, func(tx neo4j.Transaction) (any, error) {
		// Delete all existing nodes and relationships
		res, err := tx.Run(ctx, "MATCH (n) DETACH DELETE n", nil)
		if err != nil {
			return nil, fmt.Errorf("failed to clear graph: %w", err)
		}
		if _, err := res.Consume(ctx); err != nil {
			return nil, fmt.Errorf("failed to consume clear result: %w", err)
		}

		// Create all nodes and relationships using batched UNWIND queries
		if err := batchCreateContributors(ctx, tx, contributors); err != nil {
			return nil, fmt.Errorf("failed to create contributors: %w", err)
		}

		if err := batchCreateMetros(ctx, tx, metros); err != nil {
			return nil, fmt.Errorf("failed to create metros: %w", err)
		}

		if err := batchCreateDevices(ctx, tx, devices); err != nil {
			return nil, fmt.Errorf("failed to create devices: %w", err)
		}

		if err := batchCreateLinks(ctx, tx, links); err != nil {
			return nil, fmt.Errorf("failed to create links: %w", err)
		}

		if err := batchCreateUsers(ctx, tx, users); err != nil {
			return nil, fmt.Errorf("failed to create users: %w", err)
		}

		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("failed to sync graph: %w", err)
	}

	s.log.Info("graph: sync completed",
		"devices", len(devices),
		"links", len(links),
		"metros", len(metros),
		"users", len(users))

	return nil
}

// SyncWithISIS reads current state from ClickHouse and IS-IS data, then replaces the Neo4j graph
// atomically within a single transaction. This ensures there is never a moment where the graph
// has base nodes but no ISIS relationships.
func (s *Store) SyncWithISIS(ctx context.Context, lsps []isis.LSP) error {
	s.log.Debug("graph: starting sync with ISIS", "lsps", len(lsps))

	// Read current data from ClickHouse
	devices, err := dzsvc.QueryCurrentDevices(ctx, s.log, s.cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to query devices: %w", err)
	}

	links, err := dzsvc.QueryCurrentLinks(ctx, s.log, s.cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to query links: %w", err)
	}

	metros, err := dzsvc.QueryCurrentMetros(ctx, s.log, s.cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to query metros: %w", err)
	}

	users, err := dzsvc.QueryCurrentUsers(ctx, s.log, s.cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to query users: %w", err)
	}

	contributors, err := dzsvc.QueryCurrentContributors(ctx, s.log, s.cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to query contributors: %w", err)
	}

	s.log.Debug("graph: fetched data from ClickHouse",
		"devices", len(devices),
		"links", len(links),
		"metros", len(metros),
		"users", len(users),
		"contributors", len(contributors))

	session, err := s.cfg.Neo4j.Session(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Neo4j session: %w", err)
	}
	defer session.Close(ctx)

	// Perform atomic sync within a single write transaction
	_, err = session.ExecuteWrite(ctx, func(tx neo4j.Transaction) (any, error) {
		// Delete all existing nodes and relationships
		res, err := tx.Run(ctx, "MATCH (n) DETACH DELETE n", nil)
		if err != nil {
			return nil, fmt.Errorf("failed to clear graph: %w", err)
		}
		if _, err := res.Consume(ctx); err != nil {
			return nil, fmt.Errorf("failed to consume clear result: %w", err)
		}

		// Create all nodes and relationships using batched UNWIND queries
		if err := batchCreateContributors(ctx, tx, contributors); err != nil {
			return nil, fmt.Errorf("failed to create contributors: %w", err)
		}

		if err := batchCreateMetros(ctx, tx, metros); err != nil {
			return nil, fmt.Errorf("failed to create metros: %w", err)
		}

		if err := batchCreateDevices(ctx, tx, devices); err != nil {
			return nil, fmt.Errorf("failed to create devices: %w", err)
		}

		if err := batchCreateLinks(ctx, tx, links); err != nil {
			return nil, fmt.Errorf("failed to create links: %w", err)
		}

		if err := batchCreateUsers(ctx, tx, users); err != nil {
			return nil, fmt.Errorf("failed to create users: %w", err)
		}

		// Now create ISIS relationships within the same transaction
		if len(lsps) > 0 {
			if err := s.syncISISInTx(ctx, tx, lsps); err != nil {
				return nil, fmt.Errorf("failed to sync ISIS data: %w", err)
			}
		}

		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("failed to sync graph with ISIS: %w", err)
	}

	s.log.Info("graph: sync with ISIS completed",
		"devices", len(devices),
		"links", len(links),
		"metros", len(metros),
		"users", len(users),
		"lsps", len(lsps))

	return nil
}

// syncISISInTx creates ISIS relationships within an existing transaction.
func (s *Store) syncISISInTx(ctx context.Context, tx neo4j.Transaction, lsps []isis.LSP) error {
	// Build tunnel map from the newly created links
	tMaps, err := s.buildTunnelMapInTx(ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to build tunnel map: %w", err)
	}
	s.log.Debug("graph: built tunnel map", "primary_mappings", len(tMaps.primary), "by_link_code_mappings", len(tMaps.byLinkCode))

	now := time.Now()
	var unmatchedNeighbors int

	// Collect all updates into batches
	var linkUpdates []map[string]any
	var deviceUpdates []map[string]any
	var adjUpdates []map[string]any

	for _, lsp := range lsps {
		for _, neighbor := range lsp.Neighbors {
			mapping, found, ambiguous := tMaps.resolve(lsp.Hostname, neighbor.NeighborAddr)
			if !found {
				unmatchedNeighbors++
				continue
			}
			if ambiguous {
				s.log.Error("graph: duplicate tunnel_net IP with no hostname override, attribution may be wrong",
					"hostname", lsp.Hostname,
					"neighbor_addr", neighbor.NeighborAddr,
					"link", mapping.linkCode)
			}

			linkUpdates = append(linkUpdates, map[string]any{
				"pk":        mapping.linkPK,
				"metric":    neighbor.Metric,
				"adj_sids":  neighbor.AdjSIDs,
				"last_sync": now.Unix(),
			})
			deviceUpdates = append(deviceUpdates, map[string]any{
				"pk":        mapping.localPK,
				"system_id": lsp.SystemID,
				"router_id": lsp.RouterID,
				"last_sync": now.Unix(),
			})
			adjUpdates = append(adjUpdates, map[string]any{
				"from_pk":       mapping.localPK,
				"to_pk":         mapping.neighborPK,
				"metric":        neighbor.Metric,
				"neighbor_addr": neighbor.NeighborAddr,
				"adj_sids":      neighbor.AdjSIDs,
				"last_seen":     now.Unix(),
				"bandwidth_bps": mapping.bandwidth,
			})
		}
	}

	// Execute batched updates within the transaction
	if err := batchUpdateLinksISISTx(ctx, tx, linkUpdates); err != nil {
		return fmt.Errorf("failed to batch update links: %w", err)
	}
	if err := batchUpdateDevicesISISTx(ctx, tx, deviceUpdates); err != nil {
		return fmt.Errorf("failed to batch update devices: %w", err)
	}
	if err := batchCreateISISAdjacentTx(ctx, tx, adjUpdates); err != nil {
		return fmt.Errorf("failed to batch create adjacencies: %w", err)
	}

	s.log.Debug("graph: ISIS sync in transaction completed",
		"adjacencies_created", len(adjUpdates),
		"links_updated", len(linkUpdates),
		"devices_updated", len(deviceUpdates),
		"unmatched_neighbors", unmatchedNeighbors)

	// Write ISIS data to ClickHouse
	if err := s.writeISISToClickHouse(ctx, lsps, tMaps); err != nil {
		s.log.Warn("graph: failed to write ISIS data to ClickHouse", "error", err)
	}

	return nil
}

// buildTunnelMapInTx queries Links within a transaction.
func (s *Store) buildTunnelMapInTx(ctx context.Context, tx neo4j.Transaction) (*tunnelMaps, error) {
	cypher := `
		MATCH (link:Link)
		WHERE link.tunnel_net IS NOT NULL AND link.tunnel_net <> ''
		MATCH (link)-[:CONNECTS {side: 'A'}]->(devA:Device)
		MATCH (link)-[:CONNECTS {side: 'Z'}]->(devZ:Device)
		RETURN link.pk AS pk, link.tunnel_net AS tunnel_net, link.code AS code, devA.pk AS side_a_pk, devZ.pk AS side_z_pk,
		       coalesce(link.bandwidth, 0) AS bandwidth,
		       link.status IN ['soft-drained', 'hard-drained'] AS is_drained
		ORDER BY link.pk
	`
	result, err := tx.Run(ctx, cypher, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query links: %w", err)
	}

	maps := &tunnelMaps{
		primary:      make(map[string]tunnelMapping),
		byLinkCode:   make(map[string]tunnelMapping),
		duplicateIPs: make(map[string]bool),
	}

	for result.Next(ctx) {
		record := result.Record()
		tunnelNet, _ := record.Get("tunnel_net")
		sideAPK, _ := record.Get("side_a_pk")
		sideZPK, _ := record.Get("side_z_pk")
		linkPK, _ := record.Get("pk")
		linkCode, _ := record.Get("code")
		bandwidth, _ := record.Get("bandwidth")
		isDrained, _ := record.Get("is_drained")

		tunnelNetStr, ok := tunnelNet.(string)
		if !ok || tunnelNetStr == "" {
			continue
		}
		sideAPKStr, _ := sideAPK.(string)
		sideZPKStr, _ := sideZPK.(string)
		linkPKStr, _ := linkPK.(string)
		linkCodeStr, _ := linkCode.(string)
		bandwidthInt, _ := bandwidth.(int64)
		isDrainedBool, _ := isDrained.(bool)

		ip1, ip2, err := parseTunnelNet31(tunnelNetStr)
		if err != nil {
			s.log.Debug("graph: failed to parse tunnel_net",
				"tunnel_net", tunnelNetStr,
				"error", err)
			continue
		}

		mappingA := tunnelMapping{
			linkPK:     linkPKStr,
			linkCode:   linkCodeStr,
			neighborPK: sideAPKStr,
			localPK:    sideZPKStr,
			bandwidth:  bandwidthInt,
			isDrained:  isDrainedBool,
		}
		mappingZ := tunnelMapping{
			linkPK:     linkPKStr,
			linkCode:   linkCodeStr,
			neighborPK: sideZPKStr,
			localPK:    sideAPKStr,
			bandwidth:  bandwidthInt,
			isDrained:  isDrainedBool,
		}

		maps.byLinkCode[linkCodeStr+":"+ip1] = mappingA
		maps.byLinkCode[linkCodeStr+":"+ip2] = mappingZ

		if existing, ok := maps.primary[ip1]; ok {
			maps.duplicateIPs[ip1] = true
			maps.duplicateIPs[ip2] = true
			s.log.Warn("graph: duplicate tunnel_net IP, using hostname overrides for disambiguation",
				"ip", ip1,
				"tunnel_net", tunnelNetStr,
				"link", linkCodeStr,
				"existing_link", existing.linkCode)
			continue
		}

		maps.primary[ip1] = mappingA
		maps.primary[ip2] = mappingZ
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error iterating results: %w", err)
	}

	return maps, nil
}

// updateLinkISISInTx updates a Link node with IS-IS metric data within a transaction.
func batchUpdateLinksISISTx(ctx context.Context, tx neo4j.Transaction, updates []map[string]any) error {
	if len(updates) == 0 {
		return nil
	}
	cypher := `
		UNWIND $updates AS u
		MATCH (link:Link {pk: u.pk})
		SET link.isis_metric = u.metric,
		    link.isis_adj_sids = u.adj_sids,
		    link.isis_last_sync = u.last_sync
	`
	res, err := tx.Run(ctx, cypher, map[string]any{"updates": updates})
	if err != nil {
		return err
	}
	_, err = res.Consume(ctx)
	return err
}

func batchUpdateDevicesISISTx(ctx context.Context, tx neo4j.Transaction, updates []map[string]any) error {
	if len(updates) == 0 {
		return nil
	}
	cypher := `
		UNWIND $updates AS u
		MATCH (d:Device {pk: u.pk})
		SET d.isis_system_id = u.system_id,
		    d.isis_router_id = u.router_id,
		    d.isis_last_sync = u.last_sync
	`
	res, err := tx.Run(ctx, cypher, map[string]any{"updates": updates})
	if err != nil {
		return err
	}
	_, err = res.Consume(ctx)
	return err
}

func batchCreateISISAdjacentTx(ctx context.Context, tx neo4j.Transaction, updates []map[string]any) error {
	if len(updates) == 0 {
		return nil
	}
	cypher := `
		UNWIND $updates AS u
		MATCH (d1:Device {pk: u.from_pk})
		MATCH (d2:Device {pk: u.to_pk})
		MERGE (d1)-[r:ISIS_ADJACENT]->(d2)
		SET r.metric = u.metric,
		    r.neighbor_addr = u.neighbor_addr,
		    r.adj_sids = u.adj_sids,
		    r.last_seen = u.last_seen,
		    r.bandwidth_bps = u.bandwidth_bps
	`
	res, err := tx.Run(ctx, cypher, map[string]any{"updates": updates})
	if err != nil {
		return err
	}
	_, err = res.Consume(ctx)
	return err
}

// batchCreateContributors creates all Contributor nodes in a single batched query.
func batchCreateContributors(ctx context.Context, tx neo4j.Transaction, contributors []dzsvc.Contributor) error {
	if len(contributors) == 0 {
		return nil
	}

	items := make([]map[string]any, len(contributors))
	for i, c := range contributors {
		items[i] = map[string]any{
			"pk":   c.PK,
			"code": c.Code,
			"name": c.Name,
		}
	}

	cypher := `
		UNWIND $items AS item
		CREATE (c:Contributor {pk: item.pk, code: item.code, name: item.name})
	`
	res, err := tx.Run(ctx, cypher, map[string]any{"items": items})
	if err != nil {
		return err
	}
	_, err = res.Consume(ctx)
	return err
}

// batchCreateMetros creates all Metro nodes in a single batched query.
func batchCreateMetros(ctx context.Context, tx neo4j.Transaction, metros []dzsvc.Metro) error {
	if len(metros) == 0 {
		return nil
	}

	items := make([]map[string]any, len(metros))
	for i, m := range metros {
		items[i] = map[string]any{
			"pk":        m.PK,
			"code":      m.Code,
			"name":      m.Name,
			"longitude": m.Longitude,
			"latitude":  m.Latitude,
		}
	}

	cypher := `
		UNWIND $items AS item
		CREATE (m:Metro {pk: item.pk, code: item.code, name: item.name, longitude: item.longitude, latitude: item.latitude})
	`
	res, err := tx.Run(ctx, cypher, map[string]any{"items": items})
	if err != nil {
		return err
	}
	_, err = res.Consume(ctx)
	return err
}

// batchCreateDevices creates all Device nodes and their relationships in batched queries.
func batchCreateDevices(ctx context.Context, tx neo4j.Transaction, devices []dzsvc.Device) error {
	if len(devices) == 0 {
		return nil
	}

	items := make([]map[string]any, len(devices))
	for i, d := range devices {
		items[i] = map[string]any{
			"pk":             d.PK,
			"status":         d.Status,
			"device_type":    d.DeviceType,
			"code":           d.Code,
			"public_ip":      d.PublicIP,
			"max_users":      d.MaxUsers,
			"contributor_pk": d.ContributorPK,
			"metro_pk":       d.MetroPK,
		}
	}

	// Create device nodes
	cypherNodes := `
		UNWIND $items AS item
		CREATE (d:Device {
			pk: item.pk,
			status: item.status,
			device_type: item.device_type,
			code: item.code,
			public_ip: item.public_ip,
			max_users: item.max_users
		})
	`
	res, err := tx.Run(ctx, cypherNodes, map[string]any{"items": items})
	if err != nil {
		return err
	}
	if _, err := res.Consume(ctx); err != nil {
		return err
	}

	// Create OPERATES relationships to Contributors
	cypherOperates := `
		UNWIND $items AS item
		MATCH (d:Device {pk: item.pk})
		MATCH (c:Contributor {pk: item.contributor_pk})
		CREATE (d)-[:OPERATES]->(c)
	`
	res, err = tx.Run(ctx, cypherOperates, map[string]any{"items": items})
	if err != nil {
		return err
	}
	if _, err := res.Consume(ctx); err != nil {
		return err
	}

	// Create LOCATED_IN relationships to Metros
	cypherLocatedIn := `
		UNWIND $items AS item
		MATCH (d:Device {pk: item.pk})
		MATCH (m:Metro {pk: item.metro_pk})
		CREATE (d)-[:LOCATED_IN]->(m)
	`
	res, err = tx.Run(ctx, cypherLocatedIn, map[string]any{"items": items})
	if err != nil {
		return err
	}
	_, err = res.Consume(ctx)
	return err
}

// batchCreateLinks creates all Link nodes and their relationships in batched queries.
func batchCreateLinks(ctx context.Context, tx neo4j.Transaction, links []dzsvc.Link) error {
	if len(links) == 0 {
		return nil
	}

	items := make([]map[string]any, len(links))
	for i, l := range links {
		items[i] = map[string]any{
			"pk":                     l.PK,
			"status":                 l.Status,
			"code":                   l.Code,
			"tunnel_net":             l.TunnelNet,
			"link_type":              l.LinkType,
			"committed_rtt_ns":       l.CommittedRTTNs,
			"committed_jitter_ns":    l.CommittedJitterNs,
			"bandwidth":              l.Bandwidth,
			"isis_delay_override_ns": l.ISISDelayOverrideNs,
			"contributor_pk":         l.ContributorPK,
			"side_a_pk":              l.SideAPK,
			"side_z_pk":              l.SideZPK,
			"side_a_iface_name":      l.SideAIfaceName,
			"side_z_iface_name":      l.SideZIfaceName,
		}
	}

	// Create link nodes
	cypherNodes := `
		UNWIND $items AS item
		CREATE (link:Link {
			pk: item.pk,
			status: item.status,
			code: item.code,
			tunnel_net: item.tunnel_net,
			link_type: item.link_type,
			committed_rtt_ns: item.committed_rtt_ns,
			committed_jitter_ns: item.committed_jitter_ns,
			bandwidth: item.bandwidth,
			isis_delay_override_ns: item.isis_delay_override_ns
		})
	`
	res, err := tx.Run(ctx, cypherNodes, map[string]any{"items": items})
	if err != nil {
		return err
	}
	if _, err := res.Consume(ctx); err != nil {
		return err
	}

	// Create OWNED_BY relationships to Contributors
	cypherOwnedBy := `
		UNWIND $items AS item
		MATCH (link:Link {pk: item.pk})
		MATCH (c:Contributor {pk: item.contributor_pk})
		CREATE (link)-[:OWNED_BY]->(c)
	`
	res, err = tx.Run(ctx, cypherOwnedBy, map[string]any{"items": items})
	if err != nil {
		return err
	}
	if _, err := res.Consume(ctx); err != nil {
		return err
	}

	// Create CONNECTS relationships to side A devices
	cypherConnectsA := `
		UNWIND $items AS item
		MATCH (link:Link {pk: item.pk})
		MATCH (devA:Device {pk: item.side_a_pk})
		CREATE (link)-[:CONNECTS {side: 'A', iface_name: item.side_a_iface_name}]->(devA)
	`
	res, err = tx.Run(ctx, cypherConnectsA, map[string]any{"items": items})
	if err != nil {
		return err
	}
	if _, err := res.Consume(ctx); err != nil {
		return err
	}

	// Create CONNECTS relationships to side Z devices
	cypherConnectsZ := `
		UNWIND $items AS item
		MATCH (link:Link {pk: item.pk})
		MATCH (devZ:Device {pk: item.side_z_pk})
		CREATE (link)-[:CONNECTS {side: 'Z', iface_name: item.side_z_iface_name}]->(devZ)
	`
	res, err = tx.Run(ctx, cypherConnectsZ, map[string]any{"items": items})
	if err != nil {
		return err
	}
	_, err = res.Consume(ctx)
	return err
}

// batchCreateUsers creates all User nodes and their relationships in batched queries.
func batchCreateUsers(ctx context.Context, tx neo4j.Transaction, users []dzsvc.User) error {
	if len(users) == 0 {
		return nil
	}

	items := make([]map[string]any, len(users))
	for i, u := range users {
		var clientIP, dzIP string
		if u.ClientIP != nil {
			clientIP = u.ClientIP.String()
		}
		if u.DZIP != nil {
			dzIP = u.DZIP.String()
		}
		items[i] = map[string]any{
			"pk":           u.PK,
			"owner_pubkey": u.OwnerPubkey,
			"status":       u.Status,
			"kind":         u.Kind,
			"client_ip":    clientIP,
			"dz_ip":        dzIP,
			"tunnel_id":    u.TunnelID,
			"device_pk":    u.DevicePK,
		}
	}

	// Create user nodes
	cypherNodes := `
		UNWIND $items AS item
		CREATE (user:User {
			pk: item.pk,
			owner_pubkey: item.owner_pubkey,
			status: item.status,
			kind: item.kind,
			client_ip: item.client_ip,
			dz_ip: item.dz_ip,
			tunnel_id: item.tunnel_id
		})
	`
	res, err := tx.Run(ctx, cypherNodes, map[string]any{"items": items})
	if err != nil {
		return err
	}
	if _, err := res.Consume(ctx); err != nil {
		return err
	}

	// Create ASSIGNED_TO relationships to Devices
	cypherAssignedTo := `
		UNWIND $items AS item
		MATCH (user:User {pk: item.pk})
		MATCH (dev:Device {pk: item.device_pk})
		CREATE (user)-[:ASSIGNED_TO]->(dev)
	`
	res, err = tx.Run(ctx, cypherAssignedTo, map[string]any{"items": items})
	if err != nil {
		return err
	}
	_, err = res.Consume(ctx)
	return err
}

// tunnelMapping maps a tunnel IP address to Link and Device information.
type tunnelMapping struct {
	linkPK     string // Link primary key
	linkCode   string // Link code (e.g. "dz-fra-01:dzd-fra-01")
	neighborPK string // Device PK of the neighbor (device with this IP)
	localPK    string // Device PK of the other side
	bandwidth  int64  // Link bandwidth in bps
	isDrained  bool   // Whether the link is drained (status is soft-drained or hard-drained)
}

// TODO: Remove this once the duplicate tunnel_net values are fixed in the network
// configuration. Currently 6 tunnel nets are each assigned to 2 different links.
// This map disambiguates ISIS hostnames to the correct link code. Hostnames that
// appear in multiple duplicate tunnel nets (e.g. dz-lax-sw01, AU1C-NSWP-DZ01) are
// listed once per link — the lookup uses hostname+IP to resolve the correct entry.
var duplicateTunnelHostnameToLinkCodes = map[string][]string{
	// 172.16.0.212/31 — dz-fra-01:dzd-fra-01 vs lax001-dz002:tyo002-dz002
	"frankfurt":  {"dz-fra-01:dzd-fra-01"},
	"dzd-fra-01": {"dz-fra-01:dzd-fra-01", "dzd-fra-01:dzd-tok-01"},
	"la2-dz002":  {"lax001-dz002:tyo002-dz002"},
	"ty2-dz002":  {"lax001-dz002:tyo002-dz002"},

	// 172.16.0.40/31 — dz100a-lax1-tsw:dz-lax-sw01 vs tyo001-dz002:sin001-dz002
	"dz100a.lax1.teraswitch.com": {"dz100a-lax1-tsw:dz-lax-sw01"},
	"dz-lax-sw01":                {"dz100a-lax1-tsw:dz-lax-sw01", "dz-slc-sw01:dz-lax-sw01"},
	"cc1t-dz002":                 {"tyo001-dz002:sin001-dz002"},
	"sg1t1-dz002":                {"tyo001-dz002:sin001-dz002"},

	// 172.16.1.132/31 — au1c-dz01:la2r-dz01 vs dub001-dz001:dgt-dzd-dub-db2
	"AU1C-NSWP-DZ01":  {"au1c-dz01:la2r-dz01", "au1c-dz01:dz-ch2-sw01"},
	"LA2R-NSWP-DZ01":  {"au1c-dz01:la2r-dz01"},
	"DGT-DZD-DUB-DB2": {"dub001-dz001:dgt-dzd-dub-db2"},
	"dub2t1-dz001":    {"dub001-dz001:dgt-dzd-dub-db2"},

	// 172.16.1.230/31 — dz100a-sea1-tsw:dz115a-tyo2-tsw vs allnodes-fra1:dz-fr5-sw01
	"dz100a.sea1.teraswitch.com": {"dz100a-sea1-tsw:dz115a-tyo2-tsw"},
	"dz115a.tyo2.teraswitch.com": {"dz100a-sea1-tsw:dz115a-tyo2-tsw"},
	"DZ-FR5-SW01":                {"allnodes-fra1:dz-fr5-sw01"},
	"dz-1":                       {"allnodes-fra1:dz-fr5-sw01"},

	// 172.16.1.234/31 — dzd-fra-01:dzd-tok-01 vs dz100a-sgp1-tsw:dz100a-fra2-tsw
	"dzd-tok-01":                 {"dzd-fra-01:dzd-tok-01"},
	"dz100a.fra2.teraswitch.com": {"dz100a-sgp1-tsw:dz100a-fra2-tsw"},
	"dz100a.sgp1.teraswitch.com": {"dz100a-sgp1-tsw:dz100a-fra2-tsw"},

	// 172.16.1.32/31 — dz-slc-sw01:dz-lax-sw01 vs au1c-dz01:dz-ch2-sw01
	"dz-slc-sw01": {"dz-slc-sw01:dz-lax-sw01"},
	"DZ-CH2-SW01": {"au1c-dz01:dz-ch2-sw01"},
}

// SyncISIS updates the Neo4j graph with IS-IS adjacency data.
// It correlates IS-IS neighbors with existing Links via tunnel_net IP addresses,
// creates ISIS_ADJACENT relationships between Devices, and updates Link properties.
func (s *Store) SyncISIS(ctx context.Context, lsps []isis.LSP) error {
	s.log.Debug("graph: starting ISIS sync", "lsps", len(lsps))

	session, err := s.cfg.Neo4j.Session(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Neo4j session: %w", err)
	}
	defer session.Close(ctx)

	// Step 1: Query all Links with their tunnel_net and side device PKs
	tMaps, err := s.buildTunnelMap(ctx, session)
	if err != nil {
		return fmt.Errorf("failed to build tunnel map: %w", err)
	}
	s.log.Debug("graph: built tunnel map", "primary_mappings", len(tMaps.primary), "by_link_code_mappings", len(tMaps.byLinkCode))

	now := time.Now()
	var unmatchedNeighbors int

	// Step 2: Collect all updates into batches
	var linkUpdates []map[string]any
	var deviceUpdates []map[string]any
	var adjUpdates []map[string]any

	for _, lsp := range lsps {
		for _, neighbor := range lsp.Neighbors {
			mapping, found, ambiguous := tMaps.resolve(lsp.Hostname, neighbor.NeighborAddr)
			if !found {
				unmatchedNeighbors++
				s.log.Debug("graph: unmatched IS-IS neighbor",
					"neighbor_addr", neighbor.NeighborAddr,
					"neighbor_system_id", neighbor.SystemID)
				continue
			}
			if ambiguous {
				s.log.Error("graph: duplicate tunnel_net IP with no hostname override, attribution may be wrong",
					"hostname", lsp.Hostname,
					"neighbor_addr", neighbor.NeighborAddr,
					"link", mapping.linkCode)
			}

			linkUpdates = append(linkUpdates, map[string]any{
				"pk":        mapping.linkPK,
				"metric":    neighbor.Metric,
				"adj_sids":  neighbor.AdjSIDs,
				"last_sync": now.Unix(),
			})
			deviceUpdates = append(deviceUpdates, map[string]any{
				"pk":        mapping.localPK,
				"system_id": lsp.SystemID,
				"router_id": lsp.RouterID,
				"last_sync": now.Unix(),
			})
			adjUpdates = append(adjUpdates, map[string]any{
				"from_pk":       mapping.localPK,
				"to_pk":         mapping.neighborPK,
				"metric":        neighbor.Metric,
				"neighbor_addr": neighbor.NeighborAddr,
				"adj_sids":      neighbor.AdjSIDs,
				"last_seen":     now.Unix(),
				"bandwidth_bps": mapping.bandwidth,
			})
		}
	}

	// Step 3: Execute batched updates (3 queries instead of N*3)
	if err := batchUpdateLinksISIS(ctx, session, linkUpdates); err != nil {
		return fmt.Errorf("failed to batch update links: %w", err)
	}
	if err := batchUpdateDevicesISIS(ctx, session, deviceUpdates); err != nil {
		return fmt.Errorf("failed to batch update devices: %w", err)
	}
	if err := batchCreateISISAdjacent(ctx, session, adjUpdates); err != nil {
		return fmt.Errorf("failed to batch create adjacencies: %w", err)
	}

	s.log.Info("graph: ISIS sync completed",
		"lsps", len(lsps),
		"adjacencies_created", len(adjUpdates),
		"links_updated", len(linkUpdates),
		"devices_updated", len(deviceUpdates),
		"unmatched_neighbors", unmatchedNeighbors)

	// Write ISIS data to ClickHouse
	if err := s.writeISISToClickHouse(ctx, lsps, tMaps); err != nil {
		s.log.Warn("graph: failed to write ISIS data to ClickHouse", "error", err)
	}

	return nil
}

// tunnelMaps holds the primary tunnel map and an override map for duplicate tunnel nets.
type tunnelMaps struct {
	// primary maps tunnel IP → tunnelMapping (first link wins for duplicates)
	primary map[string]tunnelMapping
	// byLinkCode maps "linkCode:ip" → tunnelMapping for duplicate tunnel nets,
	// allowing hostname-based disambiguation via duplicateTunnelHostnameToLinkCodes.
	byLinkCode map[string]tunnelMapping
	// duplicateIPs tracks IPs that belong to more than one link's tunnel_net.
	duplicateIPs map[string]bool
}

// resolve looks up the correct tunnelMapping for a given hostname and neighbor IP.
// It checks hostname-based overrides for duplicate tunnel nets first, then falls back
// to the primary tunnel map. Returns an additional boolean indicating whether the
// result is an unresolved duplicate (hostname not in the override map).
func (t *tunnelMaps) resolve(hostname, neighborAddr string) (tunnelMapping, bool, bool) {
	// TODO: Remove this override logic once duplicate tunnel_net values are fixed.
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

// buildTunnelMap queries Links from Neo4j and builds a map from IP addresses to tunnel mappings.
// For each /31 tunnel_net, both IPs are mapped: one points to side_a as neighbor, one to side_z.
func (s *Store) buildTunnelMap(ctx context.Context, session neo4j.Session) (*tunnelMaps, error) {
	cypher := `
		MATCH (link:Link)
		WHERE link.tunnel_net IS NOT NULL AND link.tunnel_net <> ''
		MATCH (link)-[:CONNECTS {side: 'A'}]->(devA:Device)
		MATCH (link)-[:CONNECTS {side: 'Z'}]->(devZ:Device)
		RETURN link.pk AS pk, link.tunnel_net AS tunnel_net, link.code AS code, devA.pk AS side_a_pk, devZ.pk AS side_z_pk,
		       coalesce(link.bandwidth, 0) AS bandwidth,
		       link.status IN ['soft-drained', 'hard-drained'] AS is_drained
		ORDER BY link.pk
	`
	result, err := session.Run(ctx, cypher, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query links: %w", err)
	}

	maps := &tunnelMaps{
		primary:      make(map[string]tunnelMapping),
		byLinkCode:   make(map[string]tunnelMapping),
		duplicateIPs: make(map[string]bool),
	}

	for result.Next(ctx) {
		record := result.Record()
		tunnelNet, _ := record.Get("tunnel_net")
		sideAPK, _ := record.Get("side_a_pk")
		sideZPK, _ := record.Get("side_z_pk")
		linkPK, _ := record.Get("pk")
		linkCode, _ := record.Get("code")
		bandwidth, _ := record.Get("bandwidth")
		isDrained, _ := record.Get("is_drained")

		tunnelNetStr, ok := tunnelNet.(string)
		if !ok || tunnelNetStr == "" {
			continue
		}
		sideAPKStr, _ := sideAPK.(string)
		sideZPKStr, _ := sideZPK.(string)
		linkPKStr, _ := linkPK.(string)
		linkCodeStr, _ := linkCode.(string)
		bandwidthInt, _ := bandwidth.(int64)
		isDrainedBool, _ := isDrained.(bool)

		// Parse the /31 CIDR to get both IPs
		ip1, ip2, err := parseTunnelNet31(tunnelNetStr)
		if err != nil {
			s.log.Debug("graph: failed to parse tunnel_net",
				"tunnel_net", tunnelNetStr,
				"error", err)
			continue
		}

		mappingA := tunnelMapping{
			linkPK:     linkPKStr,
			linkCode:   linkCodeStr,
			neighborPK: sideAPKStr,
			localPK:    sideZPKStr,
			bandwidth:  bandwidthInt,
			isDrained:  isDrainedBool,
		}
		mappingZ := tunnelMapping{
			linkPK:     linkPKStr,
			linkCode:   linkCodeStr,
			neighborPK: sideZPKStr,
			localPK:    sideAPKStr,
			bandwidth:  bandwidthInt,
			isDrained:  isDrainedBool,
		}

		// Always store in the byLinkCode map for override lookups
		maps.byLinkCode[linkCodeStr+":"+ip1] = mappingA
		maps.byLinkCode[linkCodeStr+":"+ip2] = mappingZ

		// For the primary map, first link (by pk sort) wins on duplicate IPs
		if existing, ok := maps.primary[ip1]; ok {
			maps.duplicateIPs[ip1] = true
			maps.duplicateIPs[ip2] = true
			s.log.Warn("graph: duplicate tunnel_net IP, using hostname overrides for disambiguation",
				"ip", ip1,
				"tunnel_net", tunnelNetStr,
				"link", linkCodeStr,
				"existing_link", existing.linkCode)
			continue
		}

		maps.primary[ip1] = mappingA
		maps.primary[ip2] = mappingZ
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error iterating results: %w", err)
	}

	return maps, nil
}

// batchUpdateLinksISIS updates Link nodes with IS-IS metric data in a single UNWIND query.
func batchUpdateLinksISIS(ctx context.Context, session neo4j.Session, updates []map[string]any) error {
	if len(updates) == 0 {
		return nil
	}
	cypher := `
		UNWIND $updates AS u
		MATCH (link:Link {pk: u.pk})
		SET link.isis_metric = u.metric,
		    link.isis_adj_sids = u.adj_sids,
		    link.isis_last_sync = u.last_sync
	`
	res, err := session.Run(ctx, cypher, map[string]any{"updates": updates})
	if err != nil {
		return err
	}
	_, err = res.Consume(ctx)
	return err
}

// batchUpdateDevicesISIS updates Device nodes with IS-IS properties in a single UNWIND query.
func batchUpdateDevicesISIS(ctx context.Context, session neo4j.Session, updates []map[string]any) error {
	if len(updates) == 0 {
		return nil
	}
	cypher := `
		UNWIND $updates AS u
		MATCH (d:Device {pk: u.pk})
		SET d.isis_system_id = u.system_id,
		    d.isis_router_id = u.router_id,
		    d.isis_last_sync = u.last_sync
	`
	res, err := session.Run(ctx, cypher, map[string]any{"updates": updates})
	if err != nil {
		return err
	}
	_, err = res.Consume(ctx)
	return err
}

// batchCreateISISAdjacent creates/updates ISIS_ADJACENT relationships in a single UNWIND query.
func batchCreateISISAdjacent(ctx context.Context, session neo4j.Session, updates []map[string]any) error {
	if len(updates) == 0 {
		return nil
	}
	cypher := `
		UNWIND $updates AS u
		MATCH (d1:Device {pk: u.from_pk})
		MATCH (d2:Device {pk: u.to_pk})
		MERGE (d1)-[r:ISIS_ADJACENT]->(d2)
		SET r.metric = u.metric,
		    r.neighbor_addr = u.neighbor_addr,
		    r.adj_sids = u.adj_sids,
		    r.last_seen = u.last_seen,
		    r.bandwidth_bps = u.bandwidth_bps
	`
	res, err := session.Run(ctx, cypher, map[string]any{"updates": updates})
	if err != nil {
		return err
	}
	_, err = res.Consume(ctx)
	return err
}

// writeISISToClickHouse builds ISIS adjacency and device slices from LSPs and writes them to ClickHouse.
// All adjacencies are included (matched and unmatched). device_pk and link_pk are enrichment columns
// populated via tunnel net correlation when possible, empty when not.
func (s *Store) writeISISToClickHouse(ctx context.Context, lsps []isis.LSP, tMaps *tunnelMaps) error {
	isisStore, err := isis.NewStore(isis.StoreConfig{
		Logger:     s.log,
		ClickHouse: s.cfg.ClickHouse,
	})
	if err != nil {
		return fmt.Errorf("failed to create ISIS store: %w", err)
	}

	// Build device lookup: system_id -> device_pk (from tunnel map matches)
	devicePKBySystemID := make(map[string]string)
	for _, lsp := range lsps {
		for _, neighbor := range lsp.Neighbors {
			if mapping, found, _ := tMaps.resolve(lsp.Hostname, neighbor.NeighborAddr); found {
				devicePKBySystemID[lsp.SystemID] = mapping.localPK
			}
		}
	}

	// Build adjacency and device slices
	var adjacencies []isis.Adjacency
	for _, lsp := range lsps {
		for _, neighbor := range lsp.Neighbors {
			adj := isis.Adjacency{
				SystemID:         lsp.SystemID,
				NeighborSystemID: neighbor.SystemID,
				NeighborAddr:     neighbor.NeighborAddr,
				Hostname:         lsp.Hostname,
				RouterID:         lsp.RouterID,
				LocalAddr:        neighbor.LocalAddr,
				Metric:           int64(neighbor.Metric),
				AdjSIDs:          isis.AdjSIDsToJSON(neighbor.AdjSIDs),
			}
			// Enrich with device_pk and link_pk if we have a tunnel match
			if mapping, found, _ := tMaps.resolve(lsp.Hostname, neighbor.NeighborAddr); found {
				adj.DevicePK = mapping.localPK
				adj.LinkPK = mapping.linkPK
			}
			adjacencies = append(adjacencies, adj)
		}
	}

	var devices []isis.Device
	for _, lsp := range lsps {
		var overload, nodeUnreachable uint8
		if lsp.Overload {
			overload = 1
		}
		if lsp.NodeUnreachable {
			nodeUnreachable = 1
		}
		dev := isis.Device{
			SystemID:        lsp.SystemID,
			DevicePK:        devicePKBySystemID[lsp.SystemID],
			Hostname:        lsp.Hostname,
			RouterID:        lsp.RouterID,
			Overload:        overload,
			NodeUnreachable: nodeUnreachable,
			Sequence:        lsp.Sequence,
		}
		devices = append(devices, dev)
	}

	if err := isisStore.ReplaceAdjacencies(ctx, adjacencies); err != nil {
		return fmt.Errorf("failed to replace adjacencies: %w", err)
	}

	if err := isisStore.ReplaceDevices(ctx, devices); err != nil {
		return fmt.Errorf("failed to replace devices: %w", err)
	}

	s.log.Info("graph: wrote ISIS data to ClickHouse",
		"adjacencies", len(adjacencies),
		"devices", len(devices))

	return nil
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

	// For a /31, the network address and broadcast address are the two usable IPs
	ip := ipnet.IP.To4()
	if ip == nil {
		return "", "", fmt.Errorf("not an IPv4 address")
	}

	// First IP (network address in /31 is usable)
	ip1 := make(net.IP, 4)
	copy(ip1, ip)

	// Second IP (broadcast address in /31 is usable)
	ip2 := make(net.IP, 4)
	copy(ip2, ip)
	ip2[3]++

	return ip1.String(), ip2.String(), nil
}
