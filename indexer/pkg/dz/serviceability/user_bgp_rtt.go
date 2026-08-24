package dzsvc

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// The BGP round-trip time between a client box and its DoubleZero device, taken from the
// serviceability User account and written as a FACT.
//
// It is deliberately not a column on the users dimension. attrs_hash covers every payload column
// there, so a hashed rtt would mint a history row per user on every onchain rewrite — the churn
// 20260708000001 refused for last_bgp_reported_at — and an unhashed one would freeze at whatever
// value was current when some other attribute last changed. A measurement over time is what a fact
// table is for, and modelling it as one also leaves the series behind.
//
// See the migration 20260822000001_fact_dz_user_bgp_rtt.sql for the rest of the reasoning.

// userBGPRTTSchema is the fact's shape. Column order must match ToRow.
type userBGPRTTSchema struct{}

func (s *userBGPRTTSchema) Name() string { return "dz_user_bgp_rtt" }

// UniqueKeyColumns keys on the onchain WRITE, not on the observation. The agent submits only on a
// status change or its ~6-hourly keepalive, while the indexer polls every 60s, so without this the
// table would carry two orders of magnitude more rows than there are reports — all identical.
func (s *userBGPRTTSchema) UniqueKeyColumns() []string {
	return []string{"user_pk", "reported_at_slot"}
}

func (s *userBGPRTTSchema) Columns() []string {
	return []string{
		"ingested_at:TIMESTAMP",
		"user_pk:VARCHAR",
		"device_pk:VARCHAR",
		"client_ip:VARCHAR",
		"dz_ip:VARCHAR",
		"tunnel_id:INTEGER",
		"reported_at_slot:BIGINT",
		"up_at_slot:BIGINT",
		"bgp_status:VARCHAR",
		"bgp_rtt_ns:BIGINT",
	}
}

func (s *userBGPRTTSchema) TimeColumn() string { return "event_ts" }

func (s *userBGPRTTSchema) PartitionByTime() bool { return true }

func (s *userBGPRTTSchema) Grain() string {
	return "one row per onchain BGP status write per user"
}

func (s *userBGPRTTSchema) DedupMode() dataset.DedupMode { return dataset.DedupReplacing }

func (s *userBGPRTTSchema) DedupVersionColumn() string { return "ingested_at" }

var userBGPRTTFactSchema = &userBGPRTTSchema{}

// NewUserBGPRTTDataset builds the fact dataset for the user BGP RTT series.
func NewUserBGPRTTDataset(log *slog.Logger) (*dataset.FactDataset, error) {
	return dataset.NewFactDataset(log, userBGPRTTFactSchema)
}

// userBGPRTTRow renders one user's current report.
//
// Two clocks, and they are different questions. event_ts is when this REPORT was first seen, which
// is what an age on the page has to be measured from; observedAt is this poll, and it is the dedup
// version, so the newest write of a report wins. Passing the poll clock for both is what made a
// six-hour-old keepalive render as seconds old.
//
// Order must match the PHYSICAL column order of fact_dz_user_bgp_rtt, not Columns(): WriteBatch
// issues a bare `INSERT INTO <table>` with no column list, so the batch is positional. That is why
// event_ts leads here while Columns() does not declare it at all — the same shape
// permissionevents.ToRow uses, and the reason its migration says the table's column order is part
// of the contract.
func userBGPRTTRow(u User, reportSeenAt, observedAt time.Time) []any {
	return []any{
		reportSeenAt,        // event_ts
		observedAt,          // ingested_at
		u.PK,                // user_pk
		u.DevicePK,          // device_pk
		u.ClientIP.String(), // client_ip
		u.DZIP.String(),     // dz_ip
		u.TunnelID,          // tunnel_id
		u.LastBgpReportedAt, // reported_at_slot
		u.LastBgpUpAt,       // up_at_slot
		u.BgpStatus,         // bgp_status
		u.BgpRttNs,          // bgp_rtt_ns
	}
}

// usersWithBGPReport filters to the users the agent has actually reported for.
//
// A user with reported_at_slot 0 has never had a report written: the account predates the field,
// or no agent has spoken for it. Writing those would put a row per such user under one slot-0 key
// carrying an rtt of 0, which is indistinguishable on read from a measured zero. They are dropped,
// and their absence from the fact is the honest statement.
func usersWithBGPReport(users []User) []User {
	out := make([]User, 0, len(users))
	for _, u := range users {
		if u.LastBgpReportedAt == 0 {
			continue
		}
		out = append(out, u)
	}
	return out
}

// userBGPRTTKey identifies one onchain report: the fact's unique key.
type userBGPRTTKey struct {
	userPK string
	slot   uint64
}

// firstSeenUserBGPRTT is when each of these reports was first observed, for the reports already in
// the table.
//
// This is what keeps event_ts meaning "when this report appeared" instead of "when we last looked".
// The indexer re-observes an unchanged report every 60s while the agent writes one every ~6 hours,
// and the dedup version is ingested_at, so stamping event_ts with the poll clock on every write
// leaves the surviving row carrying the newest poll — a keepalive from six hours ago reading as
// seconds old on a page whose whole point is that the figure can be hours old.
//
// Keyed on (user_pk, reported_at_slot), which is the fact's own unique key, and read with min() so
// it survives whatever the merge has or has not collapsed yet. The IN prunes on the primary key,
// and the table is small by construction — it grows with onchain writes, not with polls.
func (s *Store) firstSeenUserBGPRTT(ctx context.Context, conn clickhouse.Connection, users []User) (map[userBGPRTTKey]time.Time, error) {
	pks := make([]string, 0, len(users))
	for _, u := range users {
		pks = append(pks, u.PK)
	}
	rows, err := conn.Query(ctx, `
		SELECT user_pk, reported_at_slot, min(event_ts)
		FROM fact_dz_user_bgp_rtt
		WHERE user_pk IN (?)
		GROUP BY user_pk, reported_at_slot`, pks)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[userBGPRTTKey]time.Time{}
	for rows.Next() {
		var key userBGPRTTKey
		var at time.Time
		if err := rows.Scan(&key.userPK, &key.slot, &at); err != nil {
			return nil, err
		}
		out[key] = at.UTC()
	}
	return out, rows.Err()
}

// WriteUserBGPRTT appends the current BGP report for every user that has one.
//
// Every snapshot re-writes the same rows for users whose report has not changed; the unique key is
// the report, so ReplacingMergeTree collapses them and the table grows with onchain writes rather
// than with the poll loop.
//
// A re-observation carries the event_ts of the FIRST observation, not this poll's clock, so the
// column keeps describing the report rather than the loop that keeps finding it. A failed read-back
// is not fatal: the write proceeds with this poll's clock, which is the old behaviour and costs
// freshness on the age rather than the series.
func (s *Store) WriteUserBGPRTT(ctx context.Context, users []User, observedAt time.Time) error {
	reported := usersWithBGPReport(users)
	s.log.Debug("serviceability/store: writing user bgp rtt",
		"users", len(users), "reported", len(reported))
	if len(reported) == 0 {
		return nil
	}

	d, err := NewUserBGPRTTDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create user bgp rtt dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	firstSeen, err := s.firstSeenUserBGPRTT(ctx, conn, reported)
	if err != nil {
		s.log.Warn("serviceability/store: user bgp rtt first-seen read failed, stamping this poll",
			"error", err)
		firstSeen = nil
	}

	if err := d.WriteBatch(ctx, conn, len(reported), func(i int) ([]any, error) {
		u := reported[i]
		at := observedAt
		if seen, ok := firstSeen[userBGPRTTKey{userPK: u.PK, slot: u.LastBgpReportedAt}]; ok {
			at = seen
		}
		return userBGPRTTRow(u, at, observedAt), nil
	}); err != nil {
		return fmt.Errorf("failed to write user bgp rtt to ClickHouse: %w", err)
	}
	return nil
}
