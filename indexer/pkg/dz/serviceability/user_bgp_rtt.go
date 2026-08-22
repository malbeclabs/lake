package dzsvc

import (
	"context"
	"fmt"
	"log/slog"
	"time"

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
// Order must match the PHYSICAL column order of fact_dz_user_bgp_rtt, not Columns(): WriteBatch
// issues a bare `INSERT INTO <table>` with no column list, so the batch is positional. That is why
// event_ts leads here while Columns() does not declare it at all — the same shape
// permissionevents.ToRow uses, and the reason its migration says the table's column order is part
// of the contract.
func userBGPRTTRow(u User, observedAt time.Time) []any {
	return []any{
		observedAt,          // event_ts
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

// WriteUserBGPRTT appends the current BGP report for every user that has one.
//
// Every snapshot re-writes the same rows for users whose report has not changed; the unique key is
// the report, so ReplacingMergeTree collapses them and the table grows with onchain writes rather
// than with the poll loop.
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

	if err := d.WriteBatch(ctx, conn, len(reported), func(i int) ([]any, error) {
		return userBGPRTTRow(reported[i], observedAt), nil
	}); err != nil {
		return fmt.Errorf("failed to write user bgp rtt to ClickHouse: %w", err)
	}
	return nil
}
