-- +goose Up
--
-- The four tables an edge-feed recorder's analysis tier lands in: the per-datagram
-- base fact, per-segment-per-instance coverage, one row per contiguous run of
-- missing sequence numbers, and conformance findings.
--
-- These exist because the Sequence column on the edge multicast overview
-- (api/handlers/edge_multicast_sequence.go) currently derives its gap counters from
-- a decoded level-grain table in the feeds database. That source has three faults no
-- care in the handler can fix. It exists only where a decoder does, so a feed is
-- covered on the day a bot learns to fold its book rather than on the day it is
-- first recorded. The recorder's OWN loss is not separable from the publisher's --
-- comparing nodes catches a loss one node has alone and is structurally blind to one
-- they share, which is exactly what a load spike produces. And it is TTL-less and
-- sorted for symbol and instrument questions, so a fifteen-minute question reads
-- most of a day through a remoteSecure() proxy (~135M rows), which is why the page
-- folds a ten-minute refresher's cached payload instead and the freshness of a
-- sequence verdict is bounded by the refresher rather than by the feed.
--
-- Header grain fixes all three at once. A per-datagram row is tens of bytes against a
-- level row, it carries the recorder's own admitted drop as a NUMBER TO SUBTRACT
-- rather than an inference, and sorted by the channel instance it turns a
-- fifteen-minute question into a key scan inside a day partition.
--
--
-- ============================================================================
-- The one idea every table below is shaped by: the CHANNEL INSTANCE
-- ============================================================================
--
-- A sequence number means something only under (source address, Channel ID,
-- destination port). That tuple leads the sort key of all four tables, and it is not
-- a stylistic choice: an operator may run redundant publishers serving one channel to
-- one group and port, each advancing its own sequence space and its own Reset Count.
-- A tracker keyed any coarser reads every alternation between them as backward motion
-- in one direction, while in the other it lets one publisher's heartbeats cover the
-- other's total outage.
--
-- Two folds are deliberate and are made by the READER, not by these tables:
--
--   * The destination port is folded for BOOK-level counters. Only the sequence
--     number is per port role; Reset Count, the manifest and the channel state they
--     govern span the three ports one publisher serves a channel on, so splitting a
--     book's gap, its reset and its snapshot cycle across three rows leaves each of
--     them looking incomplete. Continuity still keys on the full instance including
--     the port -- that is what a sequence number is minted per -- which is why the
--     port is a column here and never folded at rest.
--   * The recording site is NEVER folded. Two vantages of one instance are two
--     observations, and merging them hides a recorder that is missing the feed. This
--     is why site, recorder and env are in every ORDER BY below; see "Identity" below
--     for what happens when they are left out.
--
--
-- ============================================================================
-- Loss is measured in SEQUENCE VALUES, never in time
-- ============================================================================
--
-- A gap is a run of sequence numbers nobody delivered, and its size is how many of
-- them there were. Duration is not a second way of saying the same thing: at 50
-- datagrams a second a three-second gap is a hundred and fifty missing, and on a
-- channel that only heartbeats it is three. A figure in seconds is therefore not
-- comparable between two channels, between two hours of one channel, or against
-- itself after a rate change -- it measures the feed's activity as much as the loss.
--
-- Timestamps are on every row because WHEN is how a reader places a gap against an
-- incident. They place; they never quantify. That is the one divergence from the
-- consuming handler, whose Episodes are today contiguous runs of SECONDS
-- (KalshiL2GapEpisode): a gap row already IS a contiguous run, of sequence numbers,
-- so the mapping is to send the run and its count and let before_ts/after_ts put it
-- on a chart. A seconds figure derived from a run is presentable beside the count. It
-- is not a substitute for it.
--
--
-- ============================================================================
-- Identity, idempotency, and why every ORDER BY ends in (site, recorder, env)
-- ============================================================================
--
-- Reprocessing is idempotent on (object_key, object_sha256): a re-run after an
-- analyser fix must replace rows rather than double them. That is ReplacingMergeTree,
-- and it collapses on the ORDER BY -- not on the columns anyone names elsewhere. So
-- the ORDER BY *is* the row identity, and anything missing from it is a row silently
-- merged away.
--
-- The design document these tables come from ordered sequence_gap by
-- (source_addr, channel_id, dst_port, era_index, missing_from), conformance_finding by
-- (rule_id, source_addr, channel_id, dst_port, window_start), and segment_coverage by
-- (source_addr, channel_id, dst_port, segment_seq). All three drop the recording site
-- from the identity, which contradicts the same document's rule that the site is never
-- folded -- and the contradiction is not theoretical. Two sites missing the same run of
-- one instance is the single most interesting row pair in the whole schema, because it
-- is the difference between one recorder's branch and the publisher's own loss, and
-- under those keys OPTIMIZE FINAL leaves exactly one of them. Measured before writing
-- this file: two sites, one gap, two inserts, one row survives.
--
-- conformance_finding drops a second one. Its own prose says "the same window legally
-- holds two verdicts from two versions" -- a rule added next month runs against last
-- month's traffic -- and rule_set_version was then not in the key, so the older
-- version's verdict is deleted by the newer one's arrival and the dashboard can never
-- show that the rule set improved. It is second in the key here, right after rule_id,
-- because the panels that stack verdicts by rule want both versions side by side.
--
-- Every ORDER BY below therefore ends in (site, recorder, env). All three are
-- LowCardinality and sit after the columns any query actually ranges on, so they cost
-- a tiebreak in the primary index and nothing in a scan. env is included because these
-- are the RECORDER's environments, not lake's -- lake separates its own by database
-- (lake_testnet, lake_devnet), but one site may host a mainnet and a testnet recorder
-- under the same recorder name, and that pair must not collapse.
--
-- The version column is ingested_at on all four, matching fact_dz_user_bgp_rtt: a bare
-- ReplacingMergeTree keeps an arbitrary row of a collapsing set, so a reprocess would
-- win or lose by merge order. With a version the newest load wins deterministically.
-- As everywhere else in this schema, collapsing happens at merge time only -- a reader
-- that must not see a superseded row uses FINAL or an argMax, and for the derived
-- tables that is cheap because they are small.
--
--
-- ============================================================================
-- The era is a loader-assigned ordinal. The wire Reset Count is never a key.
-- ============================================================================
--
-- era_index is the sequence space a reset opens, and it is assigned by the loader,
-- incrementing per channel instance whenever the reset count changes in receive order
-- (the archive's segment_seq supplies that order). reset_count is the value that came
-- off the wire and is kept as a FACT.
--
-- Using the wire value as a key does not merely risk a wrong answer, it hides
-- findings. Reset Count is a u8: it wraps, and two eras on a long-lived instance then
-- carry the same number. Measured on two eras both carrying reset count 3, the second
-- of which is missing five datagrams: keyed on the wire value the deriver finds ZERO
-- gaps, because the earlier era's rows sit at exactly the missing sequence numbers.
-- Keyed on a monotonic index it finds the gap and its five datagrams. That experiment
-- is reproduced against the tables in this file, and for a system whose whole purpose
-- is attribution, silently losing a finding is worse than raising a false one.
--
-- The index cannot be derived in a query. A window function partitioned on it needs
-- the transition to be inside the window, and a fifteen-minute question generally does
-- not contain the reset it is downstream of. This is a load-time assignment or it is
-- nothing.
--
--
-- ============================================================================
-- drop_scope travels on the rows, because the arithmetic differs by scope
-- ============================================================================
--
-- drop_delta is what WE lost. In socket mode there is one accumulator per port role,
-- so the number is per role and may be subtracted per role. In AF_PACKET mode the ring
-- counts frames dropped BEFORE demultiplexing, so the number belongs to the capture
-- handle and to no single role -- a delta caused by mktdata frames rides on the next
-- refdata datagram that gets through.
--
-- The two scopes take DIFFERENT ARITHMETIC, not the same arithmetic at different
-- grains, which is why drop_scope is a column on the datagram and admitted_scope is a
-- column on the gap rather than something a reader looks up:
--
--   port-role       admitted = the per-instance sum of drop_delta; a gap of n is
--                   'recorder' when n <= admitted, and the residue carries on to the
--                   next verdict.
--   capture-handle  admitted is MEANINGLESS per instance. A gap is 'unverifiable' for
--                   recorder attribution whenever the handle admitted anything at all
--                   over the window, and is NEVER 'publisher'.
--
-- At handle scope the archive can only exonerate itself, and only when its own total
-- is zero -- which is the common case and the interesting one, because a recorder
-- admitting nothing turns every gap into someone else's with evidence. Summing
-- admitted drops per instance instead: measured, a ring dropped forty mktdata
-- datagrams and the delta rode on the next refdata datagram that got through; the sum
-- reported forty unexplained against mktdata and a false publisher finding, while the
-- handle had admitted all forty.
--
--
-- ============================================================================
-- Retention: the base rows expire, the derived rows do not
-- ============================================================================
--
-- fact_edge_recorder_datagram is the expensive table by three or four orders of
-- magnitude -- one row per archived datagram against one row per gap -- and every
-- question the dashboard asks is asked against the DERIVED rows, which are tens of
-- bytes against a 1232-byte datagram. So the split is: expire the base rows, keep the
-- derived ones indefinitely.
--
-- The base table's TTL is 30 days, on recv_ts. It is a placeholder for one number
-- lake does not own: the retention of the raw mktdata objects the rows are replayed
-- from. Below that window a row is a cheaper copy of something still on disk
-- elsewhere; above it, a row survives the evidence it points at, and object_key stops
-- resolving. Realigning it is `ALTER TABLE fact_edge_recorder_datagram MODIFY TTL ...`
-- and needs no migration, and nothing downstream breaks when it moves, because the
-- derived rows are computed AT LOAD TIME and not at query time. That is the whole
-- point of deriving them eagerly: the answers outlive their inputs.
--
-- The three derived tables carry no TTL, deliberately. A year of gap rows for a busy
-- instance is smaller than an hour of its datagrams, and the questions they answer --
-- was this window trustworthy, did the rule set improve, is the redundancy earning its
-- cost -- are the ones asked longest after the fact.
--
-- Partitioning follows the same split. The base table is daily (toYYYYMMDD), because
-- a day is the unit its TTL drops and the unit a fifteen-minute question prunes to.
-- The derived three are monthly (toYYYYMM), matching every other fact table here:
-- they are small and kept forever, so daily parts would mint thousands of tiny
-- partitions for no pruning worth having -- their queries prune on the sort key's
-- instance prefix, not on the partition.
--
--
-- Column order is part of the contract for any loader that issues a bare INSERT with
-- no column list, as dzsvc's WriteBatch does. send_recv_ms is MATERIALIZED and is
-- therefore excluded from the implicit column list (verified: a positional insert of
-- the other columns succeeds with it in place), but it is written last anyway so that
-- a reader counting columns against a Go struct does not have to know that.


-- ---------------------------------------------------------------------------
-- 1. fact_edge_recorder_datagram -- the base fact
-- ---------------------------------------------------------------------------
--
-- One row per archived datagram. Everything else in this file is derivable from it,
-- which is why it is the one row that has to be exactly right.
--
-- The sort key is the channel instance, then the era, then the sequence. Every loss
-- query is a scan along that key inside a day's partitions, which is what makes it
-- cheap and what the level-grain table it replaces could never be.
--
-- (channel instance, sequence number) identifies a datagram independently of who
-- received it, so the same datagram recorded at two sites self-joins on the sort key's
-- own leading columns. That join -- no credentials, no venue involvement -- is what
-- yields per-site loss on one feed, per-site arrival latency against one publisher
-- send timestamp, which site saw it first, and publisher-attributable loss isolated as
-- "absent from EVERY site, with no recorder overflow anywhere".
--
-- recv_ts_kind is carried because a latency computed from an application fallback
-- measures the recorder's own scheduler rather than the network, and a panel that
-- averages that together with a kernel-software timestamp is measuring nothing.
--
-- wire_payload_len is what was SENT and payload_len is what the archive holds, so
-- wire_payload_len > payload_len is a truncation -- a publisher violation the archive
-- keeps rather than a recording artefact.
--
-- source_addr and group_addr are String and not IPv4, which is the one deviation here
-- that is about lake rather than about the recorder. Every IP in this schema is a
-- String (dz_ip, client_ip, public_ip, gossip_ip, source_address), and the consuming
-- handler attributes a series by matching the datagram's source against the ledger's
-- dz_ip. Verified on this ClickHouse: an IPv4 column cannot be compared or joined
-- against a String column at all -- "There is no supertype for types IPv4, String"
-- (NO_COMMON_TYPE), in a JOIN ON and in a WHERE alike. IPv4 would make the one join
-- these rows exist to feed impossible without a cast on every query.
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS fact_edge_recorder_datagram
(
    recv_ts          DateTime64(9),
    ingested_at      DateTime64(3),
    send_ts          DateTime64(9),
    recv_ts_kind     LowCardinality(String),   -- kernel-software | application-fallback

    -- the channel instance, in full and never abbreviated
    source_addr      String,
    channel_id       UInt8,
    dst_port         UInt16,

    feed             LowCardinality(String),   -- the spec name, never a venue
    port_role        LowCardinality(String),   -- mktdata | refdata | snapshot
    group_addr       String,

    sequence_number  UInt64,
    reset_count      UInt8,                    -- the wire value, as sent; never a key
    era_index        UInt32,                   -- the era, assigned by the loader

    payload_len      UInt16,                   -- what the archive holds
    wire_payload_len UInt32,                   -- what was sent; larger means truncated
    drop_delta       UInt32,                   -- what the recorder lost before this one

    site             LowCardinality(String),
    recorder         LowCardinality(String),
    env              LowCardinality(String),
    drop_scope       LowCardinality(String),   -- port-role | capture-handle
    object_key       String,
    object_sha256    String,

    send_recv_ms     Float64 MATERIALIZED
                       (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(send_ts)) / 1e6
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (source_addr, channel_id, dst_port, era_index, sequence_number, site, recorder, env)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY DELETE;
-- +goose StatementEnd


-- ---------------------------------------------------------------------------
-- 2. fact_edge_recorder_segment_coverage -- the manifest, as a table
-- ---------------------------------------------------------------------------
--
-- One row per segment per channel instance, loaded straight from the archive's
-- manifest without opening a single object. It is what makes a coverage question cheap
-- and, more importantly, what makes a MISSING OBJECT visible: a hole in segment_seq
-- for one recorder run is a hole in the archive, and without this table a recorder that
-- was down for an hour is indistinguishable from a feed that was quiet for an hour.
--
-- That is also why segment_seq comes after (site, recorder) in the sort key rather
-- than before: segment_seq is minted per recorder run, so a run's numbering is only
-- contiguous within one recorder, and the hole-detection scan wants the run's rows
-- adjacent.
--
-- roles_joined is the ports the recorder was ASKED to join, and it is the difference
-- between reporting 'na' and reporting 'pass'. A port nobody joined produces no data,
-- and no data looks exactly like a clean feed; reporting a pass over a rule that never
-- ran is the failure mode this column exists to prevent.
--
-- It is Nested rather than the design's Array(Tuple(String, IPv4, UInt16)) for that
-- one query's sake. With flatten_nested on (the default) it stores as three arrays, so
-- "did anyone join mktdata" is has(roles_joined.role, 'mktdata') -- a single array
-- column read. Against Array(Tuple) the same question is
-- has(arrayMap(x -> x.1, roles_joined), 'mktdata'), which reads all three subcolumns
-- and materialises tuples to answer a question about one of them.
--
-- capture_drop_total and interface_drop_total are CUMULATIVE and never reset: a host
-- carries the sum of every burst it ever had. A panel showing the total shows history.
-- Only the delta between two rows of this table says anything about now, and reading
-- either as a rate is the second of the three ways this arithmetic goes wrong.
--
-- No era_index here, matching the design. reset_counts_seen already tells a reader that
-- a segment spanned a reset, and the loader that assigns era_index holds the mapping. If
-- the boundary test in the gap deriver turns out to need the era at segment grain, the
-- honest shape is era_indexes_seen Array(UInt32) and it is an additive ALTER, not a
-- rewrite -- noted here so the next person does not have to rediscover the gap.
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS fact_edge_recorder_segment_coverage
(
    start_ts             DateTime64(9),
    ingested_at          DateTime64(3),
    end_ts               DateTime64(9),

    site                 LowCardinality(String),
    recorder             LowCardinality(String),
    env                  LowCardinality(String),
    feed                 LowCardinality(String),

    source_addr          String,
    channel_id           UInt8,
    dst_port             UInt16,

    segment_seq          UInt64,
    first_seq            UInt64,
    last_seq             UInt64,
    datagram_count       UInt64,
    reset_counts_seen    Array(UInt8),

    capture_drop_total   UInt64,                  -- cumulative, never a rate
    interface_drop_total UInt64,                  -- cumulative, never a rate
    drop_scope           LowCardinality(String),  -- port-role | capture-handle

    roles_joined         Nested(
                             role       LowCardinality(String),
                             group_addr String,
                             port       UInt16
                         ),

    object_key           String,
    object_sha256        String,
    build_version        String,
    build_commit         String,
    config_hash          String
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(start_ts)
ORDER BY (source_addr, channel_id, dst_port, site, recorder, env, segment_seq);
-- +goose StatementEnd


-- ---------------------------------------------------------------------------
-- 3. fact_edge_recorder_sequence_gap -- one row per contiguous run of missing
--    sequence numbers, with a verdict
-- ---------------------------------------------------------------------------
--
-- The row the dashboard actually wants, and the only place attribution is decided.
-- Derived and re-derivable: nothing here cannot be rebuilt from
-- fact_edge_recorder_datagram plus fact_edge_recorder_segment_coverage while the
-- base rows are still inside their TTL.
--
-- verdict is one of five, and the ORDER THEY ARE TESTED IN is the design:
--
--   recorder      covered by our own admitted drops, at a scope where the subtraction
--                 is valid. Costs a counter and an alert on us, never a publisher
--                 finding.
--   upstream      not covered by ours, but interface drops rose over the window. A
--                 switch or link question, not a publisher one.
--   path          absent from this instance, present in a redundant instance on the
--                 same channel and port. The redundancy earned its cost; not feed loss.
--   unverifiable  the gap touches a segment boundary, a missing segment_seq, or a
--                 window with no coverage row. Costs nothing -- and saying so is the
--                 point.
--   publisher     absent from EVERY site, with no recorder overflow anywhere and
--                 coverage intact. The finding, and now a strong one.
--
-- A gap can be PARTLY ours. Five missing with three admitted is neither 'recorder' nor
-- 'publisher', and no single verdict can say so, which is why unexplained_count is a
-- column and why the verdict is decided on that residue rather than on missing_count.
-- A gap fully covered by our own drops has an unexplained count of zero and never
-- leaves our own alerting.
--
-- unverifiable is a first-class verdict and not a failure to compute. A rule set that
-- reports a violation where it merely could not see is a rule set nobody trusts twice.
--
-- reference_seqs is what missing_count is a share of: the sequence numbers this site
-- should have seen over the window. Without it there is no rate, and a bare count of
-- missing datagrams says nothing about a feed's health -- which is precisely the
-- denominator EdgeMulticastRecorderLossSeries.ReferenceSeqs carries today.
--
-- before_ts and after_ts are PLACEMENT, never the measure: they say when to look and
-- missing_count says how much was lost. sent_from_ts and sent_to_ts are when the
-- missing datagrams were actually sent, recovered from a site that DID record them --
-- Nullable because a site has no clock reading for a datagram it never received, so
-- its own bracket is the weaker answer and is all there is until the cross-site join
-- completes. This is also why a row may legitimately be written saying 'unverifiable'
-- and be upgraded to 'publisher' later.
--
-- LOADER CONTRACT, because the partition key depends on it: before_ts must always be a
-- real placement timestamp. When a gap opens a segment and there is no local datagram
-- before it, write the segment's start_ts -- the row is 'unverifiable' anyway. A zero
-- there mints a 197001 partition (verified) that no retention policy ever reaches and
-- that every time-ranged panel silently excludes.
--
-- group_addr is on this row because the consuming report keys on the multicast group
-- and a gap row without it cannot be placed on a line. It is not part of the identity:
-- the group is a property of the instance, not a discriminator within it.
--
-- Note for the deriver, which has bitten this design once: ClickHouse has no
-- correlated subqueries, so "was this sequence number seen anywhere else" cannot be a
-- per-row subselect -- it fails outright. Expand each gap into its missing sequence
-- numbers with arrayJoin(range(missing_from, missing_to + 1)) and join those on
-- equality. That is not a workaround: it attributes per datagram rather than per range,
-- so a gap half of which appears at another site is reported as half, and the join keys
-- are the base table's own sort-key columns.
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS fact_edge_recorder_sequence_gap
(
    before_ts         DateTime64(9),      -- placement, never the measure; never zero
    ingested_at       DateTime64(3),
    after_ts          DateTime64(9),

    site              LowCardinality(String),
    recorder          LowCardinality(String),
    env               LowCardinality(String),
    feed              LowCardinality(String),
    port_role         LowCardinality(String),

    group_addr        String,             -- the consuming report keys on it
    source_addr       String,
    channel_id        UInt8,
    dst_port          UInt16,

    reset_count       UInt8,              -- the wire value at the time; a fact
    era_index         UInt32,             -- the era; a gap never spans two

    missing_from      UInt64,             -- first sequence number absent
    missing_to        UInt64,             -- last sequence number absent
    missing_count     UInt64,
    reference_seqs    UInt64,             -- what the count is a share of

    sent_from_ts      Nullable(DateTime64(9)),  -- from a site that did record them
    sent_to_ts        Nullable(DateTime64(9)),

    admitted_recorder UInt64,             -- our own drops covering this gap
    admitted_scope    LowCardinality(String),   -- port-role | capture-handle
    unexplained_count UInt64,             -- missing_count less what we admit
    interface_drops   UInt64,             -- upstream of the capture point
    seen_elsewhere    UInt8,              -- present at another site
    on_redundant_path UInt8,              -- present in another instance, same channel and port
    verdict           LowCardinality(String),
    object_key        String              -- where the evidence is
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(before_ts)
ORDER BY (source_addr, channel_id, dst_port, era_index, missing_from, site, recorder, env);
-- +goose StatementEnd


-- ---------------------------------------------------------------------------
-- 4. fact_edge_recorder_conformance_finding -- the rule set's verdicts, kept
-- ---------------------------------------------------------------------------
--
-- One verdict per (rule, rule set version, channel instance, window, recording site).
-- verdict is pass | violation | unverifiable | na, and 'na' is the one a silent port
-- gets when segment_coverage.roles_joined never claimed it -- distinct from 'pass',
-- which would be a pass over a rule that never ran.
--
-- run_ts and rule_set_version are load-bearing rather than bookkeeping. A rule added
-- next month runs against last month's traffic, so one window legitimately holds two
-- verdicts from two versions, and a dashboard that cannot say which version produced a
-- verdict cannot show that the rule set improved. rule_set_version is second in the
-- sort key for that reason; run_ts is deliberately NOT in the key, so that re-running
-- one version over one window replaces its verdict instead of accumulating a row per
-- run.
--
-- The grain is one row, singular. A rule that wants to emit several findings for one
-- window must narrow its window rather than widen its row count, because the second
-- finding would collapse onto the first. first_seq/last_seq are the evidence range of
-- the one finding, not a way to carry more than one.
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS fact_edge_recorder_conformance_finding
(
    window_start     DateTime64(9),
    ingested_at      DateTime64(3),
    window_end       DateTime64(9),
    run_ts           DateTime64(9),           -- when the rule set ran; not a key

    rule_id          LowCardinality(String),
    rule_set_version LowCardinality(String),

    site             LowCardinality(String),
    recorder         LowCardinality(String),
    env              LowCardinality(String),
    feed             LowCardinality(String),
    port_role        LowCardinality(String),

    source_addr      String,
    channel_id       UInt8,
    dst_port         UInt16,

    verdict          LowCardinality(String),  -- pass | violation | unverifiable | na
    detail           String,
    object_key       String,
    first_seq        UInt64,                  -- the evidence range
    last_seq         UInt64
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (rule_id, rule_set_version, source_addr, channel_id, dst_port, window_start, site, recorder, env);
-- +goose StatementEnd


-- +goose Down

-- +goose StatementBegin
DROP TABLE IF EXISTS fact_edge_recorder_conformance_finding;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS fact_edge_recorder_sequence_gap;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS fact_edge_recorder_segment_coverage;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS fact_edge_recorder_datagram;
-- +goose StatementEnd
