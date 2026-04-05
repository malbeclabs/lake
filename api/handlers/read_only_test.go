package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsReadOnlySQL(t *testing.T) {
	t.Parallel()

	// Queries that should be allowed (read-only)
	allowed := []struct {
		name  string
		query string
	}{
		{"basic select", "SELECT 1"},
		{"select from table", "SELECT * FROM dim_devices_current"},
		{"select with where", "SELECT name FROM dim_devices_current WHERE is_active = 1"},
		{"lowercase select", "select * from foo"},
		{"mixed case", "SeLeCt * FROM foo"},
		{"leading whitespace", "   SELECT 1"},
		{"leading newline", "\n\nSELECT 1"},
		{"leading tab", "\t SELECT 1"},
		{"CTE with", "WITH cte AS (SELECT 1) SELECT * FROM cte"},
		{"nested CTE", "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a JOIN b"},
		{"show tables", "SHOW TABLES"},
		{"show create table", "SHOW CREATE TABLE dim_devices_current"},
		{"show databases", "SHOW DATABASES"},
		{"describe table", "DESCRIBE dim_devices_current"},
		{"desc table", "DESC dim_devices_current"},
		{"explain select", "EXPLAIN SELECT * FROM dim_devices_current"},
		{"explain pipeline", "EXPLAIN PIPELINE SELECT * FROM dim_devices_current"},
		{"exists", "EXISTS dim_devices_current"},
		{"select with subquery", "SELECT * FROM (SELECT 1) AS sub"},
		{"select with union", "SELECT 1 UNION ALL SELECT 2"},
		{"select with join", "SELECT a.* FROM foo a JOIN bar b ON a.id = b.id"},
		{"select with group by", "SELECT count(*) FROM foo GROUP BY bar"},
		{"select with order by", "SELECT * FROM foo ORDER BY id DESC"},
		{"select with limit", "SELECT * FROM foo LIMIT 10"},
		{"select with format", "SELECT * FROM foo FORMAT JSON"},
		{"select with settings", "SELECT * FROM foo SETTINGS max_threads=1"},
	}

	for _, tc := range allowed {
		t.Run("allow/"+tc.name, func(t *testing.T) {
			t.Parallel()
			assert.True(t, isReadOnlySQL(tc.query), "expected query to be allowed: %s", tc.query)
		})
	}

	// Queries that should be blocked (write operations)
	blocked := []struct {
		name  string
		query string
	}{
		{"insert", "INSERT INTO foo VALUES (1)"},
		{"insert select", "INSERT INTO foo SELECT * FROM bar"},
		{"alter table delete", "ALTER TABLE foo DELETE WHERE id = 1"},
		{"alter table update", "ALTER TABLE foo UPDATE name = 'x' WHERE id = 1"},
		{"alter table add column", "ALTER TABLE foo ADD COLUMN bar String"},
		{"drop table", "DROP TABLE foo"},
		{"drop database", "DROP DATABASE test"},
		{"truncate", "TRUNCATE TABLE foo"},
		{"create table", "CREATE TABLE foo (id Int32) ENGINE = Memory"},
		{"create view", "CREATE VIEW foo AS SELECT 1"},
		{"create database", "CREATE DATABASE test"},
		{"rename table", "RENAME TABLE foo TO bar"},
		{"optimize table", "OPTIMIZE TABLE foo"},
		{"system command", "SYSTEM FLUSH LOGS"},
		{"grant", "GRANT SELECT ON foo TO user1"},
		{"revoke", "REVOKE SELECT ON foo FROM user1"},
		{"lowercase insert", "insert into foo values (1)"},
		{"mixed case drop", "DrOp TABLE foo"},
		{"leading whitespace insert", "   INSERT INTO foo VALUES (1)"},
		{"comment bypass block", "/* harmless */ INSERT INTO foo VALUES (1)"},
		{"comment bypass line", "-- just a comment\nINSERT INTO foo VALUES (1)"},
		{"multi comment bypass", "/* a */ /* b */ DROP TABLE foo"},
		{"mixed comment bypass", "-- line\n/* block */ ALTER TABLE foo DELETE WHERE 1"},
		{"gibberish", "SELECTERINO * FROMONO nonexistent"},
		{"empty", ""},
		{"whitespace only", "   "},
		{"attach", "ATTACH TABLE foo"},
		{"detach", "DETACH TABLE foo"},
		{"exchange", "EXCHANGE TABLES foo AND bar"},
	}

	for _, tc := range blocked {
		t.Run("block/"+tc.name, func(t *testing.T) {
			t.Parallel()
			assert.False(t, isReadOnlySQL(tc.query), "expected query to be blocked: %s", tc.query)
		})
	}
}

func TestIsReadOnlyCypher(t *testing.T) {
	t.Parallel()

	// Queries that should be allowed (read-only)
	allowed := []struct {
		name  string
		query string
	}{
		{"basic match", "MATCH (n) RETURN n"},
		{"match with where", "MATCH (n:Device) WHERE n.name = 'foo' RETURN n"},
		{"match with limit", "MATCH (n) RETURN n LIMIT 10"},
		{"match with order", "MATCH (n) RETURN n ORDER BY n.name"},
		{"match with count", "MATCH (n) RETURN count(n)"},
		{"match with relationship", "MATCH (a)-[r]->(b) RETURN a, r, b"},
		{"match with path", "MATCH p = (a)-[*]->(b) RETURN p"},
		{"call procedure", "CALL db.labels()"},
		{"optional match", "OPTIONAL MATCH (n) RETURN n"},
		{"unwind", "UNWIND [1, 2, 3] AS x RETURN x"},
		{"lowercase match", "match (n) return n"},
		{"with clause", "MATCH (n) WITH n.name AS name RETURN name"},
		// Ensure words containing blocked substrings are not false positives
		{"column named dataset", "MATCH (n) RETURN n.dataset"},
		{"column named offset", "MATCH (n) RETURN n.offset"},
		{"column named merger", "MATCH (n) RETURN n.merger"},
	}

	for _, tc := range allowed {
		t.Run("allow/"+tc.name, func(t *testing.T) {
			t.Parallel()
			assert.True(t, isReadOnlyCypher(tc.query), "expected query to be allowed: %s", tc.query)
		})
	}

	// Queries that should be blocked (write operations)
	blocked := []struct {
		name  string
		query string
	}{
		{"create node", "CREATE (n:Foo {name: 'bar'})"},
		{"merge node", "MERGE (n:Foo {name: 'bar'})"},
		{"delete node", "MATCH (n) DELETE n"},
		{"detach delete", "MATCH (n) DETACH DELETE n"},
		{"set property", "MATCH (n) SET n.name = 'bar'"},
		{"remove property", "MATCH (n) REMOVE n.name"},
		{"match then create", "MATCH (a) CREATE (b)"},
		{"match then merge", "MATCH (a) MERGE (a)-[:REL]->(b)"},
		{"lowercase create", "create (n:Foo)"},
		{"mixed case delete", "MATCH (n) DeLeTe n"},
	}

	for _, tc := range blocked {
		t.Run("block/"+tc.name, func(t *testing.T) {
			t.Parallel()
			assert.False(t, isReadOnlyCypher(tc.query), "expected query to be blocked: %s", tc.query)
		})
	}
}
