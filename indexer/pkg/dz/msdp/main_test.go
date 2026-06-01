package msdp

import (
	"context"
	"os"
	"testing"

	clickhousetesting "github.com/malbeclabs/lake/indexer/pkg/clickhouse/testing"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
)

// sharedDB is a single ClickHouse testcontainer reused across every test
// in this package. Per-test isolation is achieved by laketesting.NewClient
// creating a fresh randomly-named database and running migrations into it.
var sharedDB *clickhousetesting.DB

func TestMain(m *testing.M) {
	log := laketesting.NewLogger()
	var err error
	sharedDB, err = clickhousetesting.NewDB(context.Background(), log, nil)
	if err != nil {
		log.Error("msdp: failed to start ClickHouse testcontainer", "error", err)
		os.Exit(1)
	}
	code := m.Run()
	sharedDB.Close()
	os.Exit(code)
}
