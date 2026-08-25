package dzingest

import (
	"context"
	"os"
	"testing"

	clickhousetesting "github.com/malbeclabs/lake/indexer/pkg/clickhouse/testing"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
)

var sharedDB *clickhousetesting.DB

func TestMain(m *testing.M) {
	log := laketesting.NewLogger()
	var err error
	sharedDB, err = clickhousetesting.NewDB(context.Background(), log, nil)
	if err != nil {
		log.Error("failed to create shared DB", "error", err)
		os.Exit(1)
	}
	code := m.Run()
	sharedDB.Close()
	os.Exit(code)
}
