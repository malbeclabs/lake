package handlers

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Export types and functions for testing.

type ExportLinkRollupRow = linkRollupRow
type ExportLinkBucketKey = linkBucketKey
type ExportInterfaceRollupRow = interfaceRollupRow

type ExportInterfaceRollupOpts struct {
	GroupBy    int
	LinkPKs    []string
	DevicePKs  []string
	UserOnly   bool
	ErrorsOnly bool
}

const (
	ExportGroupByLinkSide   = int(groupByLinkSide)
	ExportGroupByDevice     = int(groupByDevice)
	ExportGroupByDeviceIntf = int(groupByDeviceIntf)
)

func ExportParseBucketParams(timeRange string, requestedBuckets int) bucketParams {
	return parseBucketParams(timeRange, requestedBuckets)
}

func ExportQueryLinkRollup(ctx context.Context, db driver.Conn, params bucketParams, linkPKs ...string) (map[linkBucketKey]*linkRollupRow, error) {
	return queryLinkRollup(ctx, db, params, linkPKs...)
}

func ExportQueryInterfaceRollup(ctx context.Context, db driver.Conn, params bucketParams, opts ExportInterfaceRollupOpts) ([]interfaceRollupRow, error) {
	return queryInterfaceRollup(ctx, db, params, interfaceRollupOpts{
		GroupBy:    interfaceGroupBy(opts.GroupBy),
		LinkPKs:    opts.LinkPKs,
		DevicePKs:  opts.DevicePKs,
		UserOnly:   opts.UserOnly,
		ErrorsOnly: opts.ErrorsOnly,
	})
}
