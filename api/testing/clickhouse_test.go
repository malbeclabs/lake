package apitesting

import "testing"

func TestSkipTemplateTable(t *testing.T) {
	tests := []struct {
		name  string
		table string
		skip  bool
	}{
		{"migrated table", "dz_devices", false},
		{"materialized view", "dz_device_interface_ips", false},
		{"goose bookkeeping", "goose_db_version", true},
		{"mv inner table", ".inner_id.5d0a2c83-2f3e-43fd-b81d-40a97a87f610", true},
		{"named mv inner table", ".inner.some_view", true},
		// Transient rebuild target of a REFRESHABLE materialized view: its
		// create_table_query is empty, so cloning it fails with "Empty query".
		{"refresh swap temp table", ".tmp.inner_id.5d0a2c83-2f3e-43fd-b81d-40a97a87f610", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := skipTemplateTable(tt.table); got != tt.skip {
				t.Errorf("skipTemplateTable(%q) = %v, want %v", tt.table, got, tt.skip)
			}
		})
	}
}
