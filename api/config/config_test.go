package config

import "testing"

func TestFeedsDBDefaultAndSetter(t *testing.T) {
	SetFeedsDB("feeds")
	if got := GetFeedsDB(); got != "feeds" {
		t.Fatalf("default GetFeedsDB() = %q, want %q", got, "feeds")
	}
	SetFeedsDB("feeds_qa")
	if got := GetFeedsDB(); got != "feeds_qa" {
		t.Fatalf("after SetFeedsDB, GetFeedsDB() = %q, want %q", got, "feeds_qa")
	}
	SetFeedsDB("feeds") // restore default for other tests
}
