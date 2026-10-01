package pcapwarehouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A manifest as the recorder writes it, trimmed to two files.
const sampleManifest = `manifest_version: 1
recorder:
    hostname: aws-cmh-mn-recorder1
    ip_address: 3.151.138.124
    pid: 720
    doublezero_account_id: FXnUe6hauuAuLDSYA47oHT6NdG9s4p1pv6KNWEc6ATh6
offload:
    timestamp_utc: "2026-09-30T10:00:25Z"
    hour_directory: 2026/09/30/10
    duration_seconds: 11.438145441
files:
    - filename: capture_233.84.178.15_000001.pcap
      size_bytes: 52430008
      md5: 0cd93c448644f1893857009e0074e53f
      packets_captured: 83107
      first_packet_utc: "2026-09-30T10:00:00.00038Z"
      last_packet_utc: "2026-09-30T10:00:09.163602Z"
      multicast_group: 233.84.178.15
      multicast_port: 0
      s3_key: mainnet-beta/aws-cmh-mn-recorder1-3.151.138.124/2026/09/30/10/capture_233.84.178.15_000001.pcap
    - filename: capture_233.84.178.3_000001.pcap
      size_bytes: 24
      md5: d41d8cd98f00b204e9800998ecf8427e
      packets_captured: 0
      multicast_group: 233.84.178.3
      multicast_port: 0
totals:
    file_count: 2
    total_bytes: 52430032
    total_packets: 83107
`

func TestParseManifest(t *testing.T) {
	rec, err := ParseRecorder("aws-cmh-mn-recorder1-3.151.138.124")
	require.NoError(t, err)
	hour := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	key := "mainnet-beta/aws-cmh-mn-recorder1-3.151.138.124/2026/09/30/10/manifest_20260930T100025Z.yaml"

	rows, rejected, err := ParseManifest([]byte(sampleManifest), rec, hour, key)
	require.NoError(t, err)
	require.Empty(t, rejected)
	require.Len(t, rows, 2)

	r := rows[0]
	require.Equal(t, "aws-cmh-mn-recorder1", r.Recorder)
	require.Equal(t, "3.151.138.124", r.RecorderIP)
	require.Equal(t, "233.84.178.15", r.MulticastGroup)
	require.Equal(t, uint64(52430008), r.SizeBytes)
	require.Equal(t, uint64(83107), r.Packets)
	require.Equal(t, time.Date(2026, 9, 30, 10, 0, 0, 380_000, time.UTC), r.FirstPacketTS)
	require.Equal(t, time.Date(2026, 9, 30, 10, 0, 9, 163_602_000, time.UTC), r.LastPacketTS)
	require.Equal(t, time.Date(2026, 9, 30, 10, 0, 25, 0, time.UTC), r.OffloadTS)
	require.Equal(t, key, r.ManifestKey)
	require.Equal(t, hour, r.HourTS)

	// No packets: no timestamps to read, so the file sits at its hour, and with no
	// s3_key it is resolved beside the manifest.
	empty := rows[1]
	require.Equal(t, hour, empty.FirstPacketTS)
	require.Equal(t, hour, empty.LastPacketTS)
	require.Equal(t, "mainnet-beta/aws-cmh-mn-recorder1-3.151.138.124/2026/09/30/10/capture_233.84.178.3_000001.pcap", empty.S3Key)
}

func TestParseManifest_RejectsUnknownVersion(t *testing.T) {
	rec, _ := ParseRecorder("chi-mn-recorder1-208.78.39.181")
	_, _, err := ParseManifest([]byte("manifest_version: 2\nfiles: []\n"), rec, time.Now(), "k")
	require.EqualError(t, err, "manifest k: unsupported manifest_version 2")
}

func TestParseManifest_RejectsFileWithoutAPacketRange(t *testing.T) {
	rec, _ := ParseRecorder("chi-mn-recorder1-208.78.39.181")
	hour := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	const prefix = "manifest k: file capture_233.84.178.15_000001.pcap captured 10 packets with an invalid packet range "
	for name, c := range map[string]struct{ times, want string }{
		"missing first": {`last_packet_utc: "2026-09-30T10:00:09Z"`, prefix + `("" to "2026-09-30T10:00:09Z")`},
		"missing last":  {`first_packet_utc: "2026-09-30T10:00:00Z"`, prefix + `("2026-09-30T10:00:00Z" to "")`},
		"reversed": {
			"first_packet_utc: \"2026-09-30T10:00:09Z\"\n      last_packet_utc: \"2026-09-30T10:00:00Z\"",
			prefix + `("2026-09-30T10:00:09Z" to "2026-09-30T10:00:00Z")`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			data := "manifest_version: 1\nfiles:\n    - filename: capture_233.84.178.15_000001.pcap\n      packets_captured: 10\n      multicast_group: 233.84.178.15\n      " + c.times + "\n"
			rows, rejected, err := ParseManifest([]byte(data), rec, hour, "k")
			require.NoError(t, err, "a bad file entry rejects the file, not the manifest")
			require.Empty(t, rows)
			require.Len(t, rejected, 1)
			require.EqualError(t, rejected[0], c.want)
		})
	}
}

func TestParseRecorder(t *testing.T) {
	rec, err := ParseRecorder("aws-tyo-mn-recorder1-54.168.241.102")
	require.NoError(t, err)
	require.Equal(t, Recorder{Prefix: "aws-tyo-mn-recorder1-54.168.241.102", Host: "aws-tyo-mn-recorder1", IP: "54.168.241.102"}, rec)

	for bad, want := range map[string]string{
		"":             `recorder prefix "" is not <host>-<ip>`,
		"recorder":     `recorder prefix "recorder" is not <host>-<ip>`,
		"host-":        `recorder prefix "host-" is not <host>-<ip>`,
		"-1.2.3.4":     `recorder prefix "-1.2.3.4" is not <host>-<ip>`,
		"host-notanip": `recorder prefix "host-notanip" does not end in an IP address`,
	} {
		_, err := ParseRecorder(bad)
		require.EqualError(t, err, want, bad)
	}
}

// Every file's packets fall inside the hour directory it was filed under (checked on
// the real bucket), so a range outside it is a bad clock and the file is rejected.
func TestParseManifest_RejectsPacketsOutsideTheirHour(t *testing.T) {
	rec, _ := ParseRecorder("chi-mn-recorder1-208.78.39.181")
	hour := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	file := func(first, last string) string {
		return "manifest_version: 1\nfiles:\n    - filename: f.pcap\n      packets_captured: 10\n      multicast_group: 233.84.178.15\n" +
			"      first_packet_utc: \"" + first + "\"\n      last_packet_utc: \"" + last + "\"\n"
	}

	for name, c := range map[string]struct{ first, last, want string }{
		"far future": {"2026-09-30T10:00:00Z", "2031-01-01T00:00:00Z",
			"manifest k: file f.pcap packet range 2026-09-30T10:00:00Z to 2031-01-01T00:00:00Z lies outside its hour directory 2026/09/30/10"},
		"epoch": {"1970-01-01T00:00:00Z", "2026-09-30T10:00:05Z",
			"manifest k: file f.pcap packet range 1970-01-01T00:00:00Z to 2026-09-30T10:00:05Z lies outside its hour directory 2026/09/30/10"},
		"next hour": {"2026-09-30T11:10:00Z", "2026-09-30T11:20:00Z",
			"manifest k: file f.pcap packet range 2026-09-30T11:10:00Z to 2026-09-30T11:20:00Z lies outside its hour directory 2026/09/30/10"},
	} {
		t.Run(name, func(t *testing.T) {
			rows, rejected, err := ParseManifest([]byte(file(c.first, c.last)), rec, hour, "k")
			require.NoError(t, err)
			require.Empty(t, rows)
			require.Len(t, rejected, 1)
			require.EqualError(t, rejected[0], c.want)
		})
	}

	// A few minutes of skew at the edges is tolerated.
	rows, rejected, err := ParseManifest([]byte(file("2026-09-30T09:58:00Z", "2026-09-30T11:03:00Z")), rec, hour, "k")
	require.NoError(t, err)
	require.Empty(t, rejected)
	require.Len(t, rows, 1)
}

// The case seen on aws-tyo-mn-recorder1: one file entry with an impossible last
// packet among good ones. Only that file is rejected; the rest of the manifest stays.
func TestParseManifest_OneBadFileKeepsTheRest(t *testing.T) {
	rec, _ := ParseRecorder("aws-tyo-mn-recorder1-13.114.28.108")
	hour := time.Date(2026, 9, 15, 15, 0, 0, 0, time.UTC)
	data := `manifest_version: 1
offload:
    timestamp_utc: "2026-09-15T18:25:54Z"
files:
    - filename: capture_233.84.178.23_000052.pcap
      packets_captured: 324805
      first_packet_utc: "2026-09-15T15:33:11.449085Z"
      last_packet_utc: "1983-03-16T07:25:38.216201704Z"
      multicast_group: 233.84.178.23
    - filename: capture_233.84.178.23_000053.pcap
      packets_captured: 310000
      first_packet_utc: "2026-09-15T15:34:00Z"
      last_packet_utc: "2026-09-15T15:35:00Z"
      multicast_group: 233.84.178.23
`
	rows, rejected, err := ParseManifest([]byte(data), rec, hour, "k")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "233.84.178.23", rows[0].MulticastGroup)
	require.Len(t, rejected, 1)
	require.EqualError(t, rejected[0], `manifest k: file capture_233.84.178.23_000052.pcap captured 324805 packets with an invalid packet range ("2026-09-15T15:33:11.449085Z" to "1983-03-16T07:25:38.216201704Z")`)
}
