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

	rows, err := ParseManifest([]byte(sampleManifest), rec, hour, key)
	require.NoError(t, err)
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
	_, err := ParseManifest([]byte("manifest_version: 2\nfiles: []\n"), rec, time.Now(), "k")
	require.EqualError(t, err, "manifest k: unsupported manifest_version 2")
}

func TestParseManifest_RejectsCapturedFileWithoutAPacketRange(t *testing.T) {
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
			rows, err := ParseManifest([]byte(data), rec, hour, "k")
			require.Nil(t, rows)
			require.EqualError(t, err, c.want)
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
