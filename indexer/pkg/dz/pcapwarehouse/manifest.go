package pcapwarehouse

import (
	"fmt"
	"net"
	"path"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// manifest is the manifest_<ts>.yaml an offload writes into the hour directory it
// uploaded to, listing every pcap it uploaded in that pass.
type manifest struct {
	ManifestVersion int `yaml:"manifest_version"`
	Offload         struct {
		TimestampUTC string `yaml:"timestamp_utc"`
	} `yaml:"offload"`
	Files []struct {
		Filename        string `yaml:"filename"`
		SizeBytes       uint64 `yaml:"size_bytes"`
		MD5             string `yaml:"md5"`
		PacketsCaptured uint64 `yaml:"packets_captured"`
		FirstPacketUTC  string `yaml:"first_packet_utc"`
		LastPacketUTC   string `yaml:"last_packet_utc"`
		MulticastGroup  string `yaml:"multicast_group"`
		MulticastPort   uint16 `yaml:"multicast_port"`
		S3Key           string `yaml:"s3_key"`
	} `yaml:"files"`
}

// Recorder identifies one recorder instance by its key prefix, <host>-<public IP>.
// A host that is rebuilt on a new address starts a new prefix, so the pair — not the
// host — is the unit that has a contiguous history.
type Recorder struct {
	Prefix string // the directory name, e.g. aws-cmh-mn-recorder1-3.151.138.124
	Host   string
	IP     string
}

// ParseRecorder splits a recorder directory name at its last hyphen. Host names carry
// hyphens and dotted-quad addresses do not, so the last one is the separator.
func ParseRecorder(prefix string) (Recorder, error) {
	i := strings.LastIndexByte(prefix, '-')
	if i <= 0 || i == len(prefix)-1 {
		return Recorder{}, fmt.Errorf("recorder prefix %q is not <host>-<ip>", prefix)
	}
	host, ip := prefix[:i], prefix[i+1:]
	if net.ParseIP(ip) == nil {
		return Recorder{}, fmt.Errorf("recorder prefix %q does not end in an IP address", prefix)
	}
	return Recorder{Prefix: prefix, Host: host, IP: ip}, nil
}

// ParseManifest turns one manifest into file rows. hour is the directory the manifest
// was read from; key is its own S3 key.
//
// A file with no packets carries no packet timestamps, so it is stamped with the hour
// it was filed under: it still counts as a file and its bytes, and contributes no
// coverage beyond that instant.
func ParseManifest(data []byte, rec Recorder, hour time.Time, key string) ([]FileRow, error) {
	var m manifest
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest %s: %w", key, err)
	}
	if m.ManifestVersion != 1 {
		return nil, fmt.Errorf("manifest %s: unsupported manifest_version %d", key, m.ManifestVersion)
	}
	offload, err := parseTS(m.Offload.TimestampUTC)
	if err != nil {
		return nil, fmt.Errorf("manifest %s: offload timestamp: %w", key, err)
	}
	if offload.IsZero() {
		offload = hour
	}
	dir := path.Dir(key)

	rows := make([]FileRow, 0, len(m.Files))
	for _, f := range m.Files {
		if f.MulticastGroup == "" {
			return nil, fmt.Errorf("manifest %s: file %s has no multicast_group", key, f.Filename)
		}
		first, err := parseTS(f.FirstPacketUTC)
		if err != nil {
			return nil, fmt.Errorf("manifest %s: file %s first_packet_utc: %w", key, f.Filename, err)
		}
		last, err := parseTS(f.LastPacketUTC)
		if err != nil {
			return nil, fmt.Errorf("manifest %s: file %s last_packet_utc: %w", key, f.Filename, err)
		}
		if first.IsZero() {
			first = hour
		}
		if last.IsZero() || last.Before(first) {
			last = first
		}
		s3Key := f.S3Key
		if s3Key == "" {
			s3Key = path.Join(dir, f.Filename)
		}
		rows = append(rows, FileRow{
			FirstPacketTS:  first,
			Recorder:       rec.Host,
			RecorderIP:     rec.IP,
			HourTS:         hour,
			S3Key:          s3Key,
			MulticastGroup: f.MulticastGroup,
			MulticastPort:  f.MulticastPort,
			SizeBytes:      f.SizeBytes,
			Packets:        f.PacketsCaptured,
			LastPacketTS:   last,
			MD5:            f.MD5,
			ManifestKey:    key,
			OffloadTS:      offload,
		})
	}
	return rows, nil
}

func parseTS(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

// hourDir renders an hour as the warehouse's YYYY/MM/DD/HH directory.
func hourDir(t time.Time) string {
	return t.UTC().Format("2006/01/02/15")
}
