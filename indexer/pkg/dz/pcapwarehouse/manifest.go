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
	Files []manifestFile `yaml:"files"`
}

type manifestFile struct {
	Filename        string `yaml:"filename"`
	SizeBytes       uint64 `yaml:"size_bytes"`
	MD5             string `yaml:"md5"`
	PacketsCaptured uint64 `yaml:"packets_captured"`
	FirstPacketUTC  string `yaml:"first_packet_utc"`
	LastPacketUTC   string `yaml:"last_packet_utc"`
	MulticastGroup  string `yaml:"multicast_group"`
	MulticastPort   uint16 `yaml:"multicast_port"`
	S3Key           string `yaml:"s3_key"`
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
// Two kinds of fault, kept apart because they mean different things. A fault in the
// manifest itself — unreadable YAML, an unknown manifest_version, an unparseable
// offload time — refuses it whole and is returned as err: nothing in it can be
// trusted, and a run of them is the shape of a format change that stops ingest, which
// is what the activity escalates on. A fault in one file entry rejects that file only
// and is returned in rejected, while the manifest's other files are kept: the recorder
// does write the odd impossible timestamp (aws-tyo-mn-recorder1 has written
// last_packet_utc 1983-03-16 for a file of 324,805 packets), and refusing the whole
// manifest for it threw away ~138 good files each time and painted the hour as an
// outage.
//
// A file with no packets carries no packet timestamps, so it is stamped with the hour
// it was filed under: it still counts as a file and its bytes, and contributes no
// coverage beyond that instant. A file that captured packets must carry a valid packet
// range inside its hour directory.
func ParseManifest(data []byte, rec Recorder, hour time.Time, key string) (rows []FileRow, rejected []error, err error) {
	var m manifest
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, nil, fmt.Errorf("parse manifest %s: %w", key, err)
	}
	if m.ManifestVersion != 1 {
		return nil, nil, fmt.Errorf("manifest %s: unsupported manifest_version %d", key, m.ManifestVersion)
	}
	offload, err := parseTS(m.Offload.TimestampUTC)
	if err != nil {
		return nil, nil, fmt.Errorf("manifest %s: offload timestamp: %w", key, err)
	}
	if offload.IsZero() {
		offload = hour
	}
	dir := path.Dir(key)

	rows = make([]FileRow, 0, len(m.Files))
	for _, f := range m.Files {
		row, ferr := parseFile(f, rec, hour, key, dir, offload)
		if ferr != nil {
			rejected = append(rejected, ferr)
			continue
		}
		rows = append(rows, row)
	}
	return rows, rejected, nil
}

func parseFile(f manifestFile, rec Recorder, hour time.Time, key, dir string, offload time.Time) (FileRow, error) {
	if f.MulticastGroup == "" {
		return FileRow{}, fmt.Errorf("manifest %s: file %s has no multicast_group", key, f.Filename)
	}
	first, err := parseTS(f.FirstPacketUTC)
	if err != nil {
		return FileRow{}, fmt.Errorf("manifest %s: file %s first_packet_utc: %w", key, f.Filename, err)
	}
	last, err := parseTS(f.LastPacketUTC)
	if err != nil {
		return FileRow{}, fmt.Errorf("manifest %s: file %s last_packet_utc: %w", key, f.Filename, err)
	}
	if f.PacketsCaptured == 0 {
		// Nothing captured, so nothing to time: the file sits at its hour.
		if first.IsZero() {
			first = hour
		}
		if last.IsZero() || last.Before(first) {
			last = first
		}
	} else if first.IsZero() || last.IsZero() || last.Before(first) {
		// A file that captured packets without a valid packet range would enter the
		// gap arithmetic as an instant it did not cover, so it is rejected rather than
		// repaired.
		return FileRow{}, fmt.Errorf("manifest %s: file %s captured %d packets with an invalid packet range (%q to %q)",
			key, f.Filename, f.PacketsCaptured, f.FirstPacketUTC, f.LastPacketUTC)
	} else if first.Before(hour.Add(-hourSlack)) || !last.Before(hour.Add(time.Hour+hourSlack)) {
		// The recorder rotates its files at the hour and files each under the hour it
		// captured: of 1.2M files read before this check existed, every packet range lay
		// inside its hour directory. One outside it is a bad clock, and it cannot be repaired
		// later — the row is never re-read — while a far-future last packet would hide
		// every later gap behind the gap query's running max and keep the recorder
		// reading as live, and a 1970 first packet would stretch the all-history window
		// to decades of buckets.
		return FileRow{}, fmt.Errorf("manifest %s: file %s packet range %s to %s lies outside its hour directory %s",
			key, f.Filename, f.FirstPacketUTC, f.LastPacketUTC, hourDir(hour))
	}
	s3Key := f.S3Key
	if s3Key == "" {
		s3Key = path.Join(dir, f.Filename)
	}
	return FileRow{
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
	}, nil
}

// hourSlack tolerates a little clock skew at the hour's edges when checking that a
// file's packets fall inside the hour it was filed under.
const hourSlack = 5 * time.Minute

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
