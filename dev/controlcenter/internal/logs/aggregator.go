package logs

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// LogEntry represents a single log line with metadata
type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Service   string    `json:"service"`
	Stream    string    `json:"stream"` // "stdout" or "stderr"
	Level     LogLevel  `json:"level"`
	Line      string    `json:"line"`
}

// HistogramBucket represents a time bucket with counts per log level
type HistogramBucket struct {
	Time    time.Time `json:"time"`
	Error   int       `json:"error"`
	Warn    int       `json:"warn"`
	Info    int       `json:"info"`
	Debug   int       `json:"debug"`
	Unknown int       `json:"unknown"`
}

// RingBuffer is a fixed-size circular buffer for log entries
type RingBuffer struct {
	entries []LogEntry
	size    int
	pos     int
	full    bool
	mu      sync.RWMutex
}

// NewRingBuffer creates a new ring buffer with the specified size
func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{
		entries: make([]LogEntry, size),
		size:    size,
	}
}

// Append adds a log entry to the ring buffer
func (rb *RingBuffer) Append(entry LogEntry) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.entries[rb.pos] = entry
	rb.pos = (rb.pos + 1) % rb.size
	if rb.pos == 0 {
		rb.full = true
	}
}

// GetAll returns all log entries in chronological order
func (rb *RingBuffer) GetAll() []LogEntry {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if !rb.full {
		// Buffer not full yet, return from start to pos
		result := make([]LogEntry, rb.pos)
		copy(result, rb.entries[:rb.pos])
		return result
	}

	// Buffer is full, return in circular order
	result := make([]LogEntry, rb.size)
	copy(result, rb.entries[rb.pos:])
	copy(result[rb.size-rb.pos:], rb.entries[:rb.pos])
	return result
}

// GetRecent returns the last n log entries
func (rb *RingBuffer) GetRecent(n int) []LogEntry {
	all := rb.GetAll()
	if len(all) <= n {
		return all
	}
	return all[len(all)-n:]
}

// Aggregator collects and distributes logs from multiple services
type Aggregator struct {
	buffers     map[string]*RingBuffer
	subscribers []chan LogEntry
	logFiles    map[string]*os.File
	parser      *LogParser
	dataDir     string
	logger      *slog.Logger
	mu          sync.RWMutex
}

// NewAggregator creates a new log aggregator
func NewAggregator(dataDir string, bufferSize int, logger *slog.Logger) (*Aggregator, error) {
	logsDir := filepath.Join(dataDir, "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create logs directory: %w", err)
	}

	return &Aggregator{
		buffers:  make(map[string]*RingBuffer),
		logFiles: make(map[string]*os.File),
		parser:   NewLogParser(),
		dataDir:  logsDir,
		logger:   logger,
	}, nil
}

// Append adds a log entry to the aggregator
func (a *Aggregator) Append(entry LogEntry) {
	// Strip ANSI escape codes from log lines
	entry.Line = StripANSI(entry.Line)

	// Parse log level if not already set
	if entry.Level == "" || entry.Level == LogLevelUnknown {
		entry.Level = a.parser.ParseLevel(entry.Line)
	}

	a.mu.Lock()

	// Create buffer for service if it doesn't exist
	if _, exists := a.buffers[entry.Service]; !exists {
		a.buffers[entry.Service] = NewRingBuffer(10000)
	}

	// Append to ring buffer
	a.buffers[entry.Service].Append(entry)

	// Write to log file
	if err := a.writeToFile(entry); err != nil {
		a.logger.Error("failed to write to log file", "error", err, "service", entry.Service)
	}

	// Broadcast to subscribers
	subscribers := make([]chan LogEntry, len(a.subscribers))
	copy(subscribers, a.subscribers)
	a.mu.Unlock()

	for _, ch := range subscribers {
		select {
		case ch <- entry:
		default:
			// Don't block if subscriber is slow
		}
	}
}

const (
	maxLogFileSize  = 10 * 1024 * 1024 // 10MB
	maxRotatedFiles = 5
)

// rotateLogFile renames SERVICE.log → SERVICE.1.log, shifts older files up, keeps max 5.
// Must be called with a.mu held (write lock).
func (a *Aggregator) rotateLogFile(service string) error {
	// Close current file
	if f, ok := a.logFiles[service]; ok {
		f.Close()
		delete(a.logFiles, service)
	}

	logPath := filepath.Join(a.dataDir, service+".log")

	// Shift existing rotated files: SERVICE.4.log → SERVICE.5.log, etc.
	for i := maxRotatedFiles - 1; i >= 1; i-- {
		src := filepath.Join(a.dataDir, fmt.Sprintf("%s.%d.log", service, i))
		dst := filepath.Join(a.dataDir, fmt.Sprintf("%s.%d.log", service, i+1))
		if _, err := os.Stat(src); err == nil {
			os.Rename(src, dst) //nolint:errcheck
		}
	}

	// Rename current log to SERVICE.1.log
	if _, err := os.Stat(logPath); err == nil {
		if err := os.Rename(logPath, filepath.Join(a.dataDir, service+".1.log")); err != nil {
			return err
		}
	}

	return nil
}

// writeToFile writes a log entry to the service's log file
func (a *Aggregator) writeToFile(entry LogEntry) error {
	file, exists := a.logFiles[entry.Service]
	if !exists {
		logPath := filepath.Join(a.dataDir, entry.Service+".log")
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		a.logFiles[entry.Service] = f
		file = f
	}

	// Check file size and rotate if needed
	if info, err := file.Stat(); err == nil && info.Size() >= maxLogFileSize {
		if err := a.rotateLogFile(entry.Service); err != nil {
			a.logger.Error("failed to rotate log file", "error", err, "service", entry.Service)
		}
		// Re-open fresh file
		logPath := filepath.Join(a.dataDir, entry.Service+".log")
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		a.logFiles[entry.Service] = f
		file = f
	}

	line := fmt.Sprintf("[%s] [%s] [%s] %s\n",
		entry.Timestamp.Format(time.RFC3339),
		entry.Service,
		entry.Level,
		entry.Line,
	)
	_, err := file.WriteString(line)
	return err
}

// Subscribe creates a new subscription channel for log entries
func (a *Aggregator) Subscribe() chan LogEntry {
	a.mu.Lock()
	defer a.mu.Unlock()

	ch := make(chan LogEntry, 100)
	a.subscribers = append(a.subscribers, ch)
	return ch
}

// Unsubscribe removes a subscription channel
func (a *Aggregator) Unsubscribe(ch chan LogEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for i, sub := range a.subscribers {
		if sub == ch {
			a.subscribers = append(a.subscribers[:i], a.subscribers[i+1:]...)
			break
		}
	}
}

// GetRecent returns recent log entries for a service
func (a *Aggregator) GetRecent(service string, limit int) []LogEntry {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if service == "all" || service == "" {
		// Combine logs from all services
		var all []LogEntry
		for _, buffer := range a.buffers {
			all = append(all, buffer.GetAll()...)
		}
		sort.Slice(all, func(i, j int) bool {
			return all[i].Timestamp.Before(all[j].Timestamp)
		})
		if len(all) <= limit {
			return all
		}
		return all[len(all)-limit:]
	}

	buffer, exists := a.buffers[service]
	if !exists {
		return []LogEntry{}
	}

	return buffer.GetRecent(limit)
}

// GetFiltered returns log entries filtered by service, level, and time range
func (a *Aggregator) GetFiltered(service, level string, from, to time.Time, limit int) []LogEntry {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var candidates []LogEntry

	if service == "all" || service == "" {
		for _, buffer := range a.buffers {
			candidates = append(candidates, buffer.GetAll()...)
		}
	} else {
		buffer, exists := a.buffers[service]
		if !exists {
			return []LogEntry{}
		}
		candidates = buffer.GetAll()
	}

	// Sort by timestamp
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Timestamp.Before(candidates[j].Timestamp)
	})

	// Apply filters
	result := make([]LogEntry, 0, len(candidates))
	for _, entry := range candidates {
		if !from.IsZero() && entry.Timestamp.Before(from) {
			continue
		}
		if !to.IsZero() && entry.Timestamp.After(to) {
			continue
		}
		if level != "" && level != "all" && string(entry.Level) != level {
			continue
		}
		result = append(result, entry)
	}

	if limit > 0 && len(result) > limit {
		return result[len(result)-limit:]
	}
	return result
}

// GetHistogram returns log counts per time bucket for the given filters
func (a *Aggregator) GetHistogram(service, level string, from, to time.Time, bucketDuration time.Duration) []HistogramBucket {
	entries := a.GetFiltered(service, level, from, to, 0)

	if len(entries) == 0 || bucketDuration <= 0 {
		return []HistogramBucket{}
	}

	// Use provided from/to, or derive from entries
	start := from
	if start.IsZero() && len(entries) > 0 {
		start = entries[0].Timestamp
	}
	end := to
	if end.IsZero() && len(entries) > 0 {
		end = entries[len(entries)-1].Timestamp
	}

	// Align start to bucket boundary
	start = start.Truncate(bucketDuration)

	// Build bucket map
	bucketMap := make(map[int64]*HistogramBucket)
	for i := start; !i.After(end); i = i.Add(bucketDuration) {
		key := i.UnixNano()
		bucketMap[key] = &HistogramBucket{Time: i}
	}

	for _, entry := range entries {
		bucketTime := entry.Timestamp.Truncate(bucketDuration)
		key := bucketTime.UnixNano()
		b, ok := bucketMap[key]
		if !ok {
			b = &HistogramBucket{Time: bucketTime}
			bucketMap[key] = b
		}
		switch entry.Level {
		case LogLevelError:
			b.Error++
		case LogLevelWarn:
			b.Warn++
		case LogLevelInfo:
			b.Info++
		case LogLevelDebug, LogLevelTrace:
			b.Debug++
		default:
			b.Unknown++
		}
	}

	// Sort buckets by time
	keys := make([]int64, 0, len(bucketMap))
	for k := range bucketMap {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	result := make([]HistogramBucket, 0, len(keys))
	for _, k := range keys {
		result = append(result, *bucketMap[k])
	}
	return result
}

// Close closes all log files
func (a *Aggregator) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, file := range a.logFiles {
		if err := file.Close(); err != nil {
			a.logger.Error("failed to close log file", "error", err)
		}
	}
	return nil
}

// GetServices returns a list of all services that have logs
func (a *Aggregator) GetServices() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	services := make([]string, 0, len(a.buffers))
	for service := range a.buffers {
		services = append(services, service)
	}
	return services
}

// MarshalJSON implements json.Marshaler for LogEntry
func (l LogEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Timestamp string   `json:"timestamp"`
		Service   string   `json:"service"`
		Stream    string   `json:"stream"`
		Level     LogLevel `json:"level"`
		Line      string   `json:"line"`
	}{
		Timestamp: l.Timestamp.Format(time.RFC3339Nano),
		Service:   l.Service,
		Stream:    l.Stream,
		Level:     l.Level,
		Line:      l.Line,
	})
}
