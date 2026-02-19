package logs

import (
	"regexp"
)

// LogLevel represents the severity level of a log entry
type LogLevel string

const (
	LogLevelError   LogLevel = "ERROR"
	LogLevelWarn    LogLevel = "WARN"
	LogLevelInfo    LogLevel = "INFO"
	LogLevelDebug   LogLevel = "DEBUG"
	LogLevelTrace   LogLevel = "TRACE"
	LogLevelUnknown LogLevel = "UNKNOWN" // retained for compatibility; never assigned by parser
)

var ansiEscapePattern = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)

// StripANSI removes ANSI escape codes from a string
func StripANSI(s string) string {
	return ansiEscapePattern.ReplaceAllString(s, "")
}

// LogParser parses log levels from log lines
type LogParser struct {
	// errorOverridePattern bumps any line to ERROR regardless of its stated level
	errorOverridePattern *regexp.Regexp
	// shortCodePattern matches 3-char level codes used by zerolog-style loggers
	shortCodePattern *regexp.Regexp
	// slogPattern matches key=value structured log level fields
	slogPattern *regexp.Regexp
	// fallbackPatterns are checked in severity order when no structured marker is found
	fallbackPatterns []struct {
		level LogLevel
		regex *regexp.Regexp
	}
}

// NewLogParser creates a new log parser
func NewLogParser() *LogParser {
	return &LogParser{
		// Words that always indicate an error — override whatever level was detected
		errorOverridePattern: regexp.MustCompile(
			`(?i)\b(failed|failure|fatal|panic|exception|crash|critical|corrupt|goroutine \d+ \[)\b`,
		),

		// 3-char codes used by zerolog / similar loggers
		shortCodePattern: regexp.MustCompile(`\b(ERR|WRN|INF|DBG|TRC)\b`),

		// slog key=value format: level=INFO etc.
		slogPattern: regexp.MustCompile(`level=(ERROR|WARN|INFO|DEBUG|TRACE)`),

		fallbackPatterns: []struct {
			level LogLevel
			regex *regexp.Regexp
		}{
			// ERROR — explicit level words or Go stack-trace goroutine header
			{LogLevelError, regexp.MustCompile(`(?i)\b(error|err|fatal|panic|crit|critical|alert|emerg)\b`)},
			// WARN — degraded-but-running conditions
			{LogLevelWarn, regexp.MustCompile(`(?i)\b(warn|warning|deprecated|deprecation|timeout|timed.?out|slow|retry|retrying|retried|skipping|skipped|throttl)\b`)},
			// DEBUG — verbose diagnostic output; checked before INFO so that lines
			// containing both "debug" and an INFO keyword (e.g. "loaded") resolve correctly
			{LogLevelDebug, regexp.MustCompile(`(?i)\b(debug|trace|verbose|dbg)\b`)},
			// INFO — normal operational messages; also catches Go std-logger lines
			// (format: "2006/01/02 15:04:05 message") which have no level marker
			{LogLevelInfo, regexp.MustCompile(
				`(?i)` +
					`\b(info|notice|` +
					`starting|started|stopping|stopped|shutting|shutdown|` +
					`connect(ed|ing)|disconnect(ed|ing)|` +
					`initializ(ed|ing)|` +
					`listen(ed|ing)|` +
					`loaded|loading|` +
					`completed|complete|` +
					`refreshed|refreshing|` +
					`running|` +
					`registered|` +
					`migrat(ed|ing|ion))\b` +
					// Go standard log prefix: "2006/01/02 15:04:05 "
					`|\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} `,
			)},
		},
	}
}

// ParseLevel parses the log level from a log line.
// Never returns LogLevelUnknown — unclassified lines default to INFO.
func (p *LogParser) ParseLevel(line string) LogLevel {
	level := p.detectLevel(line)

	// Override to ERROR when the line contains unambiguous failure words, even if
	// the line's own level marker says otherwise (e.g. a DBG line reporting a failure).
	if level != LogLevelError && p.errorOverridePattern.MatchString(line) {
		return LogLevelError
	}

	return level
}

// detectLevel determines the base level before the error-override pass.
func (p *LogParser) detectLevel(line string) LogLevel {
	// 1. slog key=value: level=INFO etc.
	if match := p.slogPattern.FindStringSubmatch(line); len(match) > 1 {
		return LogLevel(match[1])
	}

	// 2. Short 3-char codes (zerolog style)
	if match := p.shortCodePattern.FindStringSubmatch(line); len(match) > 1 {
		switch match[1] {
		case "ERR":
			return LogLevelError
		case "WRN":
			return LogLevelWarn
		case "INF":
			return LogLevelInfo
		case "DBG":
			return LogLevelDebug
		case "TRC":
			return LogLevelTrace
		}
	}

	// 3. Keyword fallbacks, checked in severity order
	for _, pattern := range p.fallbackPatterns {
		if pattern.regex.MatchString(line) {
			return pattern.level
		}
	}

	// 4. Default: treat all unclassified output as INFO.
	//    Nearly all plain log output (build steps, progress lines, etc.) is informational.
	return LogLevelInfo
}
