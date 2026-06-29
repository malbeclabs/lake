package logger

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"

	"github.com/malbeclabs/lake/utils/pkg/redact"
)

func New(verbose bool) *slog.Logger {
	logLevel := slog.LevelInfo
	if verbose {
		logLevel = slog.LevelDebug
	}
	return slog.New(tint.NewHandler(os.Stdout, &tint.Options{
		Level:   logLevel,
		NoColor: true,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				t := a.Value.Time().UTC()
				a.Value = slog.StringValue(formatRFC3339Millis(t))
				return a
			}
			// Redact credentials embedded in URLs from string and error values.
			// Catches both direct URL fields (logger.Info("...", "url", rpcURL))
			// and URLs that bubble up inside *url.Error / wrapped error messages.
			switch a.Value.Kind() {
			case slog.KindString:
				s := a.Value.String()
				if s == "" {
					return slog.Attr{}
				}
				if r := redact.String(s); r != s {
					a.Value = slog.StringValue(r)
				}
			case slog.KindAny:
				if err, ok := a.Value.Any().(error); ok && err != nil {
					a.Value = slog.StringValue(redact.Error(err))
				}
			}
			return a
		},
	}))
}

func formatRFC3339Millis(t time.Time) string {
	t = t.UTC()
	base := t.Format("2006-01-02T15:04:05")
	ms := t.Nanosecond() / 1_000_000
	return fmt.Sprintf("%s.%03dZ", base, ms)
}
