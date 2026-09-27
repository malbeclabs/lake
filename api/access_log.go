package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/go-chi/chi/v5/middleware"
)

// forwardedLogFormatter is chi's default access-log line with any
// X-Forwarded-For / X-Real-IP header appended to the "from" address.
//
// Not middleware.RealIP: RemoteAddr is the NLB hop, and nothing establishes
// that the headers are set upstream rather than passed through from the client,
// so they are logged beside RemoteAddr instead of replacing it.
type forwardedLogFormatter struct {
	middleware.DefaultLogFormatter
}

func newAccessLogger() func(http.Handler) http.Handler {
	return middleware.RequestLogger(&forwardedLogFormatter{
		DefaultLogFormatter: middleware.DefaultLogFormatter{Logger: log.New(os.Stdout, "", log.LstdFlags)},
	})
}

func (f *forwardedLogFormatter) NewLogEntry(r *http.Request) middleware.LogEntry {
	from := forwardedFrom(r)
	if from == r.RemoteAddr {
		return f.DefaultLogFormatter.NewLogEntry(r)
	}
	// The default formatter prints r.RemoteAddr; hand it a shallow copy so the
	// request the handlers see is untouched.
	rc := *r
	rc.RemoteAddr = from
	return f.DefaultLogFormatter.NewLogEntry(&rc)
}

// forwardedFrom returns RemoteAddr followed by any forwarding headers, quoted
// because their contents are client-controlled.
func forwardedFrom(r *http.Request) string {
	var b strings.Builder
	b.WriteString(r.RemoteAddr)
	for _, h := range []string{"X-Forwarded-For", "X-Real-IP"} {
		if v := r.Header.Get(h); v != "" {
			fmt.Fprintf(&b, " %s=%q", strings.ToLower(h), v)
		}
	}
	return b.String()
}
