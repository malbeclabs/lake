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
// It deliberately does not use middleware.RealIP: lake-api sits behind a
// layer-4 NLB, so RemoteAddr is the NLB hop, and whether anything in front of
// it sets these headers (rather than passing through what the client sent) is
// not established. Logging both keeps the client address recoverable without
// letting a client-supplied header replace the one address the server knows.
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
