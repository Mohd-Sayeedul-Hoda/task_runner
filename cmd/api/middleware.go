package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type logRespWriter struct {
	http.ResponseWriter
	statusCode int
}

func newLoggingResponseWriter(w http.ResponseWriter) *logRespWriter {
	return &logRespWriter{w, http.StatusOK}
}

func (lrw *logRespWriter) writeHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func newLoggingMiddleware(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		lrw := newLoggingResponseWriter(w)
		startTime := time.Now()

		next.ServeHTTP(lrw, r)

		duration := time.Since(startTime)
		slog.Info("api info",
			"status", http.StatusText(lrw.statusCode),
			"method", r.Method,
			"uri", r.RequestURI,
			"duration", duration.String(),
		)
	}
}

func recoverPanic(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				w.Header().Set("Connection", "close")
				respondWithError(w, r, http.StatusInternalServerError, "internal server error", fmt.Errorf("%s", err))
			}
		}()

		next.ServeHTTP(w, r)
	}
}
