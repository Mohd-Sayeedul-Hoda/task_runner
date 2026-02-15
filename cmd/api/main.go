package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to load .env: %s", err.Error())
		os.Exit(1)
	}

	getenv := func(key string) string {
		return os.Getenv(key)
	}

	ctx := context.Background()
	err = run(ctx, getenv)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run(ctx context.Context, getenv func(string) string) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	mux := http.NewServeMux()
	mux.Handle("GET /api/healthz", handlerHealth())

	httpServer := http.Server{
		Addr:    ":" + getenv("HTTP_PORT"),
		Handler: mux,
	}

	serverErr := make(chan error, 1)
	go func() {
		slog.Info("starting http server", "port", getenv("HTTP_PORT"))
		err := httpServer.ListenAndServe()
		if err != nil {
			slog.Error("unable to start http server", "err", err)
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
		slog.Info("shutdown initiated", slog.String("reason", "context cancelled"))
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	slog.Info("stopping http server")
	err := httpServer.Shutdown(shutdownCtx)
	if err != nil {
		return err
	}
	slog.Info("http server stop")

	return nil
}

func handlerHealth() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respondWithJson(w, http.StatusOK, envelope{
			"msg": "service is healthy",
		})
	})
}
