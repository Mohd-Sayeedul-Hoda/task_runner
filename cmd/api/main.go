package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/Mohd-Sayeedul-Hoda/task_runner/internal/database"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

const (
	MaxOpenConn = 10
	MinConns    = 10
	MaxIdleTime = "10m"
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

	pgPool, err := openPostgresConn(ctx, getenv)
	if err != nil {
		return fmt.Errorf("unable to open postgres conn: %v", err)
	}
	slog.Info("database connected")
	defer pgPool.Close()

	db := database.New(pgPool)

	mux := http.NewServeMux()
	mux.Handle("GET /api/healthz", handlerHealth())
	mux.Handle("POST /api/tasks", createTask(db))
	mux.Handle("GET /api/tasks/{id}", getTask(db))

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
	err = httpServer.Shutdown(shutdownCtx)
	if err != nil {
		return err
	}
	slog.Info("http server stop")

	return nil
}

func openPostgresConn(ctx context.Context, getenv func(string) string) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(getenv("DB_DSN"))
	if err != nil {
		return nil, err
	}

	poolConfig.MaxConns = int32(MaxOpenConn)
	poolConfig.MinConns = int32(MinConns)

	conn, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}

	if err = conn.Ping(ctx); err != nil {
		return nil, err
	}

	return conn, nil
}
