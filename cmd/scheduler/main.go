package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/Mohd-Sayeedul-Hoda/task_runner/internal/database"
	pb "github.com/Mohd-Sayeedul-Hoda/task_runner/internal/grpcapi"
	"github.com/Mohd-Sayeedul-Hoda/task_runner/internal/scheduler"
	"google.golang.org/grpc"

	"github.com/caarlos0/env/v11"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type Config struct {
	Port         int           `env:"PORT" envDefault:"10000"`
	DSN          string        `env:"DB_DSN,required"`
	MaxOpenConn  int32         `env:"DB_MAX_OPEN_CONNS" envDefault:"10"`
	MaxIdealConn int32         `env:"DB_MAX_IDLE_CONNS" envDefault:"10"`
	MaxIdleTime  time.Duration `env:"DB_MAX_IDLE_TIME" envDefault:"10m"`
}

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	var getenv = func(key string) string {
		return os.Getenv(key)
	}

	ctx := context.Background()

	err = run(ctx, getenv, os.Stdout, os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, getenv func(string) string, w io.Writer, args []string) error {

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	var cfg Config
	err := env.Parse(&cfg)
	if err != nil {
		return fmt.Errorf("unable to parse env config: %s", err.Error())
	}
	slog.Info("successfully parse config")

	dbPool, err := OpenPostgresConn(ctx, &cfg)
	if err != nil {
		return fmt.Errorf("unable to open postgres connection: %s", err.Error())
	}
	slog.Info("database connected")

	dbQueries := database.New(dbPool)

	tcpListner, err := net.Listen("tcp", ":"+strconv.Itoa(cfg.Port))
	if err != nil {
		return fmt.Errorf("unable to start tcp server at port=%d err=%s", cfg.Port, err.Error())
	}

	server := scheduler.NewServer(dbQueries)
	schedulerRegistery := grpc.NewServer()
	pb.RegisterSchedulerServer(schedulerRegistery, server)

	serverError := make(chan error, 1)
	go func() {
		slog.Info("starting grpc server", "port", cfg.Port)
		err := schedulerRegistery.Serve(tcpListner)
		if err != nil {
			slog.Error("error while starting grpc server", "err", err.Error())
			serverError <- err
			return
		}
	}()

	select {
	case <-ctx.Done():
		slog.Info("shutdown initiated", slog.String("reason", "context cancelled"))
	case err := <-serverError:
		return err
	}

	return nil
}

func OpenPostgresConn(ctx context.Context, cfg *Config) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, err
	}

	poolConfig.MaxConns = int32(cfg.MaxOpenConn)
	poolConfig.MinConns = int32(cfg.MaxIdealConn)
	poolConfig.MaxConnIdleTime = cfg.MaxIdleTime

	conn, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}

	if err = conn.Ping(ctx); err != nil {
		return nil, err
	}

	return conn, nil
}
