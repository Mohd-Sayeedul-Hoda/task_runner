package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"

	pb "github.com/Mohd-Sayeedul-Hoda/task_runner/internal/grpcapi"
	"github.com/Mohd-Sayeedul-Hoda/task_runner/internal/worker"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		slog.Info("no .env file found, relying on system environment variables")
	}

	ctx := context.Background()
	err = run(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	var cfg worker.Config
	err := env.Parse(&cfg)
	if err != nil {
		return fmt.Errorf("unable to parse env config: %s", err.Error())
	}
	slog.Info("successfully parse config")

	schedulerConn, err := grpc.NewClient(cfg.SchedulerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("unable to connect with scheduler: %s", err.Error())
	}
	slog.Info("connect with scheduler")

	tcpListner, err := net.Listen("tcp", ":"+strconv.Itoa(cfg.WorkerPort))
	if err != nil {
		return fmt.Errorf("unable to start tcp server at port=%d err=%s", cfg.WorkerPort, err.Error())
	}
	defer tcpListner.Close()
	slog.Info("tcp server started", "port", cfg.WorkerPort)

	workerServer := worker.NewWorker(&cfg, schedulerConn)
	grpcServer := grpc.NewServer()
	pb.RegisterWorkerServiceServer(grpcServer, workerServer)

	var wg sync.WaitGroup
	serverErr := make(chan error, 1)

	go func() {
		slog.Info("starting grpc server", "port", cfg.WorkerPort)
		err := grpcServer.Serve(tcpListner)
		if err != nil {
			slog.Error("unable to start grpc server", "err", err.Error())
			serverErr <- err
			return
		}
	}()

	workerServer.ManageWorkerPool(ctx, &wg)

	wg.Add(1)
	workerServer.ManageSendHeartBeat(ctx, &wg)

	select {
	case <-serverErr:
		slog.Info("server stopped", slog.String("reason", "server error"))
		cancel()
	case <-ctx.Done():
		slog.Info("shutdown initiated", slog.String("reason", "context cancelled"))
	}

	slog.Info("waiting for background goroutine...")
	wg.Wait()

	slog.Info("stopping grpc server...")
	grpcServer.GracefulStop()

	return nil
}
