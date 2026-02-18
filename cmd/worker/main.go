package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"

	pb "github.com/Mohd-Sayeedul-Hoda/task_runner/internal/grpcapi"
	"github.com/Mohd-Sayeedul-Hoda/task_runner/internal/worker"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		slog.Info("no .env file found, relying on system environment variables")
	}

	getenv := func(key string) string {
		return os.Getenv(key)
	}

	ctx := context.Background()
	err = run(ctx, getenv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, getenv func(string) string) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	schedulerConn, err := grpc.NewClient(getenv("SCHEDULER_ADDRESS"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("unable to connect with scheduler: %s", err.Error())
	}
	defer schedulerConn.Close()
	slog.Info("connect with scheduler")

	tcpListner, err := net.Listen("tcp", ":"+getenv("WORKER_PORT"))
	if err != nil {
		return fmt.Errorf("unable to start tcp server at port=%s err=%s", getenv("WORKER_PORT"), err.Error())
	}
	defer tcpListner.Close()
	slog.Info("tcp server started", "port", getenv("WORKER_PORT"))

	workerAddress, err := getWorkerAddr(getenv)
	if err != nil {
		return err
	}
	slog.Info("worker public address", "address", workerAddress)

	workerServer := worker.NewWorker(schedulerConn, workerAddress)
	grpcServer := grpc.NewServer()
	pb.RegisterWorkerServiceServer(grpcServer, workerServer)

	var wg sync.WaitGroup
	serverErr := make(chan error, 1)

	go func() {
		slog.Info("starting grpc server", "port", getenv("WORKER_PORT"))
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

func getWorkerAddr(getenv func(string) string) (string, error) {
	if host := getenv("WORKER_HOST"); host != "" {
		return fmt.Sprintf("%s:%s", host, os.Getenv("WORKER_PORT")), nil
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("unable to get hostname: %w", err)
	}

	return fmt.Sprintf("%s:%s", hostname, getenv("WORKER_PORT")), nil
}
