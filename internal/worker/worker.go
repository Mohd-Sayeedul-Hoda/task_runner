package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	pb "github.com/Mohd-Sayeedul-Hoda/task_runner/internal/grpcapi"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Config struct {
	WorkerPort    int    `env:"WORKER_PORT" envDefault:"51000"`
	WorkerAddr    string `env:"WORKER_ADDRESS,required"`
	SchedulerAddr string `env:"SCHEDULER_ADDRESS,required"`
}

const (
	noOfConcurrentWorker = 50
	heartBeatInterval    = 5
)

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	id              uuid.UUID
	schedulerClient pb.SchedulerClient
	schedulerConn   *grpc.ClientConn
	workerAddr      string
	workerPort      int

	taskQueue chan *pb.TaskRequest
}

func NewWorker(cfg *Config, schedulerConn *grpc.ClientConn) *WorkerServer {

	schedulerClient := pb.NewSchedulerClient(schedulerConn)

	return &WorkerServer{
		id:         uuid.New(),
		workerPort: cfg.WorkerPort,
		workerAddr: cfg.WorkerAddr,

		schedulerClient: schedulerClient,
		schedulerConn:   schedulerConn,

		taskQueue: make(chan *pb.TaskRequest, 200),
	}
}

func (s *WorkerServer) SendHeartBeat(ctx context.Context) {
	if s.schedulerClient == nil {
		slog.Error("scheduler client is nil")
		return
	}

	_, err := s.schedulerClient.SendHeartbeat(ctx, &pb.HeartbeatRequest{
		WorkerId:      s.id.String(),
		WorkerAddress: s.workerAddr,
	})
	if err != nil {
		slog.Error("unable to send heart beat to scheduler", slog.String("worker_id", s.id.String()), slog.Any("err", err))
		return
	}
	slog.Info("heart beat sent")

}

func (s *WorkerServer) ManageSendHeartBeat(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(time.Second * heartBeatInterval)

	for {
		select {
		case <-ctx.Done():
			slog.Info("stopping manage heart beat")
			return
		case <-ticker.C:
			s.SendHeartBeat(ctx)
		}
	}

}

func (s *WorkerServer) ManageWorkerPool(ctx context.Context, wg *sync.WaitGroup) {
	for range noOfConcurrentWorker {
		wg.Add(1)
		go s.executeTask(ctx, wg)
	}
}

func (s *WorkerServer) executeTask(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case task := <-s.taskQueue:
			go s.schedulerClient.UpdateTaskStatus(ctx, &pb.UpdateTaskStatusRequest{
				TaskId: task.TaskId,
				Status: pb.TaskStatus_RUNNING,
			})
			err := doWork(task.TaskId, task.GetPayload())
			status := pb.TaskStatus_COMPLETE
			logMsg := ""
			if err != nil {
				status = pb.TaskStatus_FAILED
				logMsg = err.Error()
			}

			go s.schedulerClient.UpdateTaskStatus(ctx, &pb.UpdateTaskStatusRequest{
				TaskId:     task.TaskId,
				Status:     status,
				LogMessage: logMsg,
			})

		}
	}
}

func (s *WorkerServer) SubmitTask(ctx context.Context, req *pb.TaskRequest) (*pb.MessageAck, error) {
	select {
	case s.taskQueue <- req:
		return &pb.MessageAck{
			Success: true,
		}, nil
	case <-time.After(100 * time.Millisecond):
		return nil, status.Error(codes.Unavailable, "worker queued full")
	}

}

func doWork(id string, payload []byte) error {
	slog.Info("starting to execute task", "task_id", id)
	time.Sleep(time.Second * 5)
	slog.Info("completed task", "task_id", id)
	return nil
}
