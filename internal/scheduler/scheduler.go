package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Mohd-Sayeedul-Hoda/task_runner/internal/database"
	pb "github.com/Mohd-Sayeedul-Hoda/task_runner/internal/grpcapi"
	"github.com/google/uuid"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	status "google.golang.org/grpc/status"
)

type TaskStatus string

var (
	Pending   TaskStatus = "PENDING"
	Queued    TaskStatus = "QUEUED"
	Running   TaskStatus = "RUNNING"
	COMPLETED TaskStatus = "COMPLETED"
	FAILED    TaskStatus = "FAILED"
)

type SchedulerServer struct {
	pb.UnimplementedSchedulerServer
	workerPool      map[string]*WorkerInfo
	workerPoolMutex sync.Mutex

	queries *database.Queries
	wg      sync.WaitGroup
}

type WorkerInfo struct {
	workerId      string
	lastSeen      time.Time
	cpuUsage      float32
	availableSlot int32
	addr          string
	conn          *grpc.ClientConn
	client        pb.WorkerServiceClient
}

func NewServer(queries *database.Queries) *SchedulerServer {
	return &SchedulerServer{
		queries:    queries,
		workerPool: make(map[string]*WorkerInfo),
	}
}

func (s *SchedulerServer) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.MessageAck, error) {

	s.workerPoolMutex.Lock()
	if worker, ok := s.workerPool[req.GetWorkerId()]; ok {

		worker.lastSeen = time.Now()
		worker.cpuUsage = req.GetCpuUsage()
		worker.availableSlot = req.GetAvailableSlots()

		s.workerPoolMutex.Unlock()

	} else {
		s.workerPoolMutex.Unlock()

		conn, err := grpc.NewClient(req.GetWorkerAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			slog.Error("Failed to connect with worker", "error", err, "worker_id", req.GetWorkerId())
			return nil, status.Error(codes.Internal, "unable to dail worker from scheduler")
		}

		s.workerPoolMutex.Lock()

		s.workerPool[req.GetWorkerId()] = &WorkerInfo{
			workerId:      req.GetWorkerId(),
			addr:          req.GetWorkerAddress(),
			conn:          conn,
			client:        pb.NewWorkerServiceClient(conn),
			lastSeen:      time.Now(),
			cpuUsage:      req.GetCpuUsage(),
			availableSlot: req.GetAvailableSlots(),
		}

		s.workerPoolMutex.Unlock()
		slog.Info("Registed Worker", slog.String("worker_id", req.GetWorkerId()))

	}

	return &pb.MessageAck{Success: true}, nil
}

func (s *SchedulerServer) UpdateTaskStatus(ctx context.Context, req *pb.UpdateTaskStatusRequest) (*pb.MessageAck, error) {

	taskId, err := uuid.Parse(req.GetTaskId())
	if err != nil {
		slog.Error("invalid uuid received", "task_id", req.GetTaskId())
		return nil, status.Error(5, "not a valid task id")
	}

	statusStr := req.GetStatus().String()
	switch req.GetStatus() {
	case pb.TaskStatus_PENDING, pb.TaskStatus_QUEUED, pb.TaskStatus_RUNNING,
		pb.TaskStatus_COMPLETE, pb.TaskStatus_FAILED:
		slog.Info("updating task status",
			"task_id", taskId,
			"status", statusStr,
			"log", req.GetLogMessage(),
		)
	default:
		return nil, status.Error(codes.InvalidArgument, "unrecognized task status")
	}

	err = s.queries.UpdateTaskStatus(ctx, database.UpdateTaskStatusParams{
		ID:     taskId,
		Status: statusStr,
	})
	if err != nil {
		slog.Error("database update failed", "task_id", taskId.String(), "error", err)
		return nil, status.Error(codes.Internal, "intenal server error")
	}

	return &pb.MessageAck{
		Success: true,
	}, nil
}

func (s *SchedulerServer) removeInactiveWorkerPool() {
	s.workerPoolMutex.Lock()
	defer s.workerPoolMutex.Unlock()

	for workerID, worker := range s.workerPool {
		if time.Since(worker.lastSeen) > 30*time.Second {
			slog.Warn("Removing unused worker", "worker_id", workerID, "inactive_for", time.Since(worker.lastSeen).Round(time.Second).String(), "last_seen", worker.lastSeen.String())
			if worker.conn != nil {
				worker.conn.Close()
			}
			delete(s.workerPool, workerID)
		}
	}

}

func (s *SchedulerServer) ManageWorkerPool(ctx context.Context, wg *sync.WaitGroup) {
	wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	select {
	case <-ctx.Done():
		slog.Info("recived cancel context: stopping manage worker pool")
		return
	case <-ticker.C:
		s.removeInactiveWorkerPool()
	}

}
