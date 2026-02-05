package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Mohd-Sayeedul-Hoda/task_runner/internal/database"
	pb "github.com/Mohd-Sayeedul-Hoda/task_runner/internal/grpcapi"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	status "google.golang.org/grpc/status"
)

type SchedulerServer struct {
	pb.UnimplementedSchedulerServer
	workerPool      map[string]*WorkerInfo
	workerPoolMutex sync.Mutex

	queries *database.Queries
	ctx     context.Context
	cancel  context.CancelFunc
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

func NewServer(port int, queries *database.Queries) *SchedulerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &SchedulerServer{
		queries:    queries,
		workerPool: make(map[string]*WorkerInfo),
		ctx:        ctx,
		cancel:     cancel,
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
	return nil, status.Error(codes.Unimplemented, "method SendHeartbeat not implemented")
}

func (s *SchedulerServer) RemoveInactiveWorkerPool() {
	s.workerPoolMutex.Lock()
	defer s.workerPoolMutex.Unlock()

	for workerID, worker := range s.workerPool {
		if time.Since(worker.lastSeen) > 30*time.Second {
			slog.Warn("Removing unused worker", "worker_id", workerID, "last_seen", worker.lastSeen.String())
			if worker.conn != nil {
				worker.conn.Close()
			}
			delete(s.workerPool, workerID)
		}
	}

}
