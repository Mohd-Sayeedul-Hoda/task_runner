package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/Mohd-Sayeedul-Hoda/task_runner/internal/database"
	pb "github.com/Mohd-Sayeedul-Hoda/task_runner/internal/grpcapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	status "google.golang.org/grpc/status"
)

const (
	WorkerTimeout    = 30 * time.Second
	TaskPollInterval = 5 * time.Second
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
	workerPoolMutex sync.RWMutex

	queries *database.Queries
	dbPool  *pgxpool.Pool
	wg      sync.WaitGroup

	roundRobinIdx      int
	workerPoolKeyMutex sync.RWMutex
	workerPoolKey      []string
}

type WorkerInfo struct {
	workerId string
	lastSeen time.Time
	addr     string
	conn     *grpc.ClientConn
	client   pb.WorkerServiceClient
}

func NewServer(dbPool *pgxpool.Pool, queries *database.Queries) *SchedulerServer {
	return &SchedulerServer{
		dbPool:        dbPool,
		queries:       queries,
		workerPool:    make(map[string]*WorkerInfo),
		workerPoolKey: make([]string, 0),
		roundRobinIdx: 0,
	}
}

func (s *SchedulerServer) Shutdown() {
	s.workerPoolMutex.Lock()
	defer s.workerPoolMutex.Unlock()

	for _, worker := range s.workerPool {
		if worker.conn != nil {
			worker.conn.Close()
		}
	}
	slog.Info("all worker connection close...")
	s.workerPool = make(map[string]*WorkerInfo)
}

func (s *SchedulerServer) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.MessageAck, error) {

	s.workerPoolMutex.Lock()
	if worker, ok := s.workerPool[req.GetWorkerId()]; ok {

		worker.lastSeen = time.Now()

		s.workerPoolMutex.Unlock()

	} else {
		s.workerPoolMutex.Unlock()

		conn, err := grpc.NewClient(req.GetWorkerAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			slog.Error("Failed to connect with worker", "error", err, "worker_id", req.GetWorkerId())
			return nil, status.Error(codes.Internal, "unable to dail worker from scheduler")
		}

		s.workerPoolMutex.Lock()

		if _, stillNew := s.workerPool[req.GetWorkerId()]; stillNew {
			conn.Close()
		} else {
			s.workerPool[req.GetWorkerId()] = &WorkerInfo{
				workerId: req.GetWorkerId(),
				addr:     req.GetWorkerAddress(),
				conn:     conn,
				client:   pb.NewWorkerServiceClient(conn),
				lastSeen: time.Now(),
			}

			s.workerPoolKeyMutex.Lock()
			s.workerPoolKey = append(s.workerPoolKey, req.GetWorkerId())
			s.workerPoolKeyMutex.Unlock()

			slog.Info("Registered Worker", "worker_id", req.GetWorkerId())
		}

		s.workerPoolMutex.Unlock()
	}

	return &pb.MessageAck{Success: true}, nil
}

func (s *SchedulerServer) UpdateTaskStatus(ctx context.Context, req *pb.UpdateTaskStatusRequest) (*pb.MessageAck, error) {

	taskId, err := uuid.Parse(req.GetTaskId())
	if err != nil {
		slog.Error("invalid uuid received", "task_id", req.GetTaskId())
		return nil, status.Error(5, "not a valid task id")
	}

	dbStatus := s.mapGRPCStatusToDB(req.GetStatus())

	slog.Info("updating task status", "task_id", taskId, "status", dbStatus)

	err = s.queries.UpdateTaskStatus(ctx, database.UpdateTaskStatusParams{
		ID:     taskId,
		Status: dbStatus,
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
		if time.Since(worker.lastSeen) > WorkerTimeout {
			slog.Warn("Removing unused worker", "worker_id", workerID, "inactive_for", time.Since(worker.lastSeen).Round(time.Second).String(), "last_seen", worker.lastSeen.String())
			if worker.conn != nil {
				worker.conn.Close()
			}
			delete(s.workerPool, workerID)
			s.workerPoolKeyMutex.Lock()
			s.workerPoolKey = make([]string, 0, len(s.workerPool))
			for workerID := range s.workerPool {
				s.workerPoolKey = append(s.workerPoolKey, workerID)
			}
			s.workerPoolKeyMutex.Unlock()
		}
	}

}

func (s *SchedulerServer) ManageWorkerPool(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(WorkerTimeout)
	for {
		select {
		case <-ctx.Done():
			slog.Info("recived cancel context: stopping manage worker pool")
			return
		case <-ticker.C:
			s.removeInactiveWorkerPool()
		}
	}
}

func (s *SchedulerServer) ManageTask(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(TaskPollInterval)
	for {
		select {
		case <-ticker.C:
			s.manageAndScheduleTask(ctx)
		case <-ctx.Done():
			slog.Info("stopping manage task...")
			return
		}
	}
}

func (s *SchedulerServer) manageAndScheduleTask(ctx context.Context) {
	tx, err := s.dbPool.Begin(ctx)
	if err != nil {
		slog.Error("failed to begin transaciton", "error", err)
		return
	}

	defer tx.Rollback(ctx)

	qtx := s.queries.WithTx(tx)

	tasks, err := qtx.GetPendingTasksForUpdate(ctx)
	if err != nil {
		slog.Error("unable to fetch pending task", "err", err)
		return
	}

	for _, task := range tasks {
		worker, err := s.getWorker()
		if err != nil {
			slog.Error("unable to get worker", "err", err)
			continue
		}

		childCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err = worker.client.SubmitTask(childCtx, &pb.TaskRequest{
			TaskId:  task.ID.String(),
			Payload: task.Payload,
		})
		cancel()
		if err != nil {
			slog.Error("failed to submit task", "err", err)
			continue
		}

		err = qtx.UpdateTaskStatus(ctx, database.UpdateTaskStatusParams{
			Status: string(Queued),
			ID:     task.ID,
		})
		if err != nil {
			slog.Error("failed to update task status", "err", err, "task_id", task.ID)
			continue
		}
	}

	if err = tx.Commit(ctx); err != nil {
		slog.Error("failed to commit task", "err", err)
		return
	}
	slog.Info("Successfully committed task updates")
}

// using round robin to distribute worker load
func (s *SchedulerServer) getWorker() (*WorkerInfo, error) {

	s.workerPoolKeyMutex.Lock()
	defer s.workerPoolKeyMutex.Unlock()

	if len(s.workerPoolKey) == 0 {
		return nil, errors.New("no worker found")
	}

	s.roundRobinIdx = (s.roundRobinIdx + 1) % len(s.workerPoolKey)
	worker := s.workerPool[s.workerPoolKey[s.roundRobinIdx]]

	return worker, nil

}

func (s *SchedulerServer) mapGRPCStatusToDB(grpcStatus pb.TaskStatus) string {
	switch grpcStatus {
	case pb.TaskStatus_PENDING:
		return string(Pending)
	case pb.TaskStatus_QUEUED:
		return string(Queued)
	case pb.TaskStatus_RUNNING:
		return string(Running)
	case pb.TaskStatus_COMPLETE:
		return string(COMPLETED)
	case pb.TaskStatus_FAILED:
		return string(FAILED)
	default:
		return string(Pending)
	}
}
