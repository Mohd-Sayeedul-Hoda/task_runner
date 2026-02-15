package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/Mohd-Sayeedul-Hoda/task_runner/internal/database"
	"github.com/google/uuid"

	"github.com/jackc/pgx/v5"
)

type TaskStatus string

var (
	Pending   TaskStatus = "PENDING"
	Queued    TaskStatus = "QUEUED"
	Running   TaskStatus = "RUNNING"
	COMPLETED TaskStatus = "COMPLETED"
	FAILED    TaskStatus = "FAILED"
)

type Task struct {
	ID        string    `json:"id"`
	TaskType  string    `json:"task_type"`
	Payload   any       `json:"payload"`
	RunAt     time.Time `json:"run_at"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Status    string    `json:"status"`
}

func handlerHealth() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respondWithJson(w, http.StatusOK, envelope{
			"msg": "service is healthy",
		})
	})
}

func createTask(db *database.Queries) http.Handler {
	type request struct {
		TaskType string    `json:"task_type"`
		Payload  any       `json:"payload"`
		RunAt    time.Time `json:"run_at"`
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		req := request{}
		err := decode(w, r, &req)
		if err != nil {
			if errors.Is(err, errInvalidRequest) {
				respondWithError(w, http.StatusBadRequest, err.Error(), nil)
				return
			}
			respondWithError(w, http.StatusInternalServerError, "internal server error", err)
			return
		}

		if req.TaskType == "" {
			respondWithError(w, http.StatusBadRequest, "task type should not be empty", err)
			return
		}

		data, err := json.Marshal(req.Payload)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, "payload should be valid json", err)
			return
		}

		if req.RunAt.IsZero() {
			req.RunAt = time.Now()
		}

		task, err := db.CreateTask(r.Context(), database.CreateTaskParams{
			TaskType: req.TaskType,
			Status:   string(Pending),
			Payload:  data,
			RunAt:    req.RunAt,
		})
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "internal server err", err)
			return
		}

		respondWithJson(w, http.StatusCreated, Task{
			ID:        task.ID.String(),
			TaskType:  task.TaskType,
			RunAt:     task.RunAt,
			CreatedAt: task.CreatedAt,
			UpdatedAt: task.UpdatedAt,
			Payload:   req.Payload,
			Status:    task.Status,
		})
	})
}

func getTask(db *database.Queries) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idStr := r.PathValue("id")
		id, err := uuid.Parse(idStr)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, "not valid task id", nil)
			return
		}

		task, err := db.GetTaskById(r.Context(), id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				respondWithError(w, http.StatusNotFound, "task not found", nil)
				return
			}
			respondWithError(w, http.StatusInternalServerError, "internal server error", err)
			return
		}

		var data envelope
		err = json.Unmarshal(task.Payload, &data)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "internal server error", err)
			return
		}

		respondWithJson(w, http.StatusCreated, Task{
			ID:        task.ID.String(),
			TaskType:  task.TaskType,
			RunAt:     task.RunAt,
			CreatedAt: task.CreatedAt,
			UpdatedAt: task.UpdatedAt,
			Payload:   data,
			Status:    task.Status,
		})

	})

}
