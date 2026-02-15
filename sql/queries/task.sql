-- name: CreateTask :one
INSERT INTO tasks(id, task_type, payload, status,  run_at, created_at, updated_at)
VALUES( 
  gen_random_uuid(),
  $1,
  $2,
  $3,
  $4,
  NOW(),
  NOW()
)
RETURNING *;

-- name: GetTasksByStatus :many
SELECT * FROM tasks
WHERE status = $1 
ORDER BY run_at DESC;

-- name: UpdateTaskStatus :exec
UPDATE tasks
SET status = $1, updated_at = NOW()
WHERE id = $2;

-- name: GetPendingTasksForUpdate :many
SELECT * FROM tasks
WHERE status = 'PENDING' 
  AND run_at <= NOW()
ORDER BY run_at ASC
FOR UPDATE SKIP LOCKED;

-- name: GetStalledTasks :many
SELECT * FROM tasks
WHERE status IN ('RUNNING', 'QUEUED')
AND updated_at < NOW() - INTERVAL '5 minutes';

-- name: GetTaskById :one
SELECT * FROM tasks
WHERE id = $1;
