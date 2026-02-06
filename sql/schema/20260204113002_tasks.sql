-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS tasks(
  id UUID PRIMARY KEY,
  task_type TEXT NOT NULL,
  payload JSONB,
  status TEXT NOT NULL,
  run_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_tasks_status_run_at
ON tasks(status,
run_at);
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS tasks;
-- +goose StatementEnd
