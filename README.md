# Task Scheduler

A distributed task scheduling system built with Go, PostgreSQL, and gRPC.

## Architecture

![Architecture](./assets/arch.png)

## Services

- **API Server** - REST API for creating and managing tasks
- **Scheduler** - gRPC service that schedules tasks to workers
- **Worker** - Executes scheduled tasks

## Database Schema

The system uses PostgreSQL with the following main table:

### Tasks Table
| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| task_type | TEXT | Type of task (e.g., "email", "webhook") |
| payload | JSONB | Task data/content |
| status | TEXT | Current status (PENDING, QUEUED, RUNNING, COMPLETED, FAILED) |
| run_at | TIMESTAMPTZ | When the task should run |
| created_at | TIMESTAMPTZ | Creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

### Running Migrations

Migrations are handled automatically by the scheduler on startup using Goose.

To run migrations manually:

```bash
export GOOSE_DRIVER=postgres
export GOOSE_DBSTRING="postgres://postgres:password@localhost:5432/taskrunner?sslmode=disable"
goose up sql/schema
```

Or create a new migration:
```bash
goose create <migration_name> sql
```

## Quick Start with Docker Compose

```bash
docker compose up --build
```

The API server will be available at `http://localhost:8080`

To scale workers:
```bash
docker compose up -d --scale worker=3
```

## Running Locally

### Prerequisites

- Go 1.21+
- PostgreSQL 15+
- Docker (optional)

### 1. Set up PostgreSQL

```bash
# Create database
createdb taskrunner

# Or with Docker
docker run -d \
  --name postgres \
  -e POSTGRES_DB=taskrunner \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=password \
  -p 5432:5432 \
  postgres:15
```

### 2. Set Environment Variables

```bash
export DB_DSN="postgres://postgres:password@localhost:5432/taskrunner?sslmode=disable"
export HTTP_PORT="8080"
export SCHEDULER_PORT="10000"
export WORKER_PORT="51000"
export SCHEDULER_ADDRESS="localhost:10000"
```

### 3. Run Services

```bash
# Terminal 1 - Scheduler
go run ./cmd/scheduler

# Terminal 2 - Worker
go run ./cmd/worker

# Terminal 3 - API Server
go run ./cmd/api
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/healthz` | Health check |
| POST | `/api/tasks` | Create a new task |
| GET | `/api/tasks/{id}` | Get task by ID |

### Create Task Example

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "email",
    "payload": {"to": "user@example.com", "message": "Hello"},
    "run_at": "2026-02-20T10:00:00Z"
  }'
```
