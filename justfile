alias bs := build-scheduler
alias bw := build-worker
alias bh := build-httpserver
alias rs := run-scheduler
alias rw := run-worker
alias rh := run-httpserver

# Compile the scheduler binary
build-scheduler:
  go build  -o ./build/scheduler ./cmd/scheduler

# Compile the worker binary
build-worker:
  go build -o ./build/worker ./cmd/worker

# build http server
build-httpserver:
  go build -o ./build/http ./cmd/api

# Build and then execute the scheduler
run-scheduler: build-scheduler
  ./build/scheduler

# Build and then execute the worker
run-worker: build-worker
  ./build/worker

# run http server
run-httpserver: build-httpserver
  ./build/http

# Genaret sqlc file
sqlc:
  sqlc generate

# Generate Protobuf files
proto:
  protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    internal/grpcapi/services.proto

# Clean up build artifacts
clean:
    rm -rf ./build/
