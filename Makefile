.PHONY: all build test lint vet proto docker clean fmt help

# Build variables
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE    ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)"

# Go variables
GOBIN   := $(shell go env GOPATH)/bin
GOCMD   := go
GOBUILD := $(GOCMD) build $(LDFLAGS)
GOTEST  := $(GOCMD) test
GOVET   := $(GOCMD) vet
GOFMT   := gofmt

# Output directories
BIN_DIR := bin

all: fmt vet lint build ## Run fmt, vet, lint, and build

build: build-master build-agent build-ctl ## Build all binaries

build-master: ## Build gosched-master
	$(GOBUILD) -o $(BIN_DIR)/gosched-master ./cmd/gosched-master

build-agent: ## Build gosched-agent
	$(GOBUILD) -o $(BIN_DIR)/gosched-agent ./cmd/gosched-agent

build-ctl: ## Build goschedctl
	$(GOBUILD) -o $(BIN_DIR)/goschedctl ./cmd/goschedctl

test: ## Run unit tests
	$(GOTEST) -race -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -func=coverage.out

test-short: ## Run short tests only
	$(GOTEST) -short -race ./...

lint: ## Run golangci-lint
	golangci-lint run ./...

vet: ## Run go vet
	$(GOVET) ./...

fmt: ## Format code
	$(GOFMT) -s -w .

proto: ## Generate protobuf code
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/proto/v1/*.proto

docker: docker-master docker-agent ## Build all Docker images

docker-master: ## Build master Docker image
	docker build -f deploy/docker/Dockerfile.master -t gosched-master:$(VERSION) .

docker-agent: ## Build agent Docker image
	docker build -f deploy/docker/Dockerfile.agent -t gosched-agent:$(VERSION) .

clean: ## Remove build artifacts
	rm -rf $(BIN_DIR) coverage.out

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
