# Run `make` with no target to list the targets.
# Tools (staticcheck, govulncheck) are pinned in go.mod's tool block and run with `go tool`,
# so nothing has to be installed besides Go and make.

BINARY := bin/tensorgate
PKG    := ./cmd/tensorgate

.DEFAULT_GOAL := help

.PHONY: help run build test cover fmt fmt-check vet lint vuln tidy check clean

help: ## list targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

# run starts the built binary instead of `go run`, so Ctrl+C/SIGTERM reach the server itself
# and graceful shutdown exits cleanly instead of through the go run wrapper
run: build ## build and run the gateway (API on :8080, metrics on :9090)
	./$(BINARY)

build: ## build the binary into bin/
	go build -trimpath -o $(BINARY) $(PKG)

test: ## run all tests with the race detector
	go test -race ./...

cover: ## run tests and print coverage per function
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

fmt: ## format all Go files
	go fmt ./...

fmt-check: ## fail if any file isn't gofmt'ed
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "not formatted:"; echo "$$out"; exit 1; fi

vet: ## run go vet
	go vet ./...

lint: ## run staticcheck
	go tool staticcheck ./...

vuln: ## check dependencies and stdlib for known vulnerabilities
	go tool govulncheck ./...

tidy: ## sync go.mod and go.sum with the imports
	go mod tidy

check: fmt-check vet lint test ## everything CI should run before merging

clean: ## remove build and coverage output
	rm -rf bin coverage.out
