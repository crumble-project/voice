.PHONY: check build test lint vuln

check: lint test vuln

build:
	go build -trimpath -o bin/crumble-voice ./cmd/crumble-voice

test:
	go test -race -count=1 ./...

lint:
	go mod tidy -diff
	golangci-lint run ./...

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...
