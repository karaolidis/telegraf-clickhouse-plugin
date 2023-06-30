.PHONY: build

COMMIT_HASH=$(shell git rev-parse --short HEAD)

build:
	CGOENABLED=false GOOS=linux GOARCH=amd64 go build -o clickhouse_${COMMIT_HASH}_amd64 cmd/main.go
	CGOENABLED=false GOOS=linux GOARCH=arm64 go build -o clickhouse_${COMMIT_HASH}_arm64 cmd/main.go
