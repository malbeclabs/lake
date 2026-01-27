.PHONY: build lint fmt test ci

build:
	CGO_ENABLED=0 go build -v ./...

lint:
	golangci-lint run -c ./.golangci.yaml

fmt:
	go fmt ./...

test:
	go test -race -v ./...

ci: build lint test
