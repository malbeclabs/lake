.PHONY: build lint fmt test ci web-build web-lint web-test

build:
	CGO_ENABLED=0 go build -v ./...

lint:
	golangci-lint run -c ./.golangci.yaml

fmt:
	go fmt ./...

test:
	go test -race -v ./...

ci: build lint test

web-build:
	cd web && bun install --frozen-lockfile && bun run build

web-lint:
	cd web && bun install --frozen-lockfile && bun run lint

web-test:
	cd web && bun install --frozen-lockfile && bun run test:run
