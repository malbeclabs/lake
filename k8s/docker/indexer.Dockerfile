FROM golang:1.25-bookworm

WORKDIR /lake
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /usr/local/bin/lake-indexer ./indexer/cmd/indexer/main.go

ENTRYPOINT ["lake-indexer"]
