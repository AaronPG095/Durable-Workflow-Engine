FROM golang:1.24-alpine AS builder

WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/dwe-worker ./cmd/dwe-worker && \
    CGO_ENABLED=0 GOOS=linux go build -o /out/dwe ./cmd/dwe

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /out/dwe-worker /usr/local/bin/dwe-worker
COPY --from=builder /out/dwe /usr/local/bin/dwe

ENTRYPOINT ["/usr/local/bin/dwe-worker"]
