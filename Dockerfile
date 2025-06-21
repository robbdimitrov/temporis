FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o timer-service ./cmd/server

FROM scratch
WORKDIR /app
COPY --from=builder /app/timer-service .
CMD ["./timer-service"]
