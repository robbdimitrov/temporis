FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o temporis ./cmd/server

FROM scratch
WORKDIR /app
COPY --from=builder /app/temporis .
CMD ["./temporis"]
