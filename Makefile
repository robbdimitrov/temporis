.PHONY: all build test tidy fmt docker clean run

APP_NAME=temporis
IMAGE_NAME=temporis:1.0.0

all: build

build:
	cd src && go build -o ../$(APP_NAME) ./cmd/server

run:
	cd src && go run ./cmd/server

test:
	cd src && go test -v ./...

tidy:
	cd src && go mod tidy

fmt:
	cd src && go fmt ./...

docker:
	cd src && docker build -t $(IMAGE_NAME) .

clean:
	rm -f $(APP_NAME)
