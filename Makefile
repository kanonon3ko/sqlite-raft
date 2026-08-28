.PHONY: gen build buildctl test run clean

gen:
	protoc -I . --go_out=. --go_opt=module=sqlraft --go-grpc_out=. --go-grpc_opt=module=sqlraft proto/sqlraft.proto proto/log.proto proto/raft.proto

build:
	go build -o bin/sqlraftd ./cmd/sqlraftd

buildctl:
	go build -o bin/sqlraftctl ./cmd/sqlraftctl

test:
	go test ./...

run:
	go run ./cmd/sqlraftd -id 0 -addr 127.0.0.1:50051

clean:
	rm -rf bin
