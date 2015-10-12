
.PHONY: all test bin

all: test bin

test:
	go test ./...

bin:
	mkdir -p bin
	GOBIN=bin go install ./exp/cmd/...
