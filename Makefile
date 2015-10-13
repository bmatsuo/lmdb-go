
.PHONY: all test bin

bin:
	mkdir -p bin
	GOBIN=bin go install ./exp/cmd/...
	GOBIN=bin go install ./cmd/...

test: bin
	go test ./...
