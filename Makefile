
.PHONY: all test bin

bin:
	mkdir -p bin
	GOBIN=bin go install ./exp/cmd/...

test: bin
	go test ./...
