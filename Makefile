
.PHONY: all test bin

bin:
	mkdir -p bin
	GOBIN=${PWD}/bin go install ./exp/cmd/...
	GOBIN=${PWD}/bin go install ./cmd/...

test: bin
	go test ./...
