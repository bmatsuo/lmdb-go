
.PHONY: all test bin

bin:
	mkdir -p bin
	GOBIN=${PWD}/bin go install ./exp/cmd/...
	GOBIN=${PWD}/bin go install ./cmd/...

test: bin
	go test ./...

check:
	find . -name '*.go' | xargs goimports -d | tee /dev/stderr | wc -l | xargs test 0 -eq
	golint ./... | tee /dev/stderr | wc -l | xargs test 0 -ne
