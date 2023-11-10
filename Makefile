build-release:
	mkdir -p ./build/release/
	go build -ldflags "-s -w" -o "./build/release/app" ./cmd/reversedns/