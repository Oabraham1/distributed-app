server:
	go run cmd/server/main.go

download-protobuf:
	wget https://github.com/protocolbuffers/protobuf/releases/download/v3.9.0/protoc-3.9.0-osx-x86_64.zip

unzip-protobuf:
	sudo unzip protoc-3.9.0-osx-x86_64.zip -d /usr/local/protobuf

install-protobuf-runtime:
	go get google.golang.org/protobuf/...@v1.25.0

compile-protobuf:
	protoc api/v1/*.proto \
			--go_out=. \
			--go_opt=paths=source_relative \
			--proto_path=.

test-protobuf:
	go test -race ./...

install-protoc-gen-go:
	brew install protoc-gen-go


































