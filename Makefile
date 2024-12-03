COORDINATOR_GRPC_API_DIR = ./api/coordinator/grpc
COORDINATOR_GRPC_API_OUT = ./pkg/api/grpc/pb
MODULE_NAME=github.com/mtvarkosky/go-mapreduce/pkg/api/grpc/pb

BUILD_DIR=./build

BUILD_SRC_COORDINATOR=./cmd/coordinator
BUILD_OUT_COORDINATOR=$(BUILD_DIR)/coordinator

COVER_DIR=$(BUILD_DIR)/coverage

.PHONY: protogen
protoc: ## install tools to generate code stubs from .proto files
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

.PHONY: genproto
genproto: protoc ## generate code stubs from .proto files
	protoc --proto_path=${COORDINATOR_GRPC_API_DIR} --go_out=${COORDINATOR_GRPC_API_OUT} --go-grpc_out=${COORDINATOR_GRPC_API_OUT} --go_opt=Mcoordinator.proto=${MODULE_NAME} --go-grpc_opt=Mcoordinator.proto=${MODULE_NAME} ${COORDINATOR_GRPC_API_DIR}/coordinator.proto

.PHONY: build-coordinator
build-coordinator: ## builds the coordinator executable and places it to ./build/
	go build -o ${BUILD_OUT_COORDINATOR} ${BUILD_SRC_COORDINATOR}

.PHONY: docker-build-coordinator
docker-build-coordinator: ## build coordinator docker image
	DOCKER_BUILDKIT=1 docker build --ssh default . -f ./deployments/Dockerfile_Coordinator -t coordinator:latest

.PHONY: docker-run-coordinator
docker-run-coordinator: ## runs coordinator docker image
	docker compose -f ./deployments/docker-compose.yaml up

.PHONY: gomock
gomock: ## install mockgen dependency
	go get github.com/golang/mock/mockgen
	go install github.com/golang/mock/mockgen

.PHONY: mocks
mocks: gomock ## generate gomock files
	go generate ./...

.PHONY: clean-mocks
clean-mocks:
	find . -name '*_mock.go' -delete

.PHONY: install-lint
install-lint: ## install golangci dependency
	go get github.com/golangci/golangci-lint/cmd/golangci-lint
	go install github.com/golangci/golangci-lint/cmd/golangci-lint

.PHONY: lint
lint: install-lint ## run golang linter against the source code
	golangci-lint run ./... --fix

.PHONY: test
test: mocks ## run unit-tests
	mkdir -p ${COVER_DIR}; CGO_ENABLED=1; go test -coverprofile=${COVER_DIR}/coverage.out ./...
	go tool cover -html=${COVER_DIR}/coverage.out -o ${COVER_DIR}/coverage.html

# generate help info from comments: thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help: ## help information about make commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

