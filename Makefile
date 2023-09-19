COORDINATOR_GRPC_API_DIR = ./api/coordinator/grpc
COORDINATOR_GRPC_API_OUT = ./pkg/api/grpc/pb
MODULE_NAME=github.com/mtvarkosky/go-mapreduce/pkg/api/grpc/pb

BUILD_DIR=./build

BUILD_SRC_COORDINATOR=./cmd/coordinator
BUILD_OUT_COORDINATOR=$(BUILD_DIR)/coordinator

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
	docker-compose -f ./deployments/docker-compose.yaml up