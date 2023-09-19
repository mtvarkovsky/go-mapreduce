package grpc

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func NewServer() *grpc.Server {
	gRPCServer := grpc.NewServer()

	reflection.Register(gRPCServer)
	return gRPCServer
}
