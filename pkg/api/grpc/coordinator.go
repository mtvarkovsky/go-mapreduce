package grpc

import (
	"context"
	"fmt"
	"github.com/mtvarkovsky/go-mapreduce/pkg/api/grpc/pb"
	"github.com/mtvarkovsky/go-mapreduce/pkg/errors"
	"github.com/mtvarkovsky/go-mapreduce/pkg/logger"
	"github.com/mtvarkovsky/go-mapreduce/pkg/mapreduce"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type coordinatorServer struct {
	pb.UnimplementedServiceServer
	coordinator mapreduce.Coordinator
	log         logger.Logger
}

func NewCoordinator(coordinator mapreduce.Coordinator, log logger.Logger) pb.ServiceServer {
	return &coordinatorServer{
		coordinator: coordinator,
		log:         log.Logger("CoordinatorGrpcServer"),
	}
}

func (c *coordinatorServer) CreateMapTask(ctx context.Context, task *pb.NewMapTask) (*pb.MapTask, error) {
	if task == nil {
		return nil, status.Error(codes.InvalidArgument, "request can't be nil")
	}
	t, err := c.coordinator.CreateMapTask(ctx, task.InputFile)
	if err != nil {
		return nil, errors.ToGrpcError(err)
	}
	return &pb.MapTask{
		Id:        t.ID,
		InputFile: t.InputFile,
	}, nil
}

func (c *coordinatorServer) CreateReduceTask(ctx context.Context, task *pb.NewReduceTask) (*pb.ReduceTask, error) {
	if task == nil {
		return nil, status.Error(codes.InvalidArgument, "request can't be nil")
	}
	t, err := c.coordinator.CreateReduceTask(ctx, task.InputFiles...)
	if err != nil {
		return nil, errors.ToGrpcError(err)
	}
	return &pb.ReduceTask{
		Id:         t.ID,
		InputFiles: t.InputFiles,
	}, nil
}

func (c *coordinatorServer) GetMapTask(ctx context.Context, empty *emptypb.Empty) (*pb.MapTask, error) {
	task, err := c.coordinator.GetMapTask(ctx)
	if err != nil {
		return nil, errors.ToGrpcError(err)
	}
	return &pb.MapTask{
		Id:        task.ID,
		InputFile: task.InputFile,
	}, nil
}

func (c *coordinatorServer) GetReduceTask(ctx context.Context, empty *emptypb.Empty) (*pb.ReduceTask, error) {
	task, err := c.coordinator.GetReduceTask(ctx)
	if err != nil {
		return nil, errors.ToGrpcError(err)
	}
	return &pb.ReduceTask{
		Id:         task.ID,
		InputFiles: task.InputFiles,
	}, nil
}

func (c *coordinatorServer) ReportMapTaskResult(ctx context.Context, taskResult *pb.MapTaskResult) (*emptypb.Empty, error) {
	if taskResult == nil {
		return nil, status.Error(codes.InvalidArgument, "request can't be nil")
	}
	tr := mapreduce.MapTaskResult{
		TaskID:      taskResult.TaskId,
		OutputFiles: taskResult.OutputFiles,
	}
	if taskResult.Error != nil {
		tr.Error = fmt.Errorf("%s", *taskResult.Error)
	}
	err := c.coordinator.ReportMapTask(ctx, tr)
	if err != nil {
		return nil, errors.ToGrpcError(err)
	}
	return &emptypb.Empty{}, nil
}

func (c *coordinatorServer) ReportReduceTaskResult(ctx context.Context, taskResult *pb.ReduceTaskResult) (*emptypb.Empty, error) {
	if taskResult == nil {
		return nil, status.Error(codes.InvalidArgument, "request can't be nil")
	}
	tr := mapreduce.ReduceTaskResult{
		TaskID:     taskResult.TaskId,
		OutputFile: taskResult.OutputFile,
	}
	if taskResult.Error != nil {
		tr.Error = fmt.Errorf("%s", *taskResult.Error)
	}
	err := c.coordinator.ReportReduceTask(ctx, tr)
	if err != nil {
		return nil, errors.ToGrpcError(err)
	}
	return &emptypb.Empty{}, nil
}

func (c *coordinatorServer) FlushCreatedTasksToWorkers(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	err := c.coordinator.FlushCreatedTasksToWorkers(ctx)
	if err != nil {
		return nil, errors.ToGrpcError(err)
	}
	return &emptypb.Empty{}, nil
}
