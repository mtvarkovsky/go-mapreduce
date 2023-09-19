package errors

import (
	"errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrorIs = errors.Is

	ErrCreateTask       = errors.New("can't create task")
	ErrTaskDoesNotExist = errors.New("can't get task, task does not exist")
	ErrReportTask       = errors.New("can't report task, task must have `InProgress` status")
	ErrInternal         = errors.New("internal error")
	ErrNoTasks          = errors.New("can't find any tasks")
)

func ToGrpcError(err error) error {
	if ErrorIs(err, ErrTaskDoesNotExist) {
		return status.Error(codes.NotFound, "task not found")
	}
	if ErrorIs(err, ErrCreateTask) {
		return status.Error(codes.InvalidArgument, "can't create task")
	}
	if ErrorIs(err, ErrReportTask) {
		return status.Error(codes.InvalidArgument, "can't create task")
	}
	if ErrorIs(err, ErrInternal) {
		return status.Error(codes.Internal, "internal error")
	}
	return status.Error(codes.Internal, "internal error")
}
