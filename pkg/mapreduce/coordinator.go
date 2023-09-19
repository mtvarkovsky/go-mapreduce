//go:generate mockgen -source coordinator.go -destination mocks/coordinator_mock.go -package mocks Coordinator

package mapreduce

import (
	"context"
)

type (
	Coordinator interface {
		CreateMapTask(ctx context.Context, inputFile string) (MapTask, error)
		CreateReduceTask(ctx context.Context, inputFiles ...string) (ReduceTask, error)

		GetMapTask(ctx context.Context) (MapTask, error)
		GetReduceTask(ctx context.Context) (ReduceTask, error)

		ReportMapTask(ctx context.Context, taskResult MapTaskResult) error
		ReportReduceTask(ctx context.Context, taskResult ReduceTaskResult) error

		FlushCreatedTasksToWorkers(ctx context.Context) error

		MapTasksRescheduler()
		ReduceTasksRescheduler()
	}
)
