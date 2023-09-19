package grpc

import (
	"context"
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/mtvarkovsky/go-mapreduce/pkg/mapreduce"
	"github.com/mtvarkovsky/go-mapreduce/pkg/mapreduce/mocks"
	"github.com/mtvarkovsky/go-mapreduce/pkg/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"
	"testing"
	"time"
)

type (
	testCoordinatorServer struct {
		*coordinatorServer
		mockCoordinator *mocks.MockCoordinator
	}
)

var (
	testMapTask = mapreduce.MapTask{
		ID:        "testMapTaskID",
		InputFile: "test.test",
		Status:    mapreduce.TaskCreated,
		CreatedAt: time.Now(),
	}

	testReduceTask = mapreduce.ReduceTask{
		ID:         "testReduceTaskID",
		InputFiles: []string{"test.test"},
		Status:     mapreduce.TaskCreated,
		CreatedAt:  time.Now(),
	}
)

func newTestCoordinatorServer(t *testing.T) *testCoordinatorServer {
	ctrl := gomock.NewController(t)
	mockCoordinator := mocks.NewMockCoordinator(ctrl)
	srvr := &coordinatorServer{coordinator: mockCoordinator}
	return &testCoordinatorServer{
		coordinatorServer: srvr,
		mockCoordinator:   mockCoordinator,
	}
}

func TestCoordinatorServer_CreateMapTask(t *testing.T) {
	crdntr := newTestCoordinatorServer(t)
	crdntr.mockCoordinator.EXPECT().
		CreateMapTask(gomock.Any(), testMapTask.InputFile).
		Return(testMapTask, nil)
	task, err := crdntr.CreateMapTask(context.Background(), &NewMapTask{
		InputFile: testMapTask.InputFile,
	})
	assert.NoError(t, err)
	expected := &MapTask{
		Id:        testMapTask.ID,
		InputFile: testMapTask.InputFile,
	}
	assert.Equal(t, expected, task)
}

func TestCoordinatorServer_CreateReduceTask(t *testing.T) {
	crdntr := newTestCoordinatorServer(t)
	crdntr.mockCoordinator.EXPECT().
		CreateReduceTask(gomock.Any(), testReduceTask.InputFiles).
		Return(testReduceTask, nil)
	task, err := crdntr.CreateReduceTask(context.Background(), &NewReduceTask{
		InputFiles: testReduceTask.InputFiles,
	})
	assert.NoError(t, err)
	expected := &ReduceTask{
		Id:         testReduceTask.ID,
		InputFiles: testReduceTask.InputFiles,
	}
	assert.Equal(t, expected, task)
}

func TestCoordinatorServer_GetMapTask(t *testing.T) {
	crdntr := newTestCoordinatorServer(t)
	crdntr.mockCoordinator.EXPECT().
		GetMapTask(gomock.Any()).
		Return(testMapTask, nil)
	task, err := crdntr.GetMapTask(context.Background(), &emptypb.Empty{})
	assert.NoError(t, err)
	expected := &MapTask{
		Id:        testMapTask.ID,
		InputFile: testMapTask.InputFile,
	}
	assert.Equal(t, expected, task)
}

func TestCoordinatorServer_GetReduceTask(t *testing.T) {
	crdntr := newTestCoordinatorServer(t)
	crdntr.mockCoordinator.EXPECT().
		GetReduceTask(gomock.Any()).
		Return(testReduceTask, nil)
	task, err := crdntr.GetReduceTask(context.Background(), &emptypb.Empty{})
	assert.NoError(t, err)
	expected := &ReduceTask{
		Id:         testReduceTask.ID,
		InputFiles: testReduceTask.InputFiles,
	}
	assert.Equal(t, expected, task)
}

func TestCoordinatorServer_ReportMapTaskResult(t *testing.T) {
	crdntr := newTestCoordinatorServer(t)
	crdntr.mockCoordinator.EXPECT().
		ReportMapTask(gomock.Any(), mapreduce.MapTaskResultMatcher{
			MapTaskResult: mapreduce.MapTaskResult{
				TaskID: testMapTask.ID,
				Error:  fmt.Errorf("map task error"),
			},
		}).Return(nil)
	_, err := crdntr.ReportMapTaskResult(context.Background(), &MapTaskResult{
		TaskId:      testMapTask.ID,
		OutputFiles: []string{},
		Error:       utils.Pointer("map task error"),
	})
	assert.NoError(t, err)
}

func TestCoordinatorServer_ReportReduceTaskResult(t *testing.T) {
	crdntr := newTestCoordinatorServer(t)
	crdntr.mockCoordinator.EXPECT().
		ReportReduceTask(gomock.Any(), mapreduce.ReduceTaskResultMatcher{
			ReduceTaskResult: mapreduce.ReduceTaskResult{
				TaskID:     testReduceTask.ID,
				OutputFile: "output.file",
				Error:      nil,
			},
		}).Return(nil)
	_, err := crdntr.ReportReduceTaskResult(context.Background(), &ReduceTaskResult{
		TaskId:     testReduceTask.ID,
		OutputFile: "output.file",
		Error:      nil,
	})
	assert.NoError(t, err)
}

func TestCoordinatorServer_FlushCreatedTasksToWorkers(t *testing.T) {
	crdntr := newTestCoordinatorServer(t)
	crdntr.mockCoordinator.EXPECT().
		FlushCreatedTasksToWorkers(gomock.Any()).
		Return(nil)

	_, err := crdntr.FlushCreatedTasksToWorkers(context.Background(), &emptypb.Empty{})
	assert.NoError(t, err)
}
