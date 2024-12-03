package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/mtvarkovsky/go-mapreduce/pkg/config"
	"github.com/mtvarkovsky/go-mapreduce/pkg/events"
	"github.com/mtvarkovsky/go-mapreduce/pkg/ids"
	"github.com/mtvarkovsky/go-mapreduce/pkg/logger"
	"github.com/mtvarkovsky/go-mapreduce/pkg/mapreduce"
	"github.com/mtvarkovsky/go-mapreduce/pkg/repository"
	"github.com/mtvarkovsky/go-mapreduce/pkg/service/mocks"
	"github.com/mtvarkovsky/go-mapreduce/pkg/utils"
	"github.com/stretchr/testify/assert"
)

type (
	testCoordinator struct {
		*Coordinator
		mockRepo     *mocks.MockRepository
		mockProducer *mocks.MockProducer
		mockIDGen    *mocks.MockIDGenerator
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

func newTestCoordinator(t *testing.T) *testCoordinator {
	ctrl := gomock.NewController(t)
	cfg := config.Coordinator{
		CheckForTasksToRescheduleEvery: time.Microsecond,
		RescheduleInProgressTasksAfter: time.Minute * 10,
	}
	mockRepo := mocks.NewMockRepository(ctrl)
	mockProducer := mocks.NewMockProducer(ctrl)
	mockIDGen := mocks.NewMockIDGenerator(ctrl)
	crdntr := NewCoordinator(context.Background(), cfg, mockRepo, mockIDGen, mockProducer, logger.NewTestZapLogger())
	return &testCoordinator{
		Coordinator:  crdntr,
		mockRepo:     mockRepo,
		mockProducer: mockProducer,
		mockIDGen:    mockIDGen,
	}
}

func TestCoordinator_CreateMapTask(t *testing.T) {
	crdntr := newTestCoordinator(t)

	crdntr.mockIDGen.EXPECT().
		GetID(gomock.Any(), ids.MapTask).
		Return(testMapTask.ID, nil)

	crdntr.mockRepo.EXPECT().
		CreateMapTask(gomock.Any(), mapreduce.MapTaskMatcher{
			MapTask: mapreduce.MapTask{
				ID:        testMapTask.ID,
				InputFile: testMapTask.InputFile,
				Status:    mapreduce.TaskCreated,
			},
		}).Return(nil)

	crdntr.mockProducer.EXPECT().
		Produce(gomock.Any(), events.Event{
			Type: events.MapTaskCreated,
		})

	task, err := crdntr.CreateMapTask(context.Background(), testMapTask.InputFile)
	assert.NoError(t, err)

	assert.Equal(t, testMapTask.ID, task.ID)
	assert.Equal(t, testMapTask.InputFile, task.InputFile)
	assert.Equal(t, mapreduce.TaskCreated, task.Status)
}

func TestCoordinator_CreateReduceTask(t *testing.T) {
	crdntr := newTestCoordinator(t)

	crdntr.mockIDGen.EXPECT().
		GetID(gomock.Any(), ids.ReduceTask).
		Return(testReduceTask.ID, nil)

	crdntr.mockRepo.EXPECT().
		CreateReduceTask(gomock.Any(), mapreduce.ReduceTaskMatcher{
			ReduceTask: mapreduce.ReduceTask{
				ID:         testReduceTask.ID,
				InputFiles: testReduceTask.InputFiles,
				Status:     mapreduce.TaskCreated,
			},
		}).Return(nil)

	crdntr.mockProducer.EXPECT().
		Produce(gomock.Any(), events.Event{
			Type: events.ReduceTaskCreated,
		})

	task, err := crdntr.CreateReduceTask(context.Background(), testReduceTask.InputFiles...)
	assert.NoError(t, err)

	assert.Equal(t, testReduceTask.ID, task.ID)
	assert.Equal(t, testReduceTask.InputFiles, task.InputFiles)
	assert.Equal(t, mapreduce.TaskCreated, task.Status)
}

func TestCoordinator_GetMapTask(t *testing.T) {
	crdntr := newTestCoordinator(t)

	crdntr.mockRepo.EXPECT().
		QueryMapTasks(gomock.Any(), repository.Filter{
			Statuses: []mapreduce.TaskStatus{mapreduce.TaskCreated, mapreduce.TaskRescheduled},
			Limit:    utils.Pointer(uint32(1)),
		}).Return([]mapreduce.MapTask{testMapTask}, nil)

	crdntr.mockRepo.EXPECT().
		UpdateMapTask(gomock.Any(), mapreduce.MapTaskMatcher{
			MapTask: mapreduce.MapTask{
				ID:           testMapTask.ID,
				InputFile:    testMapTask.InputFile,
				Status:       mapreduce.TaskInProgress,
				CreatedAt:    testMapTask.CreatedAt,
				InProgressAt: utils.Pointer(time.Now()),
			},
		}).Return(nil)

	task, err := crdntr.GetMapTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, testMapTask.ID, task.ID)
	assert.Equal(t, testMapTask.InputFile, task.InputFile)
	assert.Equal(t, mapreduce.TaskInProgress, task.Status)
}

func TestCoordinator_GetReduceTask(t *testing.T) {
	crdntr := newTestCoordinator(t)

	crdntr.mockRepo.EXPECT().
		QueryReduceTasks(gomock.Any(), repository.Filter{
			Statuses: []mapreduce.TaskStatus{mapreduce.TaskCreated, mapreduce.TaskRescheduled},
			Limit:    utils.Pointer(uint32(1)),
		}).Return([]mapreduce.ReduceTask{testReduceTask}, nil)

	crdntr.mockRepo.EXPECT().
		UpdateReduceTask(gomock.Any(), mapreduce.ReduceTaskMatcher{
			ReduceTask: mapreduce.ReduceTask{
				ID:           testReduceTask.ID,
				InputFiles:   testReduceTask.InputFiles,
				Status:       mapreduce.TaskInProgress,
				CreatedAt:    testReduceTask.CreatedAt,
				InProgressAt: utils.Pointer(time.Now()),
			},
		}).Return(nil)

	task, err := crdntr.GetReduceTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, testReduceTask.ID, task.ID)
	assert.Equal(t, testReduceTask.InputFiles, task.InputFiles)
	assert.Equal(t, mapreduce.TaskInProgress, task.Status)
}

func TestCoordinator_Integration_ReportMapTask_TaskDone(t *testing.T) {
	crdntr := newTestCoordinator(t)
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	repo, cntnr := repository.NewTestMongoDBTasksRepository(t)
	defer cntnr.Terminate(context.Background())
	crdntr.repo = repo

	// create map task
	crdntr.mockIDGen.EXPECT().
		GetID(gomock.Any(), ids.MapTask).
		Return(testMapTask.ID, nil)
	crdntr.mockProducer.EXPECT().
		Produce(gomock.Any(), events.Event{
			Type: events.MapTaskCreated,
		})
	createdTask, err := crdntr.CreateMapTask(context.Background(), testMapTask.InputFile)
	assert.NoError(t, err)

	// check map task created
	mapTask, err := crdntr.GetMapTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, createdTask.ID, mapTask.ID)
	assert.Equal(t, createdTask.InputFile, mapTask.InputFile)
	assert.Equal(t, mapreduce.TaskInProgress, mapTask.Status)

	// report map task
	crdntr.mockIDGen.EXPECT().
		GetID(gomock.Any(), ids.ReduceTask).
		Return(testReduceTask.ID, nil)
	crdntr.mockProducer.EXPECT().
		Produce(gomock.Any(), events.Event{
			Type: events.ReduceTaskCreated,
		})
	err = crdntr.ReportMapTask(context.Background(), mapreduce.MapTaskResult{
		TaskID:      testMapTask.ID,
		OutputFiles: testReduceTask.InputFiles,
	})
	assert.NoError(t, err)

	// check map task was reported
	mapTask, err = repo.GetMapTask(context.Background(), testMapTask.ID)
	assert.NoError(t, err)
	assert.Equal(t, createdTask.ID, mapTask.ID)
	assert.Equal(t, createdTask.InputFile, mapTask.InputFile)
	assert.Equal(t, mapreduce.TaskDone, mapTask.Status)

	// check reduce task was created
	reduceTask, err := repo.GetReduceTask(context.Background(), testReduceTask.ID)
	assert.NoError(t, err)
	assert.Equal(t, testReduceTask.ID, reduceTask.ID)
	assert.Equal(t, testReduceTask.InputFiles, reduceTask.InputFiles)
	assert.Equal(t, mapreduce.TaskCreated, reduceTask.Status)
}

func TestCoordinator_Integration_ReportMapTask_TaskFailed(t *testing.T) {
	crdntr := newTestCoordinator(t)
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	repo, cntnr := repository.NewTestMongoDBTasksRepository(t)
	defer cntnr.Terminate(context.Background())
	crdntr.repo = repo

	// create map task
	crdntr.mockIDGen.EXPECT().
		GetID(gomock.Any(), ids.MapTask).
		Return(testMapTask.ID, nil)
	crdntr.mockProducer.EXPECT().
		Produce(gomock.Any(), events.Event{
			Type: events.MapTaskCreated,
		})
	createdTask, err := crdntr.CreateMapTask(context.Background(), testMapTask.InputFile)
	assert.NoError(t, err)

	// check map task created
	mapTask, err := crdntr.GetMapTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, createdTask.ID, mapTask.ID)
	assert.Equal(t, createdTask.InputFile, mapTask.InputFile)
	assert.Equal(t, mapreduce.TaskInProgress, mapTask.Status)

	// report map task
	err = crdntr.ReportMapTask(context.Background(), mapreduce.MapTaskResult{
		TaskID:      testMapTask.ID,
		OutputFiles: []string{},
		Error:       fmt.Errorf("map task failed"),
	})
	assert.NoError(t, err)

	// check map task was reported
	mapTask, err = repo.GetMapTask(context.Background(), testMapTask.ID)
	assert.NoError(t, err)
	assert.Equal(t, createdTask.ID, mapTask.ID)
	assert.Equal(t, createdTask.InputFile, mapTask.InputFile)
	assert.Equal(t, mapreduce.TaskFailed, mapTask.Status)

	// check reduce task was not created
	_, err = repo.GetReduceTask(context.Background(), testReduceTask.ID)
	assert.Error(t, err)
}

func TestCoordinator_ReportReduceTask_TaskDone(t *testing.T) {
	crdntr := newTestCoordinator(t)

	crdntr.mockRepo.EXPECT().
		GetReduceTask(gomock.Any(), testReduceTask.ID).
		Return(
			mapreduce.ReduceTask{
				ID:         testReduceTask.ID,
				InputFiles: testReduceTask.InputFiles,
				Status:     mapreduce.TaskInProgress,
			},
			nil,
		)

	crdntr.mockRepo.EXPECT().
		UpdateReduceTask(gomock.Any(), mapreduce.ReduceTaskMatcher{
			ReduceTask: mapreduce.ReduceTask{
				ID:         testReduceTask.ID,
				InputFiles: testReduceTask.InputFiles,
				Status:     mapreduce.TaskDone,
				DoneAt:     utils.Pointer(time.Now()),
			},
		}).Return(nil)

	err := crdntr.ReportReduceTask(
		context.Background(),
		mapreduce.ReduceTaskResult{
			TaskID:     testReduceTask.ID,
			OutputFile: "output.file",
			Error:      nil,
		},
	)
	assert.NoError(t, err)
}

func TestCoordinator_ReportReduceTask_TaskFailed(t *testing.T) {
	crdntr := newTestCoordinator(t)

	crdntr.mockRepo.EXPECT().
		GetReduceTask(gomock.Any(), testReduceTask.ID).
		Return(
			mapreduce.ReduceTask{
				ID:         testReduceTask.ID,
				InputFiles: testReduceTask.InputFiles,
				Status:     mapreduce.TaskInProgress,
			},
			nil,
		)

	crdntr.mockRepo.EXPECT().
		UpdateReduceTask(gomock.Any(), mapreduce.ReduceTaskMatcher{
			ReduceTask: mapreduce.ReduceTask{
				ID:         testReduceTask.ID,
				InputFiles: testReduceTask.InputFiles,
				Status:     mapreduce.TaskFailed,
				FailedAt:   utils.Pointer(time.Now()),
			},
		}).Return(nil)

	err := crdntr.ReportReduceTask(
		context.Background(),
		mapreduce.ReduceTaskResult{
			TaskID:     testReduceTask.ID,
			OutputFile: "",
			Error:      fmt.Errorf("reduce task failed"),
		},
	)
	assert.NoError(t, err)
}

func TestCoordinator_FlushCreatedTasksToWorkers(t *testing.T) {
	crdntr := newTestCoordinator(t)

	crdntr.mockRepo.EXPECT().
		QueryMapTasks(gomock.Any(), repository.Filter{
			Statuses: []mapreduce.TaskStatus{mapreduce.TaskCreated, mapreduce.TaskRescheduled},
		}).Return([]mapreduce.MapTask{testMapTask}, nil)

	crdntr.mockProducer.EXPECT().
		Produce(gomock.Any(), events.Event{
			Type: events.MapTaskCreated,
		}).Return(nil)

	crdntr.mockRepo.EXPECT().
		QueryReduceTasks(gomock.Any(), repository.Filter{
			Statuses: []mapreduce.TaskStatus{mapreduce.TaskCreated, mapreduce.TaskRescheduled},
		}).Return([]mapreduce.ReduceTask{testReduceTask}, nil)

	crdntr.mockProducer.EXPECT().
		Produce(gomock.Any(), events.Event{
			Type: events.ReduceTaskCreated,
		}).Return(nil)

	err := crdntr.FlushCreatedTasksToWorkers(context.Background())
	assert.NoError(t, err)
}

func TestCoordinator_MapTasksRescheduler(t *testing.T) {
	crdntr := newTestCoordinator(t)

	crdntr.mockRepo.EXPECT().
		Transaction(gomock.Any(), gomock.Any()).
		Return([]string{testMapTask.ID}, nil).
		MinTimes(1)
	crdntr.mockProducer.EXPECT().
		Produce(gomock.Any(), events.Event{
			Type: events.MapTaskCreated,
		}).Return(nil).
		MinTimes(1)

	go crdntr.MapTasksRescheduler()
	time.Sleep(time.Millisecond * 3)
}

func TestCoordinator_ReduceTasksRescheduler(t *testing.T) {
	crdntr := newTestCoordinator(t)

	crdntr.mockRepo.EXPECT().
		Transaction(gomock.Any(), gomock.Any()).
		Return([]string{testReduceTask.ID}, nil).
		MinTimes(1)
	crdntr.mockProducer.EXPECT().
		Produce(gomock.Any(), events.Event{
			Type: events.ReduceTaskCreated,
		}).Return(nil).
		MinTimes(1)

	go crdntr.ReduceTasksRescheduler()
	time.Sleep(time.Millisecond * 3)
}

func TestCoordinator_Integration_rescheduleMapTasks(t *testing.T) {
	crdntr := newTestCoordinator(t)
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	repo, cntnr := repository.NewTestMongoDBTasksRepository(t)
	defer cntnr.Terminate(context.Background())
	crdntr.repo = repo

	// create map task
	crdntr.mockIDGen.EXPECT().
		GetID(gomock.Any(), ids.MapTask).
		Return(testMapTask.ID, nil)
	crdntr.mockProducer.EXPECT().
		Produce(gomock.Any(), events.Event{
			Type: events.MapTaskCreated,
		})
	mapTask, err := crdntr.CreateMapTask(context.Background(), testMapTask.InputFile)
	assert.NoError(t, err)

	// make map task valid for rescheduling
	mapTask.Status = mapreduce.TaskInProgress
	inProgressAt := time.Now().Add(-crdntr.cfg.RescheduleInProgressTasksAfter)
	mapTask.InProgressAt = utils.Pointer(inProgressAt)
	err = repo.UpdateMapTask(context.Background(), mapTask)
	assert.NoError(t, err)

	// reschedule map tasks
	crdntr.mockProducer.EXPECT().
		Produce(gomock.Any(), events.Event{
			Type: events.MapTaskCreated,
		})
	err = crdntr.rescheduleMapTasks()
	assert.NoError(t, err)

	// check map task was rescheduled
	rescheduledTask, err := repo.GetMapTask(context.Background(), mapTask.ID)
	assert.NoError(t, err)
	assert.Equal(t, mapTask.ID, rescheduledTask.ID)
	assert.Equal(t, mapreduce.TaskRescheduled, rescheduledTask.Status)
	assert.NotNil(t, rescheduledTask.RescheduledAt)
}

func TestCoordinator_Integration_rescheduleReduceTasks(t *testing.T) {
	crdntr := newTestCoordinator(t)
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	repo, cntnr := repository.NewTestMongoDBTasksRepository(t)
	defer cntnr.Terminate(context.Background())
	crdntr.repo = repo

	// create reduce task
	crdntr.mockIDGen.EXPECT().
		GetID(gomock.Any(), ids.ReduceTask).
		Return(testMapTask.ID, nil)
	crdntr.mockProducer.EXPECT().
		Produce(gomock.Any(), events.Event{
			Type: events.ReduceTaskCreated,
		})
	reduceTask, err := crdntr.CreateReduceTask(context.Background(), testReduceTask.InputFiles...)
	assert.NoError(t, err)

	// make reduce task valid for rescheduling
	reduceTask.Status = mapreduce.TaskInProgress
	inProgressAt := time.Now().Add(-crdntr.cfg.RescheduleInProgressTasksAfter)
	reduceTask.InProgressAt = utils.Pointer(inProgressAt)
	err = repo.UpdateReduceTask(context.Background(), reduceTask)
	assert.NoError(t, err)

	// reschedule reduce tasks
	crdntr.mockProducer.EXPECT().
		Produce(gomock.Any(), events.Event{
			Type: events.ReduceTaskCreated,
		})
	err = crdntr.rescheduleReduceTasks()
	assert.NoError(t, err)

	// check reduce task was rescheduled
	rescheduledTask, err := repo.GetReduceTask(context.Background(), reduceTask.ID)
	assert.NoError(t, err)
	assert.Equal(t, reduceTask.ID, rescheduledTask.ID)
	assert.Equal(t, mapreduce.TaskRescheduled, rescheduledTask.Status)
	assert.NotNil(t, rescheduledTask.RescheduledAt)
}
