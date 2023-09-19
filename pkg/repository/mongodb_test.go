package repository

import (
	"context"
	"github.com/mtvarkovsky/go-mapreduce/pkg/mapreduce"
	"github.com/mtvarkovsky/go-mapreduce/pkg/utils"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"testing"
	"time"
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

func TestMongoDBTasksRepository_Integration_CreateMapTask(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	repo, cntnr := NewTestMongoDBTasksRepository(t)
	defer cntnr.Terminate(context.Background())
	err := repo.CreateMapTask(context.Background(), testMapTask)
	assert.NoError(t, err)
}

func TestMongoDBTasksRepository_Integration_CreateReduceTask(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	repo, cntnr := NewTestMongoDBTasksRepository(t)
	defer cntnr.Terminate(context.Background())
	err := repo.CreateReduceTask(context.Background(), testReduceTask)
	assert.NoError(t, err)
}

func TestMongoDBTasksRepository_Integration_GetMapTask(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	repo, cntnr := NewTestMongoDBTasksRepository(t)
	defer cntnr.Terminate(context.Background())
	err := repo.CreateMapTask(context.Background(), testMapTask)
	assert.NoError(t, err)
	task, err := repo.GetMapTask(context.Background(), testMapTask.ID)
	assert.NoError(t, err)
	assert.Equal(t, testMapTask.ID, task.ID)
	assert.Equal(t, testMapTask.Status, task.Status)
	assert.Equal(t, testMapTask.InputFile, task.InputFile)
	assert.WithinDuration(t, testMapTask.CreatedAt, task.CreatedAt, time.Millisecond)
	assert.Nil(t, task.InProgressAt)
	assert.Nil(t, task.FailedAt)
	assert.Nil(t, task.DoneAt)
	assert.Nil(t, task.RescheduledAt)
}

func TestMongoDBTasksRepository_Integration_GetReduceTask(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	repo, cntnr := NewTestMongoDBTasksRepository(t)
	defer cntnr.Terminate(context.Background())
	err := repo.CreateReduceTask(context.Background(), testReduceTask)
	assert.NoError(t, err)
	task, err := repo.GetReduceTask(context.Background(), testReduceTask.ID)
	assert.NoError(t, err)
	assert.Equal(t, testReduceTask.ID, task.ID)
	assert.Equal(t, testReduceTask.Status, task.Status)
	assert.Equal(t, testReduceTask.InputFiles, task.InputFiles)
	assert.WithinDuration(t, testReduceTask.CreatedAt, task.CreatedAt, time.Millisecond)
	assert.Nil(t, task.InProgressAt)
	assert.Nil(t, task.FailedAt)
	assert.Nil(t, task.DoneAt)
	assert.Nil(t, task.RescheduledAt)
}

func TestMongoDBTasksRepository_Integration_UpdateMapTask(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	repo, cntnr := NewTestMongoDBTasksRepository(t)
	defer cntnr.Terminate(context.Background())
	err := repo.CreateMapTask(context.Background(), testMapTask)
	assert.NoError(t, err)
	task, err := repo.GetMapTask(context.Background(), testMapTask.ID)
	assert.NoError(t, err)
	now := time.Now()
	task.Status = mapreduce.TaskDone
	task.InProgressAt = &now
	task.FailedAt = &now
	task.DoneAt = &now
	task.RescheduledAt = &now
	err = repo.UpdateMapTask(context.Background(), task)
	assert.NoError(t, err)
	updatedTask, err := repo.GetMapTask(context.Background(), task.ID)
	assert.NoError(t, err)
	assert.Equal(t, task.ID, updatedTask.ID)
	assert.Equal(t, task.Status, updatedTask.Status)
	assert.Equal(t, task.InputFile, updatedTask.InputFile)
	assert.WithinDuration(t, task.CreatedAt, updatedTask.CreatedAt, time.Millisecond)
	assert.WithinDuration(t, *task.InProgressAt, *updatedTask.InProgressAt, time.Millisecond)
	assert.WithinDuration(t, *task.FailedAt, *updatedTask.FailedAt, time.Millisecond)
	assert.WithinDuration(t, *task.DoneAt, *updatedTask.DoneAt, time.Millisecond)
	assert.WithinDuration(t, *task.RescheduledAt, *updatedTask.RescheduledAt, time.Millisecond)
}

func TestMongoDBTasksRepository_Integration_UpdateReduceTask(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	repo, cntnr := NewTestMongoDBTasksRepository(t)
	defer cntnr.Terminate(context.Background())
	err := repo.CreateReduceTask(context.Background(), testReduceTask)
	assert.NoError(t, err)
	task, err := repo.GetReduceTask(context.Background(), testReduceTask.ID)
	assert.NoError(t, err)
	now := time.Now()
	task.Status = mapreduce.TaskDone
	task.InProgressAt = &now
	task.FailedAt = &now
	task.DoneAt = &now
	task.RescheduledAt = &now
	err = repo.UpdateReduceTask(context.Background(), task)
	assert.NoError(t, err)
	updatedTask, err := repo.GetReduceTask(context.Background(), task.ID)
	assert.NoError(t, err)
	assert.Equal(t, task.ID, updatedTask.ID)
	assert.Equal(t, task.Status, updatedTask.Status)
	assert.Equal(t, task.InputFiles, updatedTask.InputFiles)
	assert.WithinDuration(t, task.CreatedAt, updatedTask.CreatedAt, time.Millisecond)
	assert.WithinDuration(t, *task.InProgressAt, *updatedTask.InProgressAt, time.Millisecond)
	assert.WithinDuration(t, *task.FailedAt, *updatedTask.FailedAt, time.Millisecond)
	assert.WithinDuration(t, *task.DoneAt, *updatedTask.DoneAt, time.Millisecond)
	assert.WithinDuration(t, *task.RescheduledAt, *updatedTask.RescheduledAt, time.Millisecond)
}

func TestMongoDBTasksRepository_Integration_UpdateMapTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	repo, cntnr := NewTestMongoDBTasksRepository(t)
	defer cntnr.Terminate(context.Background())
	now := time.Now()
	task1 := mapreduce.MapTask{
		ID:           "mapTask1",
		InputFile:    "1.test",
		Status:       mapreduce.TaskInProgress,
		CreatedAt:    now,
		InProgressAt: &now,
	}
	task2 := mapreduce.MapTask{
		ID:           "mapTask2",
		InputFile:    "2.test",
		Status:       mapreduce.TaskInProgress,
		CreatedAt:    now,
		InProgressAt: &now,
	}
	task3 := mapreduce.MapTask{
		ID:           "mapTask3",
		InputFile:    "3.test",
		Status:       mapreduce.TaskInProgress,
		CreatedAt:    now,
		InProgressAt: &now,
	}
	task4 := mapreduce.MapTask{
		ID:           "mapTask4",
		InputFile:    "4.test",
		Status:       mapreduce.TaskInProgress,
		CreatedAt:    now,
		InProgressAt: &now,
	}
	err := repo.CreateMapTask(context.Background(), task1)
	assert.NoError(t, err)
	err = repo.CreateMapTask(context.Background(), task2)
	assert.NoError(t, err)
	err = repo.CreateMapTask(context.Background(), task3)
	assert.NoError(t, err)
	err = repo.CreateMapTask(context.Background(), task4)
	assert.NoError(t, err)

	now = time.Now()
	err = repo.UpdateMapTasks(
		context.Background(),
		[]string{task1.ID, task2.ID, task3.ID},
		UpdateFields{
			Status:        utils.Pointer(mapreduce.TaskRescheduled),
			RescheduledAt: &now,
		},
	)
	assert.NoError(t, err)

	task1Updated, err := repo.GetMapTask(context.Background(), task1.ID)
	assert.NoError(t, err)
	task2Updated, err := repo.GetMapTask(context.Background(), task2.ID)
	assert.NoError(t, err)
	task3Updated, err := repo.GetMapTask(context.Background(), task3.ID)
	assert.NoError(t, err)
	task4NotUpdated, err := repo.GetMapTask(context.Background(), task4.ID)
	assert.NoError(t, err)

	assert.Equal(t, task1.ID, task1Updated.ID)
	assert.Equal(t, mapreduce.TaskRescheduled, task1Updated.Status)
	assert.Equal(t, task1.InputFile, task1Updated.InputFile)
	assert.WithinDuration(t, task1.CreatedAt, task1Updated.CreatedAt, time.Millisecond)
	assert.WithinDuration(t, now, *task1Updated.RescheduledAt, time.Millisecond)
	assert.WithinDuration(t, *task1.InProgressAt, *task1Updated.InProgressAt, time.Second)
	assert.Equal(t, task1.FailedAt, task1Updated.FailedAt)
	assert.Equal(t, task1.DoneAt, task1Updated.DoneAt)

	assert.Equal(t, task2.ID, task2Updated.ID)
	assert.Equal(t, mapreduce.TaskRescheduled, task2Updated.Status)
	assert.Equal(t, task2.InputFile, task2Updated.InputFile)
	assert.WithinDuration(t, task2.CreatedAt, task2Updated.CreatedAt, time.Millisecond)
	assert.WithinDuration(t, now, *task2Updated.RescheduledAt, time.Millisecond)
	assert.WithinDuration(t, *task2.InProgressAt, *task2Updated.InProgressAt, time.Second)
	assert.Equal(t, task2.FailedAt, task2Updated.FailedAt)
	assert.Equal(t, task2.DoneAt, task2Updated.DoneAt)

	assert.Equal(t, task3.ID, task3Updated.ID)
	assert.Equal(t, mapreduce.TaskRescheduled, task3Updated.Status)
	assert.Equal(t, task3.InputFile, task3Updated.InputFile)
	assert.WithinDuration(t, task3.CreatedAt, task3Updated.CreatedAt, time.Millisecond)
	assert.WithinDuration(t, now, *task3Updated.RescheduledAt, time.Millisecond)
	assert.WithinDuration(t, *task3.InProgressAt, *task3Updated.InProgressAt, time.Second)
	assert.Equal(t, task3.FailedAt, task3Updated.FailedAt)
	assert.Equal(t, task3.DoneAt, task3Updated.DoneAt)

	assert.Equal(t, task4.ID, task4NotUpdated.ID)
	assert.Equal(t, task4.Status, task4NotUpdated.Status)
	assert.Equal(t, task4.InputFile, task4NotUpdated.InputFile)
	assert.WithinDuration(t, task4.CreatedAt, task4NotUpdated.CreatedAt, time.Millisecond)
	assert.Equal(t, task4NotUpdated.RescheduledAt, task4NotUpdated.RescheduledAt)
	assert.WithinDuration(t, *task4.InProgressAt, *task4NotUpdated.InProgressAt, time.Second)
	assert.Equal(t, task3.FailedAt, task3Updated.FailedAt)
	assert.Equal(t, task3.DoneAt, task3Updated.DoneAt)
}

func TestMongoDBTasksRepository_Integration_UpdateReduceTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	repo, cntnr := NewTestMongoDBTasksRepository(t)
	defer cntnr.Terminate(context.Background())
	now := time.Now()
	task1 := mapreduce.ReduceTask{
		ID:           "reduceTask1",
		InputFiles:   []string{"1.test"},
		Status:       mapreduce.TaskInProgress,
		CreatedAt:    now,
		InProgressAt: &now,
	}
	task2 := mapreduce.ReduceTask{
		ID:           "reduceTask2",
		InputFiles:   []string{"2.test"},
		Status:       mapreduce.TaskInProgress,
		CreatedAt:    now,
		InProgressAt: &now,
	}
	task3 := mapreduce.ReduceTask{
		ID:           "reduceTask3",
		InputFiles:   []string{"3.test"},
		Status:       mapreduce.TaskInProgress,
		CreatedAt:    now,
		InProgressAt: &now,
	}
	task4 := mapreduce.ReduceTask{
		ID:           "reduceTask4",
		InputFiles:   []string{"4.test"},
		Status:       mapreduce.TaskInProgress,
		CreatedAt:    now,
		InProgressAt: &now,
	}
	err := repo.CreateReduceTask(context.Background(), task1)
	assert.NoError(t, err)
	err = repo.CreateReduceTask(context.Background(), task2)
	assert.NoError(t, err)
	err = repo.CreateReduceTask(context.Background(), task3)
	assert.NoError(t, err)
	err = repo.CreateReduceTask(context.Background(), task4)
	assert.NoError(t, err)

	now = time.Now()
	err = repo.UpdateReduceTasks(
		context.Background(),
		[]string{task1.ID, task2.ID, task3.ID},
		UpdateFields{
			Status:        utils.Pointer(mapreduce.TaskRescheduled),
			RescheduledAt: &now,
		},
	)
	assert.NoError(t, err)

	task1Updated, err := repo.GetReduceTask(context.Background(), task1.ID)
	assert.NoError(t, err)
	task2Updated, err := repo.GetReduceTask(context.Background(), task2.ID)
	assert.NoError(t, err)
	task3Updated, err := repo.GetReduceTask(context.Background(), task3.ID)
	assert.NoError(t, err)
	task4NotUpdated, err := repo.GetReduceTask(context.Background(), task4.ID)
	assert.NoError(t, err)

	assert.Equal(t, task1.ID, task1Updated.ID)
	assert.Equal(t, mapreduce.TaskRescheduled, task1Updated.Status)
	assert.Equal(t, task1.InputFiles, task1Updated.InputFiles)
	assert.WithinDuration(t, task1.CreatedAt, task1Updated.CreatedAt, time.Millisecond)
	assert.WithinDuration(t, now, *task1Updated.RescheduledAt, time.Millisecond)
	assert.WithinDuration(t, *task1.InProgressAt, *task1Updated.InProgressAt, time.Second)
	assert.Equal(t, task1.FailedAt, task1Updated.FailedAt)
	assert.Equal(t, task1.DoneAt, task1Updated.DoneAt)

	assert.Equal(t, task2.ID, task2Updated.ID)
	assert.Equal(t, mapreduce.TaskRescheduled, task2Updated.Status)
	assert.Equal(t, task2.InputFiles, task2Updated.InputFiles)
	assert.WithinDuration(t, task2.CreatedAt, task2Updated.CreatedAt, time.Millisecond)
	assert.WithinDuration(t, now, *task2Updated.RescheduledAt, time.Millisecond)
	assert.WithinDuration(t, *task2.InProgressAt, *task2Updated.InProgressAt, time.Second)
	assert.Equal(t, task2.FailedAt, task2Updated.FailedAt)
	assert.Equal(t, task2.DoneAt, task2Updated.DoneAt)

	assert.Equal(t, task3.ID, task3Updated.ID)
	assert.Equal(t, mapreduce.TaskRescheduled, task3Updated.Status)
	assert.Equal(t, task3.InputFiles, task3Updated.InputFiles)
	assert.WithinDuration(t, task3.CreatedAt, task3Updated.CreatedAt, time.Millisecond)
	assert.WithinDuration(t, now, *task3Updated.RescheduledAt, time.Millisecond)
	assert.WithinDuration(t, *task3.InProgressAt, *task3Updated.InProgressAt, time.Second)
	assert.Equal(t, task3.FailedAt, task3Updated.FailedAt)
	assert.Equal(t, task3.DoneAt, task3Updated.DoneAt)

	assert.Equal(t, task4.ID, task4NotUpdated.ID)
	assert.Equal(t, task4.Status, task4NotUpdated.Status)
	assert.Equal(t, task4.InputFiles, task4NotUpdated.InputFiles)
	assert.WithinDuration(t, task4.CreatedAt, task4NotUpdated.CreatedAt, time.Millisecond)
	assert.Equal(t, task4NotUpdated.RescheduledAt, task4NotUpdated.RescheduledAt)
	assert.WithinDuration(t, *task4.InProgressAt, *task4NotUpdated.InProgressAt, time.Second)
	assert.Equal(t, task3.FailedAt, task3Updated.FailedAt)
	assert.Equal(t, task3.DoneAt, task3Updated.DoneAt)
}

func TestMongoDBTasksRepository_Integration_QueryMapTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	repo, cntnr := NewTestMongoDBTasksRepository(t)
	defer cntnr.Terminate(context.Background())
	now := time.Now()
	task1 := mapreduce.MapTask{
		ID:           "mapTask1",
		InputFile:    "1.test",
		Status:       mapreduce.TaskInProgress,
		CreatedAt:    now.Add(time.Hour * 1),
		InProgressAt: utils.Pointer(now.Add(time.Hour * 24)),
	}
	task2 := mapreduce.MapTask{
		ID:           "mapTask2",
		InputFile:    "2.test",
		Status:       mapreduce.TaskInProgress,
		CreatedAt:    now.Add(time.Hour * 2),
		InProgressAt: utils.Pointer(now.Add(-time.Hour * 24)),
	}
	task3 := mapreduce.MapTask{
		ID:           "mapTask3",
		InputFile:    "3.test",
		Status:       mapreduce.TaskDone,
		CreatedAt:    now.Add(time.Hour * 3),
		InProgressAt: &now,
	}
	task4 := mapreduce.MapTask{
		ID:           "mapTask4",
		InputFile:    "4.test",
		Status:       mapreduce.TaskFailed,
		CreatedAt:    now.Add(time.Hour * 4),
		InProgressAt: &now,
	}
	err := repo.CreateMapTask(context.Background(), task1)
	assert.NoError(t, err)
	err = repo.CreateMapTask(context.Background(), task2)
	assert.NoError(t, err)
	err = repo.CreateMapTask(context.Background(), task3)
	assert.NoError(t, err)
	err = repo.CreateMapTask(context.Background(), task4)
	assert.NoError(t, err)

	tasks, err := repo.QueryMapTasks(
		context.Background(),
		Filter{
			OrderBy: utils.Pointer(OrderByCreatedAt),
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 4)
	assert.Equal(t, task1.ID, tasks[0].ID)
	assert.Equal(t, task2.ID, tasks[1].ID)
	assert.Equal(t, task3.ID, tasks[2].ID)
	assert.Equal(t, task4.ID, tasks[3].ID)

	tasks, err = repo.QueryMapTasks(
		context.Background(),
		Filter{
			OrderBy:        utils.Pointer(OrderByCreatedAt),
			OrderDirection: utils.Pointer(Descending),
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 4)
	assert.Equal(t, task1.ID, tasks[3].ID)
	assert.Equal(t, task2.ID, tasks[2].ID)
	assert.Equal(t, task3.ID, tasks[1].ID)
	assert.Equal(t, task4.ID, tasks[0].ID)

	tasks, err = repo.QueryMapTasks(
		context.Background(),
		Filter{
			OrderBy:        utils.Pointer(OrderByCreatedAt),
			OrderDirection: utils.Pointer(Descending),
			Limit:          utils.Pointer(uint32(1)),
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
	assert.Equal(t, task4.ID, tasks[0].ID)

	tasks, err = repo.QueryMapTasks(
		context.Background(),
		Filter{
			OrderBy:        utils.Pointer(OrderByCreatedAt),
			OrderDirection: utils.Pointer(Descending),
			Limit:          utils.Pointer(uint32(1)),
			Offset:         utils.Pointer(uint32(1)),
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
	assert.Equal(t, task3.ID, tasks[0].ID)

	tasks, err = repo.QueryMapTasks(
		context.Background(),
		Filter{
			OrderBy:        utils.Pointer(OrderByCreatedAt),
			OrderDirection: utils.Pointer(Descending),
			Limit:          utils.Pointer(uint32(1)),
			Offset:         utils.Pointer(uint32(2)),
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
	assert.Equal(t, task2.ID, tasks[0].ID)

	tasks, err = repo.QueryMapTasks(
		context.Background(),
		Filter{
			OrderBy:        utils.Pointer(OrderByCreatedAt),
			OrderDirection: utils.Pointer(Descending),
			Limit:          utils.Pointer(uint32(1)),
			Offset:         utils.Pointer(uint32(3)),
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
	assert.Equal(t, task1.ID, tasks[0].ID)

	tasks, err = repo.QueryMapTasks(
		context.Background(),
		Filter{
			OrderBy:        utils.Pointer(OrderByCreatedAt),
			OrderDirection: utils.Pointer(Ascending),
			IDs:            []string{task3.ID, task4.ID},
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 2)
	assert.Equal(t, task3.ID, tasks[0].ID)
	assert.Equal(t, task4.ID, tasks[1].ID)

	tasks, err = repo.QueryMapTasks(
		context.Background(),
		Filter{
			OrderBy: utils.Pointer(OrderByCreatedAt),
			Statuses: []mapreduce.TaskStatus{
				mapreduce.TaskDone,
				mapreduce.TaskFailed,
			},
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 2)
	assert.Equal(t, task3.ID, tasks[0].ID)
	assert.Equal(t, task4.ID, tasks[1].ID)

	tasks, err = repo.QueryMapTasks(
		context.Background(),
		Filter{
			OrderBy: utils.Pointer(OrderByCreatedAt),
			Statuses: []mapreduce.TaskStatus{
				mapreduce.TaskInProgress,
			},
			InProgressFor: utils.Pointer(time.Hour * 24),
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
	assert.Equal(t, task2.ID, tasks[0].ID)

	tasks, err = repo.QueryMapTasks(
		context.Background(),
		Filter{
			OrderBy: utils.Pointer(OrderByCreatedAt),
			Statuses: []mapreduce.TaskStatus{
				mapreduce.TaskInProgress,
			},
			InProgressFor: utils.Pointer(time.Hour * 100),
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 0)
}

func TestMongoDBTasksRepository_Integration_QueryReduceTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	repo, cntnr := NewTestMongoDBTasksRepository(t)
	defer cntnr.Terminate(context.Background())
	now := time.Now()
	task1 := mapreduce.ReduceTask{
		ID:           "reduceTask1",
		InputFiles:   []string{"1.test"},
		Status:       mapreduce.TaskInProgress,
		CreatedAt:    now.Add(time.Hour * 1),
		InProgressAt: utils.Pointer(now.Add(time.Hour * 24)),
	}
	task2 := mapreduce.ReduceTask{
		ID:           "reduceTask2",
		InputFiles:   []string{"2.test"},
		Status:       mapreduce.TaskInProgress,
		CreatedAt:    now.Add(time.Hour * 2),
		InProgressAt: utils.Pointer(now.Add(-time.Hour * 24)),
	}
	task3 := mapreduce.ReduceTask{
		ID:           "reduceTask3",
		InputFiles:   []string{"3.test"},
		Status:       mapreduce.TaskDone,
		CreatedAt:    now.Add(time.Hour * 3),
		InProgressAt: &now,
	}
	task4 := mapreduce.ReduceTask{
		ID:           "reduceTask4",
		InputFiles:   []string{"4.test"},
		Status:       mapreduce.TaskFailed,
		CreatedAt:    now.Add(time.Hour * 4),
		InProgressAt: &now,
	}
	err := repo.CreateReduceTask(context.Background(), task1)
	assert.NoError(t, err)
	err = repo.CreateReduceTask(context.Background(), task2)
	assert.NoError(t, err)
	err = repo.CreateReduceTask(context.Background(), task3)
	assert.NoError(t, err)
	err = repo.CreateReduceTask(context.Background(), task4)
	assert.NoError(t, err)

	tasks, err := repo.QueryReduceTasks(
		context.Background(),
		Filter{
			OrderBy: utils.Pointer(OrderByCreatedAt),
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 4)
	assert.Equal(t, task1.ID, tasks[0].ID)
	assert.Equal(t, task2.ID, tasks[1].ID)
	assert.Equal(t, task3.ID, tasks[2].ID)
	assert.Equal(t, task4.ID, tasks[3].ID)

	tasks, err = repo.QueryReduceTasks(
		context.Background(),
		Filter{
			OrderBy:        utils.Pointer(OrderByCreatedAt),
			OrderDirection: utils.Pointer(Descending),
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 4)
	assert.Equal(t, task1.ID, tasks[3].ID)
	assert.Equal(t, task2.ID, tasks[2].ID)
	assert.Equal(t, task3.ID, tasks[1].ID)
	assert.Equal(t, task4.ID, tasks[0].ID)

	tasks, err = repo.QueryReduceTasks(
		context.Background(),
		Filter{
			OrderBy:        utils.Pointer(OrderByCreatedAt),
			OrderDirection: utils.Pointer(Descending),
			Limit:          utils.Pointer(uint32(1)),
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
	assert.Equal(t, task4.ID, tasks[0].ID)

	tasks, err = repo.QueryReduceTasks(
		context.Background(),
		Filter{
			OrderBy:        utils.Pointer(OrderByCreatedAt),
			OrderDirection: utils.Pointer(Descending),
			Limit:          utils.Pointer(uint32(1)),
			Offset:         utils.Pointer(uint32(1)),
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
	assert.Equal(t, task3.ID, tasks[0].ID)

	tasks, err = repo.QueryReduceTasks(
		context.Background(),
		Filter{
			OrderBy:        utils.Pointer(OrderByCreatedAt),
			OrderDirection: utils.Pointer(Descending),
			Limit:          utils.Pointer(uint32(1)),
			Offset:         utils.Pointer(uint32(2)),
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
	assert.Equal(t, task2.ID, tasks[0].ID)

	tasks, err = repo.QueryReduceTasks(
		context.Background(),
		Filter{
			OrderBy:        utils.Pointer(OrderByCreatedAt),
			OrderDirection: utils.Pointer(Descending),
			Limit:          utils.Pointer(uint32(1)),
			Offset:         utils.Pointer(uint32(3)),
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
	assert.Equal(t, task1.ID, tasks[0].ID)

	tasks, err = repo.QueryReduceTasks(
		context.Background(),
		Filter{
			OrderBy:        utils.Pointer(OrderByCreatedAt),
			OrderDirection: utils.Pointer(Ascending),
			IDs:            []string{task3.ID, task4.ID},
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 2)
	assert.Equal(t, task3.ID, tasks[0].ID)
	assert.Equal(t, task4.ID, tasks[1].ID)

	tasks, err = repo.QueryReduceTasks(
		context.Background(),
		Filter{
			OrderBy: utils.Pointer(OrderByCreatedAt),
			Statuses: []mapreduce.TaskStatus{
				mapreduce.TaskDone,
				mapreduce.TaskFailed,
			},
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 2)
	assert.Equal(t, task3.ID, tasks[0].ID)
	assert.Equal(t, task4.ID, tasks[1].ID)

	tasks, err = repo.QueryReduceTasks(
		context.Background(),
		Filter{
			OrderBy: utils.Pointer(OrderByCreatedAt),
			Statuses: []mapreduce.TaskStatus{
				mapreduce.TaskInProgress,
			},
			InProgressFor: utils.Pointer(time.Hour * 24),
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
	assert.Equal(t, task2.ID, tasks[0].ID)

	tasks, err = repo.QueryReduceTasks(
		context.Background(),
		Filter{
			OrderBy: utils.Pointer(OrderByCreatedAt),
			Statuses: []mapreduce.TaskStatus{
				mapreduce.TaskInProgress,
			},
			InProgressFor: utils.Pointer(time.Hour * 100),
		},
	)
	assert.NoError(t, err)
	assert.Len(t, tasks, 0)
}

func TestNewMongoDBTasksRepository_Integration_Transaction_Commit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	repo, cntnr := NewTestMongoDBTasksRepository(t)
	defer cntnr.Terminate(context.Background())
	transaction := func(ctx context.Context) (any, error) {
		sessionCtx, ok := ctx.(mongo.SessionContext)
		assert.True(t, ok)
		err := repo.CreateMapTask(ctx, testMapTask)
		assert.NoError(t, err)
		err = sessionCtx.CommitTransaction(ctx)
		assert.NoError(t, err)
		return "success", nil
	}
	res, err := repo.Transaction(context.Background(), transaction)
	assert.NoError(t, err)
	assert.Equal(t, "success", res.(string))
	task, err := repo.GetMapTask(context.Background(), testMapTask.ID)
	assert.NoError(t, err)
	assert.Equal(t, testMapTask.ID, task.ID)
}

func TestNewMongoDBTasksRepository_Integration_Transaction_Abort(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	repo, cntnr := NewTestMongoDBTasksRepository(t)
	defer cntnr.Terminate(context.Background())
	transaction := func(ctx context.Context) (any, error) {
		sessionCtx, ok := ctx.(mongo.SessionContext)
		assert.True(t, ok)
		err := repo.CreateMapTask(ctx, testMapTask)
		assert.NoError(t, err)
		err = sessionCtx.AbortTransaction(ctx)
		assert.NoError(t, err)
		return "abort", nil
	}
	res, err := repo.Transaction(context.Background(), transaction)
	assert.NoError(t, err)
	assert.Equal(t, "abort", res.(string))
	_, err = repo.GetMapTask(context.Background(), testMapTask.ID)
	assert.Error(t, err)
}
