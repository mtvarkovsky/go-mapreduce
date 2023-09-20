package service

import (
	"context"
	"fmt"
	"github.com/mtvarkovsky/go-mapreduce/pkg/config"
	"github.com/mtvarkovsky/go-mapreduce/pkg/errors"
	"github.com/mtvarkovsky/go-mapreduce/pkg/events"
	"github.com/mtvarkovsky/go-mapreduce/pkg/ids"
	"github.com/mtvarkovsky/go-mapreduce/pkg/logger"
	"github.com/mtvarkovsky/go-mapreduce/pkg/mapreduce"
	"github.com/mtvarkovsky/go-mapreduce/pkg/repository"
	"github.com/mtvarkovsky/go-mapreduce/pkg/utils"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type (
	coordinator struct {
		ctx      context.Context
		cfg      config.Coordinator
		repo     repository.Tasks
		idGen    ids.IDGenerator
		producer events.Producer
		log      logger.Logger
	}
)

func NewCoordinator(
	ctx context.Context,
	config config.Coordinator,
	repo repository.Tasks,
	idGen ids.IDGenerator,
	producer events.Producer,
	log logger.Logger,
) mapreduce.Coordinator {
	l := log.Logger("CoordinatorService")
	l.Infof("init coordinator service")
	return &coordinator{
		ctx:      ctx,
		cfg:      config,
		repo:     repo,
		idGen:    idGen,
		producer: producer,
		log:      l,
	}
}

func (c *coordinator) TaskFlusher() {
	c.log.Infof("init task flusher")
	ticker := time.NewTicker(c.cfg.AutoFlushTasksEvery)

	for {
		select {
		case <-ticker.C:
			c.log.Infof("flush tasks")
			go c.FlushCreatedTasksToWorkers(c.ctx)
		}
	}
}

func (c *coordinator) MapTasksRescheduler() {
	c.log.Infof("init map task rescheduler")
	ticker := time.NewTicker(c.cfg.CheckForTasksToRescheduleEvery)

	for {
		select {
		case <-ticker.C:
			c.log.Infof("reschedule map tasks")
			go c.rescheduleMapTasks()
		}
	}
}

func (c *coordinator) ReduceTasksRescheduler() {
	c.log.Infof("init reduce task rescheduler")
	ticker := time.NewTicker(c.cfg.CheckForTasksToRescheduleEvery)

	for {
		select {
		case <-ticker.C:
			c.log.Infof("reschedule reduce tasks")
			go c.rescheduleReduceTasks()
		}
	}
}

func (c *coordinator) rescheduleMapTasks() error {
	c.log.Infof("try to reschedule map tasks")
	transaction := func(ctx context.Context) (any, error) {
		sessionCtx, ok := ctx.(mongo.SessionContext)
		if !ok {
			return nil, fmt.Errorf("expected mongo.SessionContext")
		}
		c.log.Infof("get map tasks to reschedule")
		tasksToReschedule, err := c.repo.QueryMapTasks(
			ctx,
			repository.Filter{
				Statuses:      []mapreduce.TaskStatus{mapreduce.TaskInProgress},
				InProgressFor: &c.cfg.RescheduleInProgressTasksAfter,
				Limit:         utils.Pointer(uint32(1000)),
			},
		)
		if err != nil {
			c.log.Warnf("can't get map tasks to reschedule: (%s)", err.Error())
			sessionCtx.AbortTransaction(ctx)
			return nil, err
		}
		if len(tasksToReschedule) == 0 {
			c.log.Infof("no map tasks to reschedule")
			sessionCtx.CommitTransaction(ctx)
			return nil, nil
		}
		now := time.Now()
		taskIDs := make([]string, len(tasksToReschedule))
		for i, t := range tasksToReschedule {
			taskIDs[i] = t.ID
		}
		c.log.Infof("reschedule map tasks with ids=%s", taskIDs)
		err = c.repo.UpdateMapTasks(
			ctx,
			taskIDs,
			repository.UpdateFields{
				Status:        utils.Pointer(mapreduce.TaskRescheduled),
				RescheduledAt: &now,
			},
		)
		if err != nil {
			c.log.Warnf("can't reschedule map tasks with ids=%s: (%s)", taskIDs, err.Error())
			sessionCtx.AbortTransaction(ctx)
			return nil, err
		}
		err = sessionCtx.CommitTransaction(ctx)
		if err != nil {
			return nil, err
		}
		c.log.Infof("rescheduled map tasks with ids=%s", taskIDs)
		return taskIDs, nil
	}

	res, err := c.repo.Transaction(c.ctx, transaction)
	if err != nil {
		return err
	}

	var taskIDs []string
	if res != nil {
		taskIDs = res.([]string)
	}

	c.log.Infof("produce events %s for map tasks with ids=%s", events.MapTaskCreated, taskIDs)
	for _, id := range taskIDs {
		err = c.producer.Produce(c.ctx, events.Event{Type: events.MapTaskCreated})
		if err != nil {
			c.log.Warnf("cant produce %s event for map task with id=%s: (%s)", events.MapTaskCreated, id)
		}
	}
	return nil
}

func (c *coordinator) rescheduleReduceTasks() error {
	c.log.Infof("try to reschedule reduce tasks")
	transaction := func(ctx context.Context) (any, error) {
		sessionCtx, ok := ctx.(mongo.SessionContext)
		if !ok {
			return nil, fmt.Errorf("expected mongo.SessionContext")
		}
		c.log.Infof("get reduce tasks to reschedule")
		tasksToReschedule, err := c.repo.QueryReduceTasks(
			ctx,
			repository.Filter{
				Statuses:      []mapreduce.TaskStatus{mapreduce.TaskInProgress},
				InProgressFor: &c.cfg.RescheduleInProgressTasksAfter,
				Limit:         utils.Pointer(uint32(1000)),
			},
		)
		if err != nil {
			c.log.Warnf("can't get reduce tasks to reschedule: (%s)", err.Error())
			sessionCtx.AbortTransaction(ctx)
			return nil, err
		}
		if len(tasksToReschedule) == 0 {
			c.log.Infof("no reduce tasks to reschedule")
			sessionCtx.CommitTransaction(ctx)
			return nil, nil
		}
		now := time.Now()
		taskIDs := make([]string, len(tasksToReschedule))
		for i, t := range tasksToReschedule {
			taskIDs[i] = t.ID
		}
		c.log.Infof("reschedule reduce tasks with ids=%s", taskIDs)
		err = c.repo.UpdateReduceTasks(
			ctx,
			taskIDs,
			repository.UpdateFields{
				Status:        utils.Pointer(mapreduce.TaskRescheduled),
				RescheduledAt: &now,
			},
		)
		if err != nil {
			c.log.Warnf("can't reschedule reduce tasks with ids=%s: (%s)", taskIDs, err.Error())
			sessionCtx.AbortTransaction(ctx)
			return nil, err
		}
		err = sessionCtx.CommitTransaction(ctx)
		if err != nil {
			return nil, err
		}
		c.log.Infof("rescheduled reduce tasks with ids=%s", taskIDs)
		return taskIDs, nil
	}

	res, err := c.repo.Transaction(c.ctx, transaction)
	if err != nil {
		return err
	}

	var taskIDs []string
	if res != nil {
		taskIDs = res.([]string)
	}

	c.log.Infof("produce events %s for reduce tasks with ids=%s", events.ReduceTaskCreated, taskIDs)
	for _, id := range taskIDs {
		c.producer.Produce(c.ctx, events.Event{Type: events.ReduceTaskCreated})
		if err != nil {
			c.log.Warnf("cant produce %s event for reduce task with id=%s: (%s)", events.ReduceTaskCreated, id)
		}
	}
	return nil
}

func (c *coordinator) CreateMapTask(ctx context.Context, inputFile string) (mapreduce.MapTask, error) {
	c.log.Infof("create map task for file=%s", inputFile)
	now := time.Now()
	id, err := c.idGen.GetID(ctx, ids.MapTask)
	if err != nil {
		c.log.Warnf("can't get id for map task with file=%s", inputFile)
		return mapreduce.MapTask{}, err
	}
	task := mapreduce.MapTask{
		ID:        id,
		InputFile: inputFile,
		Status:    mapreduce.TaskCreated,
		CreatedAt: now,
	}
	err = c.repo.CreateMapTask(ctx, task)
	if err != nil {
		c.log.Warnf("can't create map task with id=%s: (%s)", id, err.Error())
		return mapreduce.MapTask{}, err
	}
	err = c.producer.Produce(ctx, events.Event{Type: events.MapTaskCreated})
	if err != nil {
		c.log.Warnf("can't produce %s event for map task with id=%s: (%s)", events.MapTaskCreated, id, err.Error())
	}
	return task, nil
}

func (c *coordinator) CreateReduceTask(ctx context.Context, inputFiles ...string) (mapreduce.ReduceTask, error) {
	c.log.Infof("create reduce task for files=%s", inputFiles)
	now := time.Now()
	id, err := c.idGen.GetID(ctx, ids.ReduceTask)
	if err != nil {
		c.log.Warnf("can't get id for reduce task with files=%s", inputFiles)
		return mapreduce.ReduceTask{}, err
	}
	task := mapreduce.ReduceTask{
		ID:         id,
		InputFiles: inputFiles,
		Status:     mapreduce.TaskCreated,
		CreatedAt:  now,
	}
	err = c.repo.CreateReduceTask(ctx, task)
	if err != nil {
		c.log.Warnf("can't create reduce task with id=%s: (%s)", id, err.Error())
		return mapreduce.ReduceTask{}, err
	}
	err = c.producer.Produce(ctx, events.Event{Type: events.ReduceTaskCreated})
	if err != nil {
		c.log.Warnf("can't produce %s event for reduce task with id=%s: (%s)", events.ReduceTaskCreated, id, err.Error())
	}
	return task, nil
}

func (c *coordinator) GetMapTask(ctx context.Context) (mapreduce.MapTask, error) {
	c.log.Infof("get map task for processing")
	tasks, err := c.repo.QueryMapTasks(
		ctx,
		repository.Filter{
			Statuses: []mapreduce.TaskStatus{mapreduce.TaskCreated, mapreduce.TaskRescheduled},
			Limit:    utils.Pointer(uint32(1)),
		},
	)
	if err != nil {
		c.log.Warnf("can't get map task for processing: (%s)", err.Error())
		return mapreduce.MapTask{}, err
	}
	if len(tasks) == 0 {
		c.log.Infof("no map tasks for processing")
		return mapreduce.MapTask{}, errors.ErrNoTasks
	}
	task := tasks[0]
	task.Status = mapreduce.TaskInProgress
	task.InProgressAt = utils.Pointer(time.Now())
	c.log.Infof("set map task with id=%s to %s", task.ID, task.Status)
	err = c.repo.UpdateMapTask(ctx, task)
	if err != nil {
		c.log.Warnf("can't set map task with id=%s to %s: (%s)", task.ID, task.Status, err.Error())
		return mapreduce.MapTask{}, fmt.Errorf(
			"can't get task, can't change task status to=%s with id=%s: %w",
			task.ID,
			mapreduce.TaskInProgress,
			err,
		)
	}
	return task, nil
}

func (c *coordinator) GetReduceTask(ctx context.Context) (mapreduce.ReduceTask, error) {
	c.log.Infof("get reduce task for processing")
	tasks, err := c.repo.QueryReduceTasks(
		ctx,
		repository.Filter{
			Statuses: []mapreduce.TaskStatus{mapreduce.TaskCreated, mapreduce.TaskRescheduled},
			Limit:    utils.Pointer(uint32(1)),
		},
	)
	if err != nil {
		c.log.Warnf("can't get reduce task for processing: (%s)", err.Error())
		return mapreduce.ReduceTask{}, err
	}
	if len(tasks) == 0 {
		c.log.Infof("no reduce tasks for processing")
		return mapreduce.ReduceTask{}, errors.ErrNoTasks
	}
	task := tasks[0]
	task.Status = mapreduce.TaskInProgress
	task.InProgressAt = utils.Pointer(time.Now())
	c.log.Infof("set reduce task with id=%s to %s", task.ID, task.Status)
	err = c.repo.UpdateReduceTask(ctx, task)
	if err != nil {
		c.log.Warnf("can't set reduce task with id=%s to %s: (%s)", task.ID, task.Status, err.Error())
		return mapreduce.ReduceTask{}, fmt.Errorf(
			"can't get task, can't change task status to=%s with id=%s: %w",
			mapreduce.TaskInProgress,
			task.ID,
			err,
		)
	}
	return task, nil
}

func (c *coordinator) ReportMapTask(ctx context.Context, taskResult mapreduce.MapTaskResult) error {
	c.log.Infof("try to report map task result with task id=%s", taskResult.TaskID)
	transaction := func(ctx context.Context) (any, error) {
		sessionCtx, ok := ctx.(mongo.SessionContext)
		if !ok {
			return nil, fmt.Errorf("expected mongo.SessionContext")
		}
		c.log.Infof("get map task with id=%s to report result", taskResult.TaskID)
		task, err := c.repo.GetMapTask(ctx, taskResult.TaskID)
		if err != nil {
			c.log.Warnf("can't get map task with id=%s to report result: (%s)", taskResult.TaskID, err.Error())
			sessionCtx.AbortTransaction(ctx)
			return nil, err
		}
		if task.Status != mapreduce.TaskInProgress {
			c.log.Warnf(
				"can't report map task with id=%s: task must be in %s status instead of %s",
				task.ID,
				mapreduce.TaskInProgress,
				task.Status,
			)
			sessionCtx.AbortTransaction(ctx)
			return nil, errors.ErrReportTask
		}
		now := time.Now()
		if taskResult.Error != nil {
			task.FailedAt = &now
			task.Status = mapreduce.TaskFailed
		} else {
			task.DoneAt = &now
			task.Status = mapreduce.TaskDone
		}
		c.log.Infof("report map task result with id=%s", task.ID)
		err = c.repo.UpdateMapTask(ctx, task)
		if err != nil {
			c.log.Warnf("can't report map task result with id=%s: (%s)", task.ID, err.Error())
			sessionCtx.AbortTransaction(ctx)
			return nil, fmt.Errorf(
				"can't report map task, can't change task status to=%s with id=%s: %w",
				mapreduce.TaskFailed,
				taskResult.TaskID,
				err,
			)
		}
		if task.Status == mapreduce.TaskDone {
			c.log.Infof("map task with id=%s is done, create reduce task")
			_, err = c.CreateReduceTask(ctx, taskResult.OutputFiles...)
			if err != nil {
				c.log.Warnf("can't create reduce task with input files=%s for map task with id=%s: (%s)", taskResult.OutputFiles, task.ID, err.Error())
				sessionCtx.AbortTransaction(ctx)
				return nil, fmt.Errorf("can't create reduce task for map task with id=%s: %w", taskResult.TaskID, err)
			}
		}
		sessionCtx.CommitTransaction(ctx)
		return nil, nil
	}
	_, err := c.repo.Transaction(ctx, transaction)
	if err != nil {
		return err
	}
	return nil
}

func (c *coordinator) ReportReduceTask(ctx context.Context, taskResult mapreduce.ReduceTaskResult) error {
	c.log.Infof("try to report reduce task result with task id=%s", taskResult.TaskID)
	c.log.Infof("get reduce task with id=%s to report result", taskResult.TaskID)
	task, err := c.repo.GetReduceTask(ctx, taskResult.TaskID)
	if err != nil {
		c.log.Warnf("can't get reduce task with id=%s to report result: (%s)", taskResult.TaskID, err.Error())
		return fmt.Errorf("can't report reduce task with id=%s: %w", taskResult.TaskID, err)
	}
	if task.Status != mapreduce.TaskInProgress {
		c.log.Warnf(
			"can't report reduce task with id=%s: task must be in %s status instead of %s",
			task.ID,
			mapreduce.TaskInProgress,
			task.Status,
		)
		return errors.ErrReportTask
	}
	now := time.Now()
	if taskResult.Error != nil {
		task.FailedAt = &now
		task.Status = mapreduce.TaskFailed
	} else {
		task.DoneAt = &now
		task.Status = mapreduce.TaskDone
	}
	c.log.Infof("report reduce task result with id=%s", task.ID)
	err = c.repo.UpdateReduceTask(ctx, task)
	if err != nil {
		c.log.Warnf("can't report reduce task result with id=%s: (%s)", task.ID, err.Error())
		return fmt.Errorf(
			"can't report reduce task, can't change task status to=%s with id=%s: %w",
			taskResult.TaskID,
			mapreduce.TaskFailed,
			err,
		)
	}
	return nil
}

func (c *coordinator) FlushCreatedTasksToWorkers(ctx context.Context) error {
	c.log.Infof("get map tasks to resend to workers")
	mapTasks, err := c.repo.QueryMapTasks(
		ctx,
		repository.Filter{
			Statuses: []mapreduce.TaskStatus{mapreduce.TaskCreated, mapreduce.TaskRescheduled},
		},
	)
	if err != nil {
		c.log.Warnf("can't get map tasks to resend to workers: (%s)", err.Error())
		return err
	}
	for _, t := range mapTasks {
		c.log.Infof("resend map task with id=%s to workers", t.ID)
		err = c.producer.Produce(ctx, events.Event{Type: events.MapTaskCreated})
		if err != nil {
			c.log.Warnf("can't resend map task with id=%s to workers: (%s)", t.ID, err.Error())
		}
	}

	c.log.Infof("get reduce tasks to resend to workers")
	reduceTasks, err := c.repo.QueryReduceTasks(
		ctx,
		repository.Filter{
			Statuses: []mapreduce.TaskStatus{mapreduce.TaskCreated, mapreduce.TaskRescheduled},
		},
	)
	if err != nil {
		c.log.Warnf("can't get reduce tasks to resend to workers: (%s)", err.Error())
		return err
	}
	for _, t := range reduceTasks {
		c.log.Infof("resend reduce task with id=%s to workers", t.ID)
		err = c.producer.Produce(ctx, events.Event{Type: events.ReduceTaskCreated})
		if err != nil {
			c.log.Warnf("can't resend reduce task with id=%s to workers: (%s)", t.ID, err.Error())
		}
	}

	return nil
}
