package repository

import (
	"context"
	"fmt"
	"github.com/mtvarkovsky/go-mapreduce/pkg/config"
	"github.com/mtvarkovsky/go-mapreduce/pkg/errors"
	"github.com/mtvarkovsky/go-mapreduce/pkg/logger"
	"github.com/mtvarkovsky/go-mapreduce/pkg/mapreduce"
	"github.com/mtvarkovsky/go-mapreduce/pkg/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const (
	mapTasksCollection    = "map_tasks"
	reduceTasksCollection = "reduce_tasks"
)

type (
	mongoDBTasksRepository struct {
		config config.MongoDB
		client *mongo.Client
		log    logger.Logger
	}

	mongoMapTask struct {
		ID            string               `bson:"id"`
		InputFile     string               `bson:"input_file"`
		Status        mapreduce.TaskStatus `bson:"status"`
		CreatedAt     primitive.DateTime   `bson:"created_at"`
		InProgressAt  *primitive.DateTime  `bson:"in_progress_at,omitempty"`
		FailedAt      *primitive.DateTime  `bson:"failed_at,omitempty"`
		DoneAt        *primitive.DateTime  `bson:"done_at,omitempty"`
		RescheduledAt *primitive.DateTime  `bson:"rescheduled_at,omitempty"`
	}

	mongoReduceTask struct {
		ID            string               `bson:"id"`
		InputFiles    []string             `bson:"input_files"`
		Status        mapreduce.TaskStatus `bson:"status"`
		CreatedAt     primitive.DateTime   `bson:"created_at"`
		InProgressAt  *primitive.DateTime  `bson:"in_progress_at,omitempty"`
		FailedAt      *primitive.DateTime  `bson:"failed_at,omitempty"`
		DoneAt        *primitive.DateTime  `bson:"done_at,omitempty"`
		RescheduledAt *primitive.DateTime  `bson:"rescheduled_at,omitempty"`
	}
)

func NewMongoDBTasksRepository(ctx context.Context, config config.MongoDB, log logger.Logger) (Tasks, error) {
	l := log.Logger("MongoDBRepository")
	l.Infof("init mongodb repository")
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(config.URI))
	if err != nil {
		l.Errorf("can't connect to mongodb at=%s: (%s)", config.URI, err.Error())
		return nil, fmt.Errorf("can't connect to mongodb at uri=%s: %w", config.URI, err)
	}
	repo := &mongoDBTasksRepository{
		config: config,
		client: client,
		log:    l,
	}
	l.Infof("setup mongodb repository")
	err = repo.setup(ctx)
	if err != nil {
		l.Infof("can't setup mongodb repository: (%s)", err.Error())
		return nil, err
	}
	return repo, nil
}

func toMongoMapTask(task mapreduce.MapTask) mongoMapTask {
	t := mongoMapTask{
		ID:        task.ID,
		InputFile: task.InputFile,
		Status:    task.Status,
		CreatedAt: primitive.NewDateTimeFromTime(task.CreatedAt),
	}
	if task.InProgressAt != nil {
		t.InProgressAt = utils.Pointer(primitive.NewDateTimeFromTime(*task.InProgressAt))
	}
	if task.FailedAt != nil {
		t.FailedAt = utils.Pointer(primitive.NewDateTimeFromTime(*task.FailedAt))
	}
	if task.DoneAt != nil {
		t.DoneAt = utils.Pointer(primitive.NewDateTimeFromTime(*task.DoneAt))
	}
	if task.RescheduledAt != nil {
		t.RescheduledAt = utils.Pointer(primitive.NewDateTimeFromTime(*task.RescheduledAt))
	}
	return t
}

func fromMongoMapTask(task mongoMapTask) mapreduce.MapTask {
	t := mapreduce.MapTask{
		ID:        task.ID,
		Status:    task.Status,
		InputFile: task.InputFile,
		CreatedAt: task.CreatedAt.Time(),
	}
	if task.InProgressAt != nil {
		t.InProgressAt = utils.Pointer(task.InProgressAt.Time())
	}
	if task.FailedAt != nil {
		t.FailedAt = utils.Pointer(task.FailedAt.Time())
	}
	if task.DoneAt != nil {
		t.DoneAt = utils.Pointer(task.DoneAt.Time())
	}
	if task.RescheduledAt != nil {
		t.RescheduledAt = utils.Pointer(task.RescheduledAt.Time())
	}
	return t
}

func toMongoReduceTask(task mapreduce.ReduceTask) mongoReduceTask {
	t := mongoReduceTask{
		ID:         task.ID,
		InputFiles: task.InputFiles,
		Status:     task.Status,
		CreatedAt:  primitive.NewDateTimeFromTime(task.CreatedAt),
	}
	if task.InProgressAt != nil {
		t.InProgressAt = utils.Pointer(primitive.NewDateTimeFromTime(*task.InProgressAt))
	}
	if task.FailedAt != nil {
		t.FailedAt = utils.Pointer(primitive.NewDateTimeFromTime(*task.FailedAt))
	}
	if task.DoneAt != nil {
		t.DoneAt = utils.Pointer(primitive.NewDateTimeFromTime(*task.DoneAt))
	}
	if task.RescheduledAt != nil {
		t.RescheduledAt = utils.Pointer(primitive.NewDateTimeFromTime(*task.RescheduledAt))
	}
	return t
}

func fromMongoReduceTask(task mongoReduceTask) mapreduce.ReduceTask {
	t := mapreduce.ReduceTask{
		ID:         task.ID,
		Status:     task.Status,
		InputFiles: task.InputFiles,
		CreatedAt:  task.CreatedAt.Time(),
	}
	if task.InProgressAt != nil {
		t.InProgressAt = utils.Pointer(task.InProgressAt.Time())
	}
	if task.FailedAt != nil {
		t.FailedAt = utils.Pointer(task.FailedAt.Time())
	}
	if task.DoneAt != nil {
		t.DoneAt = utils.Pointer(task.DoneAt.Time())
	}
	if task.RescheduledAt != nil {
		t.RescheduledAt = utils.Pointer(task.RescheduledAt.Time())
	}
	return t
}

func (r *mongoDBTasksRepository) setup(ctx context.Context) error {
	mapCollection := r.getMapTasksCollection()
	err := r.createIndex(ctx, mapCollection, "id", 1, options.Index().SetUnique(true))
	if err != nil {
		return fmt.Errorf("can't setup mongodb repo, can't create index for collextion=%s: %w", mapCollection.Name(), err)
	}

	reduceCollection := r.getReduceTasksCollection()
	err = r.createIndex(ctx, reduceCollection, "id", 1, options.Index().SetUnique(true))
	if err != nil {
		return fmt.Errorf("can't setup mongodb repo, can't create index for collextion=%s: %w", reduceCollection.Name(), err)
	}

	return nil
}

func (r *mongoDBTasksRepository) createIndex(
	ctx context.Context,
	collection *mongo.Collection,
	key string,
	indexType int,
	opt *options.IndexOptions,
) error {
	_, err := collection.Indexes().CreateOne(
		ctx,
		mongo.IndexModel{
			Keys:    bson.D{{key, indexType}},
			Options: opt,
		},
	)
	if err != nil {
		return fmt.Errorf("can't create mongodb index for fied=%s: %w", key, err)
	}
	return nil
}

func (r *mongoDBTasksRepository) getMapTasksCollection() *mongo.Collection {
	return r.client.Database(r.config.DBName).Collection(mapTasksCollection)
}

func (r *mongoDBTasksRepository) getReduceTasksCollection() *mongo.Collection {
	return r.client.Database(r.config.DBName).Collection(reduceTasksCollection)
}

func (r *mongoDBTasksRepository) Transaction(ctx context.Context, transaction func(ctx context.Context) (any, error)) (any, error) {
	r.log.Infof("start transaction")
	session, err := r.client.StartSession()
	if err != nil {
		r.log.Infof("can't start transaction: (%s)", err.Error())
		return nil, fmt.Errorf("can't get session: %w", err)
	}
	defer session.EndSession(ctx)
	trnsctn := func(ctx mongo.SessionContext) (any, error) {
		return transaction(ctx)
	}
	res, err := session.WithTransaction(ctx, trnsctn)
	if err != nil {
		r.log.Warnf("can't execute transaction: (%s)", err.Error())
		return nil, err
	}
	r.log.Infof("transaction done")
	return res, nil
}

func (r *mongoDBTasksRepository) CreateMapTask(ctx context.Context, task mapreduce.MapTask) error {
	r.log.Infof("create map task with id=%s", task.ID)
	coll := r.getMapTasksCollection()
	_, err := coll.InsertOne(ctx, toMongoMapTask(task))
	if err != nil {
		r.log.Warnf("can't create map task with id=%s: (%s)", task.ID, err.Error())
		return fmt.Errorf("can't create map task with id=%s: %w", task.ID, errors.ErrCreateTask)
	}
	return nil
}

func (r *mongoDBTasksRepository) CreateReduceTask(ctx context.Context, task mapreduce.ReduceTask) error {
	r.log.Infof("create reduce task with id=%s", task.ID)
	coll := r.getReduceTasksCollection()
	_, err := coll.InsertOne(ctx, toMongoReduceTask(task))
	if err != nil {
		r.log.Warnf("can't create reduce task with id=%s: (%s)", task.ID, err.Error())
		return fmt.Errorf("can't create reduce task with id=%s: %w", task.ID, errors.ErrCreateTask)
	}
	return nil
}

func (r *mongoDBTasksRepository) UpdateMapTask(ctx context.Context, task mapreduce.MapTask) error {
	r.log.Infof("update map task with id=%s", task.ID)
	t := toMongoMapTask(task)
	coll := r.getMapTasksCollection()
	filter := bson.D{{"id", t.ID}}
	update := bson.D{{"$set",
		bson.D{
			{"status", t.Status},
			{"in_progress_at", t.InProgressAt},
			{"failed_at", t.FailedAt},
			{"done_at", t.DoneAt},
			{"rescheduled_at", t.RescheduledAt},
		},
	}}
	_, err := coll.UpdateOne(ctx, filter, update)
	if err != nil {
		r.log.Warnf("can't update map task with id=%s: (%s)", task.ID, err.Error())
		if errors.ErrorIs(err, mongo.ErrNoDocuments) {
			return fmt.Errorf("can't get update task with id=%s: %w", task.ID, errors.ErrTaskDoesNotExist)
		}
		return fmt.Errorf("can't update map task with id=%s: %w", task.ID, errors.ErrInternal)
	}
	return nil
}

func (r *mongoDBTasksRepository) UpdateReduceTask(ctx context.Context, task mapreduce.ReduceTask) error {
	r.log.Infof("update reduce task with id=%s", task.ID)
	t := toMongoReduceTask(task)
	coll := r.getReduceTasksCollection()
	filter := bson.D{{"id", task.ID}}
	update := bson.D{{"$set",
		bson.D{
			{"status", t.Status},
			{"in_progress_at", t.InProgressAt},
			{"failed_at", t.FailedAt},
			{"done_at", t.DoneAt},
			{"rescheduled_at", t.RescheduledAt},
		},
	}}
	_, err := coll.UpdateOne(ctx, filter, update)
	if err != nil {
		r.log.Warnf("can't update reduce task with id=%s: (%s)", task.ID, err.Error())
		if errors.ErrorIs(err, mongo.ErrNoDocuments) {
			return fmt.Errorf("can't update reduce task with id=%s: %w", task.ID, errors.ErrTaskDoesNotExist)
		}
		return fmt.Errorf("can't update reduce task with id=%s: %w", task.ID, errors.ErrInternal)
	}
	return nil
}

func (r *mongoDBTasksRepository) UpdateMapTasks(
	ctx context.Context,
	ids []string,
	fields UpdateFields,
) error {
	r.log.Infof("update map tasks with ids=%s", ids)
	coll := r.getMapTasksCollection()
	filter := bson.M{
		"id": bson.M{"$in": ids},
	}
	upd := bson.M{}
	if fields.Status != nil {
		upd["status"] = *fields.Status
	}
	if fields.RescheduledAt != nil {
		upd["rescheduled_at"] = primitive.NewDateTimeFromTime(*fields.RescheduledAt)
	}
	update := bson.D{{"$set", upd}}
	_, err := coll.UpdateMany(ctx, filter, update)
	if err != nil {
		r.log.Warnf("can't update map tasks with ids=%s: (%s)", ids, err.Error())
		if errors.ErrorIs(err, mongo.ErrNoDocuments) {
			return fmt.Errorf("can't get map tasks with ids=%s: %w", ids, errors.ErrTaskDoesNotExist)
		}
		return fmt.Errorf("can't update map tasks with id=%s: %w", ids, errors.ErrInternal)
	}
	return nil
}

func (r *mongoDBTasksRepository) UpdateReduceTasks(
	ctx context.Context,
	ids []string,
	fields UpdateFields,
) error {
	r.log.Infof("update reduce tasks with ids=%s", ids)
	coll := r.getReduceTasksCollection()
	filter := bson.M{
		"id": bson.M{"$in": ids},
	}
	upd := bson.M{}
	if fields.Status != nil {
		upd["status"] = *fields.Status
	}
	if fields.RescheduledAt != nil {
		upd["rescheduled_at"] = primitive.NewDateTimeFromTime(*fields.RescheduledAt)
	}
	update := bson.D{{"$set", upd}}
	_, err := coll.UpdateMany(ctx, filter, update)
	if err != nil {
		r.log.Warnf("can't update reduce tasks with ids=%s: (%s)", ids, err.Error())
		if errors.ErrorIs(err, mongo.ErrNoDocuments) {
			return fmt.Errorf("can't get reduce tasks with ids=%s: %w", ids, errors.ErrTaskDoesNotExist)
		}
		return fmt.Errorf("can't update reduce tasks with id=%s: %w", ids, errors.ErrInternal)
	}
	return nil
}

func (r *mongoDBTasksRepository) GetMapTask(ctx context.Context, id string) (mapreduce.MapTask, error) {
	r.log.Infof("get map task with id=%s", id)
	coll := r.getMapTasksCollection()
	filter := bson.D{{"id", id}}
	var task mongoMapTask
	err := coll.FindOne(ctx, filter).Decode(&task)
	if err != nil {
		r.log.Warnf("can't get map task with id=%s: (%s)", id, err.Error())
		if errors.ErrorIs(err, mongo.ErrNoDocuments) {
			return mapreduce.MapTask{}, fmt.Errorf("can't get map task with id=%s: %w", id, errors.ErrTaskDoesNotExist)
		}
		return mapreduce.MapTask{}, fmt.Errorf("can't get map task with id=%s: %w", id, errors.ErrInternal)
	}
	return fromMongoMapTask(task), nil
}

func (r *mongoDBTasksRepository) GetReduceTask(ctx context.Context, id string) (mapreduce.ReduceTask, error) {
	r.log.Infof("get reduce task with id=%s", id)
	coll := r.getReduceTasksCollection()
	filter := bson.D{{"id", id}}
	var task mongoReduceTask
	err := coll.FindOne(ctx, filter).Decode(&task)
	if err != nil {
		r.log.Warnf("can't get reduce task with id=%s: (%s)", id, err.Error())
		if errors.ErrorIs(err, mongo.ErrNoDocuments) {
			return mapreduce.ReduceTask{}, fmt.Errorf("can't get reduce task with id=%s: %w", id, errors.ErrTaskDoesNotExist)
		}
		return mapreduce.ReduceTask{}, fmt.Errorf("can't get reduce task with id=%s: %w", id, errors.ErrInternal)
	}
	return fromMongoReduceTask(task), nil
}

func toMongoDBFilter(filter Filter) (bson.M, []*options.FindOptions, error) {
	fltr := bson.M{}
	if len(filter.IDs) > 0 {
		fltr["id"] = bson.M{"$in": filter.IDs}
	}
	if len(filter.Statuses) > 0 {
		fltr["status"] = bson.M{"$in": filter.Statuses}
	}
	var opts []*options.FindOptions
	if filter.OrderBy != nil {
		order, err := toMongoDBOrderField(*filter.OrderBy)
		if err != nil {
			return bson.M{}, nil, err
		}
		direction := toMongoDBOrderDirection(filter.OrderDirection)
		opts = append(opts, options.Find().SetSort(bson.D{{order, direction}}))
	}

	if filter.Limit != nil {
		opts = append(opts, options.Find().SetLimit(int64(*filter.Limit)))
	}
	if filter.Offset != nil {
		opts = append(opts, options.Find().SetSkip(int64(*filter.Offset)))
	}
	if filter.InProgressFor != nil {
		now := time.Now()
		fltr["in_progress_at"] = bson.M{
			"$lte": primitive.NewDateTimeFromTime(now.Add(-*filter.InProgressFor)),
		}
	}

	return fltr, opts, nil
}

func toMongoDBOrderField(orderField OrderField) (string, error) {
	switch orderField {
	case OrderByCreatedAt:
		return "created_at", nil
	default:
		return "", fmt.Errorf("unknown OrderField=%s", orderField)
	}
}

func toMongoDBOrderDirection(orderDirection *OrderDirection) int {
	if orderDirection == nil {
		return 1
	}
	switch *orderDirection {
	case Ascending:
		return 1
	case Descending:
		return -1
	default:
		return 1
	}
}

func (r *mongoDBTasksRepository) QueryMapTasks(ctx context.Context, filter Filter) ([]mapreduce.MapTask, error) {
	r.log.Infof("query map tasks with filter=%s", filter)
	coll := r.getMapTasksCollection()
	fltr, opts, err := toMongoDBFilter(filter)
	cursor, err := coll.Find(
		ctx,
		fltr,
		opts...,
	)
	if err != nil {
		r.log.Warnf("can't query map tasks with filter=%s, can't get cursor: (%s)", filter, err.Error())
		return nil, fmt.Errorf("can't query map tasks: %w", errors.ErrInternal)
	}
	var tasks []mongoMapTask
	err = cursor.All(ctx, &tasks)
	if err != nil {
		r.log.Warnf("can't query map tasks with filter=%s, can't get tasks: (%s)", filter, err.Error())
		return nil, fmt.Errorf("can't query map tasks: %w", errors.ErrInternal)
	}
	tsks := make([]mapreduce.MapTask, len(tasks))
	for i, t := range tasks {
		tsks[i] = fromMongoMapTask(t)
	}
	return tsks, nil
}

func (r *mongoDBTasksRepository) QueryReduceTasks(ctx context.Context, filter Filter) ([]mapreduce.ReduceTask, error) {
	r.log.Infof("query reduce tasks with filter=%s", filter)
	coll := r.getReduceTasksCollection()
	fltr, opts, err := toMongoDBFilter(filter)
	cursor, err := coll.Find(
		ctx,
		fltr,
		opts...,
	)
	if err != nil {
		r.log.Warnf("can't query reduce tasks with filter=%s, can't get cursor: (%s)", filter, err.Error())
		return nil, fmt.Errorf("can't query reduce tasks: %w", errors.ErrInternal)
	}
	var tasks []mongoReduceTask
	err = cursor.All(ctx, &tasks)
	if err != nil {
		r.log.Warnf("can't query reduce tasks with filter=%s, can't get tasks: (%s)", filter, err.Error())
		return nil, fmt.Errorf("can't query reduce tasks: %w", errors.ErrInternal)
	}
	tsks := make([]mapreduce.ReduceTask, len(tasks))
	for i, t := range tasks {
		tsks[i] = fromMongoReduceTask(t)
	}
	return tsks, nil
}
