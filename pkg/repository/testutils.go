package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/mtvarkovsky/go-mapreduce/pkg/config"
	"github.com/mtvarkovsky/go-mapreduce/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
)

type (
	UpdateFieldsMatcher struct {
		UpdateFields
	}
)

func (m UpdateFieldsMatcher) Matches(x interface{}) bool {
	other, ok := x.(UpdateFields)
	if !ok {
		return false
	}

	if m.Status == nil && other.Status != nil {
		return false
	} else if m.Status != nil && other.Status == nil {
		return false
	} else if m.Status != nil && other.Status != nil {
		if m.Status != other.Status {
			return false
		}
	}
	if m.RescheduledAt == nil && other.RescheduledAt != nil {
		return false
	} else if m.RescheduledAt != nil && other.RescheduledAt == nil {
		return false
	} else if m.RescheduledAt != nil && other.RescheduledAt != nil {
		if m.RescheduledAt.Sub(*other.RescheduledAt) > time.Millisecond {
			return false
		}
	}

	return true
}

func (m UpdateFieldsMatcher) String() string {
	return fmt.Sprintf("UpdateFields{Status=%s, RescheduleAt=%s}", m.Status, m.RescheduledAt)
}

func NewTestMongoDBTasksRepository(t *testing.T) (*MongoDBTasksRepository, *mongodb.MongoDBContainer) {
	ctx := context.Background()
	mongodbContainer, err := mongodb.RunContainer(
		ctx,
		testcontainers.WithImage("mongo:latest"),
		testcontainers.WithConfigModifier(
			func(config *container.Config) {
				config.Cmd = append(config.Cmd, "--replSet", "rs0")
			},
		),
	)
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)
	_, _, err = mongodbContainer.Exec(
		ctx,
		[]string{
			"mongosh",
			"--eval",
			"rs.initiate().ok;",
			"&&",
		},
	)
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)
	endpoint, err := mongodbContainer.ConnectionString(ctx)
	assert.NoError(t, err)
	cfg := config.MongoDB{
		URI:    endpoint + "/?replicaSet=rs0&directConnection=true",
		DBName: "coordinator",
	}
	repo, err := NewMongoDBTasksRepository(ctx, cfg, logger.NewTestZapLogger())
	assert.NoError(t, err)
	return repo, mongodbContainer
}
