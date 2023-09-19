//go:generate mockgen -source repository.go -destination mocks/repository_mock.go -package mocks Tasks

package repository

import (
	"context"
	"fmt"
	"github.com/mtvarkovsky/go-mapreduce/pkg/mapreduce"
	"strings"
	"time"
)

type (
	Transaction interface {
		Transaction(ctx context.Context, transaction func(ctx context.Context) (any, error)) (any, error)
	}

	Tasks interface {
		Transaction

		CreateMapTask(ctx context.Context, task mapreduce.MapTask) error
		CreateReduceTask(ctx context.Context, task mapreduce.ReduceTask) error

		UpdateMapTask(ctx context.Context, task mapreduce.MapTask) error
		UpdateReduceTask(ctx context.Context, task mapreduce.ReduceTask) error

		UpdateMapTasks(ctx context.Context, ids []string, fields UpdateFields) error
		UpdateReduceTasks(ctx context.Context, ids []string, fields UpdateFields) error

		GetMapTask(ctx context.Context, id string) (mapreduce.MapTask, error)
		GetReduceTask(ctx context.Context, id string) (mapreduce.ReduceTask, error)

		QueryMapTasks(ctx context.Context, filter Filter) ([]mapreduce.MapTask, error)
		QueryReduceTasks(ctx context.Context, filter Filter) ([]mapreduce.ReduceTask, error)
	}

	Filter struct {
		IDs            []string
		Statuses       []mapreduce.TaskStatus
		Limit          *uint32
		Offset         *uint32
		OrderBy        *OrderField
		OrderDirection *OrderDirection
		InProgressFor  *time.Duration
	}

	OrderField     int
	OrderDirection int

	UpdateFields struct {
		Status        *mapreduce.TaskStatus
		RescheduledAt *time.Time
	}
)

const (
	OrderByCreatedAt OrderField = iota
)

func (of OrderField) String() string {
	return [...]string{
		"OrderByCreatedAt",
	}[of]
}

const (
	Ascending OrderDirection = iota
	Descending
)

func (od OrderDirection) String() string {
	return [...]string{
		"Ascending",
		"Descending",
	}[od]
}

func (f Filter) String() string {
	str := strings.Builder{}
	str.WriteString(fmt.Sprintf("Filter{IDs=%s, Statuses=%s", f.IDs, f.Statuses))
	if f.Offset != nil {
		str.WriteString(fmt.Sprintf(", Offset=%d", *f.Offset))
	}
	if f.Limit != nil {
		str.WriteString(fmt.Sprintf(", Limit=%d", *f.Limit))
	}
	if f.OrderBy != nil {
		str.WriteString(fmt.Sprintf(", OrderBy=%s", *f.OrderBy))
	}
	if f.OrderDirection != nil {
		str.WriteString(fmt.Sprintf(", OrderDirection=%s", *f.OrderDirection))
	} else {
		str.WriteString(fmt.Sprintf(", OrderDirection=%s", Ascending))
	}
	if f.InProgressFor != nil {
		str.WriteString(fmt.Sprintf(", InProgressFor=%s", *f.InProgressFor))
	}
	str.WriteString("}")
	return str.String()
}
