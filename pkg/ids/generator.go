//go:generate mockgen -source generator.go -destination mocks/generator_mock.go -package mocks IDGenerator

package ids

import (
	"context"
)

type (
	IDGenerator interface {
		GetID(ctx context.Context, entityType EntityType) (string, error)
	}

	EntityType int
)

const (
	MapTask EntityType = iota
	ReduceTask
)

func (et EntityType) String() string {
	return [...]string{
		"MapTask",
		"ReduceTask",
	}[et]
}
