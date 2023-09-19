//go:generate mockgen -source producer.go -destination mocks/producer_mock.go -package mocks Producer

package events

import "context"

type (
	Producer interface {
		Produce(ctx context.Context, event Event) error
		Close(ctx context.Context) error
	}
)
