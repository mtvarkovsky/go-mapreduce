//go:generate mockgen -source consumer.go -destination mocks/consumer_mock.go -package mocks Consumer

package events

type (
	Consumer interface {
		Consume() (<-chan Event, error)
		Close() error
	}
)
