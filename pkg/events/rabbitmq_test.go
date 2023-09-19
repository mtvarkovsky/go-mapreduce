package events

import (
	"context"
	"github.com/mtvarkovsky/go-mapreduce/pkg/config"
	"github.com/mtvarkovsky/go-mapreduce/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
)

func newRabbitMQContainer(t *testing.T) (testcontainers.Container, config.RabbitMQ) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:latest",
		ExposedPorts: []string{"5672:5672/tcp", "15672:15672/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForLog("Server startup complete"),
			wait.ForListeningPort("5672/tcp"),
		),
	}
	cntnr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	assert.NoError(t, err)
	cfg := config.RabbitMQ{
		URL:              "amqp://guest:guest@localhost",
		QueueName:        "TaskEvents",
		EventsBufferSize: 100,
	}
	return cntnr, cfg
}

func TestRabbitMQProducer_Integration_Produce(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	cntnr, cfg := newRabbitMQContainer(t)
	defer cntnr.Terminate(context.Background())
	prdcr, err := NewRabbitMQProducer(cfg, logger.NewTestZapLogger())
	assert.NoError(t, err)

	err = prdcr.Produce(context.Background(), Event{
		Type: MapTaskCreated,
	})
	assert.NoError(t, err)
}

func TestRabbitMQProducer_Integration_Close(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	cntnr, cfg := newRabbitMQContainer(t)
	defer cntnr.Terminate(context.Background())
	prdcr, err := NewRabbitMQProducer(cfg, logger.NewTestZapLogger())
	assert.NoError(t, err)

	err = prdcr.Close(context.Background())
	assert.NoError(t, err)

	err = prdcr.Produce(context.Background(), Event{
		Type: MapTaskCreated,
	})
	assert.Error(t, err)
}

func TestNewRabbitMQConsumer_Integration_Consume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	cntnr, cfg := newRabbitMQContainer(t)
	defer cntnr.Terminate(context.Background())

	prdcr, err := NewRabbitMQProducer(cfg, logger.NewTestZapLogger())
	assert.NoError(t, err)
	cnsmr, err := NewRabbitMQConsumer(cfg, logger.NewTestZapLogger())
	assert.NoError(t, err)

	events, err := cnsmr.Consume()
	assert.NoError(t, err)

	eventSent := Event{
		Type: MapTaskCreated,
	}

	err = prdcr.Produce(context.Background(), eventSent)
	assert.NoError(t, err)

	eventReceived := <-events

	assert.Equal(t, eventSent, eventReceived)
}

func TestNewRabbitMQConsumer_Integration_Close(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
		return
	}
	cntnr, cfg := newRabbitMQContainer(t)
	defer cntnr.Terminate(context.Background())

	prdcr, err := NewRabbitMQProducer(cfg, logger.NewTestZapLogger())
	assert.NoError(t, err)
	cnsmr, err := NewRabbitMQConsumer(cfg, logger.NewTestZapLogger())
	assert.NoError(t, err)

	events, err := cnsmr.Consume()
	assert.NoError(t, err)

	eventSent := Event{
		Type: MapTaskCreated,
	}

	err = prdcr.Produce(context.Background(), eventSent)
	assert.NoError(t, err)

	eventReceived := <-events

	assert.Equal(t, eventSent, eventReceived)

	err = cnsmr.Close()
	assert.NoError(t, err)

	_, ok := <-events
	assert.False(t, ok)
}
