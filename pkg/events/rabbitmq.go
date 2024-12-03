package events

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mtvarkovsky/go-mapreduce/pkg/config"
	"github.com/mtvarkovsky/go-mapreduce/pkg/errors"
	"github.com/mtvarkovsky/go-mapreduce/pkg/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

type (
	RabbitMQProducer struct {
		log  logger.Logger
		cfg  config.RabbitMQ
		conn *amqp.Connection
		ch   *amqp.Channel
		q    amqp.Queue
	}

	RabbitMQConsumer struct {
		log    logger.Logger
		events chan Event
		done   chan bool
		cfg    config.RabbitMQ
		conn   *amqp.Connection
		ch     *amqp.Channel
		q      amqp.Queue
	}
)

func getConnection(config config.RabbitMQ) (*amqp.Connection, error) {
	return amqp.Dial(config.URL)
}

func getChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	return conn.Channel()
}

func getQueue(channel *amqp.Channel, config config.RabbitMQ) (amqp.Queue, error) {
	return channel.QueueDeclare(
		config.QueueName,
		true,
		false,
		false,
		true,
		nil,
	)
}

func NewRabbitMQProducer(config config.RabbitMQ, log logger.Logger) (*RabbitMQProducer, error) {
	l := log.Logger("RabbitMQProducer")
	l.Infof("start new event producer")
	conn, err := getConnection(config)
	if err != nil {
		l.Errorf("can't connect to rabbitmq instance at=%s: (%s)", config.URL, err.Error())
		return nil, fmt.Errorf("can't connect to rabbitmq instance at url=%s: %w", config.URL, err)
	}
	ch, err := getChannel(conn)
	if err != nil {
		l.Errorf("can't get rabbitmq channel: (%s)", err.Error())
		return nil, fmt.Errorf("can't get rabbitmq channel: %w", err)
	}
	q, err := getQueue(ch, config)
	if err != nil {
		l.Errorf("can't get rabbitmq queue=%s: (%s)", config.QueueName, err.Error())
		return nil, fmt.Errorf("can't get rabbitqm queue=%s: %w", config.QueueName, err)
	}
	p := &RabbitMQProducer{
		log:  l,
		conn: conn,
		ch:   ch,
		q:    q,
		cfg:  config,
	}
	return p, nil
}

func (p *RabbitMQProducer) Produce(ctx context.Context, event Event) error {
	p.log.Infof("produce event=%s", event.Type)
	evnt, err := json.Marshal(event)
	if err != nil {
		p.log.Errorf("can't marshall event=%s: (%s)", event.Type, err.Error())
		return fmt.Errorf("can't marshall event=%s: %w", event, errors.ErrInternal)
	}
	err = p.ch.PublishWithContext(
		ctx,
		"",
		p.q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        evnt,
		},
	)
	if err != nil {
		p.log.Errorf("can't produce event=%s: (%s)", event.Type, err.Error())
		return fmt.Errorf("can't produce event=%s: %w", event, errors.ErrInternal)
	}
	return nil
}

func (p *RabbitMQProducer) Close(ctx context.Context) error {
	p.log.Infof("stop event producer")
	err := p.conn.Close()
	if err != nil {
		p.log.Errorf("can't stop event producer: (%s)", err.Error())
		return fmt.Errorf("can't close rabbitmq publisher: %w", errors.ErrInternal)
	}
	return nil
}

func NewRabbitMQConsumer(config config.RabbitMQ, log logger.Logger) (*RabbitMQConsumer, error) {
	l := log.Logger("RabbitMQConsumer")
	l.Infof("start new event pconsumer")
	conn, err := getConnection(config)
	if err != nil {
		l.Errorf("can't connect to rabbitmq instance at=%s: (%s)", config.URL, err.Error())
		return nil, fmt.Errorf("can't connect to rabbitmq instance at url=%s: %w", config.URL, err)
	}
	ch, err := getChannel(conn)
	if err != nil {
		l.Errorf("can't get rabbitmq channel: (%s)", err.Error())
		return nil, fmt.Errorf("can't get rabbitmq channel: %w", err)
	}
	q, err := getQueue(ch, config)
	if err != nil {
		l.Errorf("can't get rabbitmq queue=%s: (%s)", config.QueueName, err.Error())
		return nil, fmt.Errorf("can't get rabbitqm queue=%s: %w", config.QueueName, err)
	}
	c := &RabbitMQConsumer{
		log:    l,
		conn:   conn,
		ch:     ch,
		q:      q,
		cfg:    config,
		events: make(chan Event, config.EventsBufferSize),
		done:   make(chan bool),
	}
	return c, nil
}

func (c *RabbitMQConsumer) getEvents() (<-chan amqp.Delivery, error) {
	return c.ch.Consume(
		c.q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
}

func (c *RabbitMQConsumer) processEvents(events <-chan amqp.Delivery) {
	c.log.Infof("start processing events")
	for {
		select {
		case e := <-events:
			event, err := c.toEvent(e)
			if err == nil {
				c.log.Infof("received event=%s", event.Type)
				c.events <- event
			} else {
				c.log.Warnf("can't process received event: (%s)", err.Error())
			}
		case <-c.done:
			c.log.Infof("stop processing events")
			return
		}
	}
}

func (c *RabbitMQConsumer) toEvent(e amqp.Delivery) (Event, error) {
	var event Event
	err := json.Unmarshal(e.Body, &event)
	if err != nil {
		return Event{}, err
	}
	return event, nil
}

func (c *RabbitMQConsumer) Consume() (<-chan Event, error) {
	c.log.Infof("start consuming events")
	events, err := c.getEvents()
	if err != nil {
		c.log.Errorf("can't consume events: (%s)", err.Error())
		return nil, err
	}
	go c.processEvents(events)
	return c.events, nil
}

func (c *RabbitMQConsumer) Close() error {
	c.log.Infof("stop consuming events")
	c.done <- true
	close(c.events)
	err := c.conn.Close()
	if err != nil {
		c.log.Errorf("can't close rabbitmq connection: (%s)", err.Error())
		return fmt.Errorf("can't close rabbitmq consumer: %w", err)
	}

	return nil
}
