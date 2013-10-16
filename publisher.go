package rbtmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

type Publisher struct {
	conn       *amqp.Connection
	channel    *amqp.Channel
	routingKey string
	exchange   string
}

func NewPublisher(connection *amqp.Connection, exchange string, exchangeType string, queue string, routingKey string) (*Publisher, error) {
	publisher := new(Publisher)
	publisher.conn = connection

	channel, err := connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}
	publisher.channel = channel

	if err := publisher.channel.ExchangeDeclare(
		exchange,     // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, fmt.Errorf("Exchange Declare: %s", err)
	}

	_, err = channel.QueueDeclare(
		exchange, // name of the queue
		true,           // durable
		false,          // delete when usused
		false,          // exclusive
		false,          // noWait
		nil,            // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}
	if err := channel.QueueBind(
		queue, // name of the queue
		routingKey,     // bindingKey
		exchange,       // sourceExchange
		false,          // noWait
		nil,            // arguments
	); err != nil {
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}
	publisher.exchange = exchange
	publisher.routingKey = routingKey

	return publisher, nil
}

func (p *Publisher) Publish(body []byte, reliable bool) error {
	if reliable {
		if err := p.channel.Confirm(false); err != nil {
			return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
		}

		ack, nack := p.channel.NotifyConfirm(make(chan uint64, 1), make(chan uint64, 1))
		defer confirmOne(ack, nack)
	}

	if err := p.channel.Publish(
		p.exchange,   // publish to an exchange
		p.routingKey, // routing to 0 or more queues
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            body,
			DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
			Priority:        0,               // 0-9
		},
	); err != nil {
		return fmt.Errorf("Exchange Publish: %s", err)
	}

	return nil
}

func confirmOne(ack, nack chan uint64) {
	log.Printf("waiting for confirmation of one publishing")

	select {
	case tag := <-ack:
		log.Printf("confirmed delivery with delivery tag: %d", tag)
	case tag := <-nack:
		log.Printf("failed delivery of delivery tag: %d", tag)
	}
}
