package rbtmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
}

func NewConsumer(connection *amqp.Connection, exchange, exchangeType, queueName, ctag string) (*Consumer, error) {
	c := &Consumer{
		conn:    connection,
		channel: nil,
		tag:     ctag,
		// Done:    make(chan error),
	}

	var err error

	log.Printf("got Connection, getting channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("channel: %s", err)
	}

	if err = c.channel.ExchangeDeclare(
		"dead_letter", // name of the exchange
		"topic",       // type
		true,          // durable
		false,         // delete when complete
		false,         // internal
		false,         // noWait
		nil,           // arguments
	); err != nil {
		return nil, fmt.Errorf("Exchange Declare: %s", err)
	}
	_, err = c.channel.QueueDeclare(
		"dead_letter", // name of the queue
		true,          // durable
		false,         // delete when usused
		false,         // exclusive
		false,         // noWait
		nil,           // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}
	bindToQueue("dead_letter", "dead_letter", "*")

	log.Printf("got channel, declaring Exchange (%q)", exchange)
	if err = c.channel.ExchangeDeclare(
		exchange,     // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, fmt.Errorf("Exchange Declare: %s", err)
	}

	log.Printf("declared Exchange, declaring Queue %q", queueName)
	queueArgs := amqp.Table{
		"x-dead-letter-exchange": "dead_letter",
	}
	_, err = c.channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // noWait
		queueArgs, // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}
	bindToQueue(exchange, queueName, "*")

	return c, nil
}

// func (c *Consumer) Shutdown() error{
// 	// will close() the deliveries channel
// 	if err := c.channel.Cancel(c.tag, true); err != nil {
// 		return fmt.Errorf("Consumer cancel failed: %s", err)
// 	}

// 	if err := c.conn.Close(); err != nil {
// 		return fmt.Errorf("AMQP connection close error: %s", err)
// 	}

// 	defer log.Printf("AMQP shutdown OK")
// 	return nil
// 	// wait for handle() to exit
// 	// return <-c.Done
// }

func (c *Consumer) bindToQueue(exchange, queueName, key string) {
	if err := c.channel.QueueBind(
		queueName, // name of the queue
		key,       // bindingKey
		exchange,  // sourceExchange
		false,     // noWait
		nil,       // arguments
	); err != nil {
		fmt.Errorf("Queue Bind: %s", err)
	}

}

func (c *Consumer) Consume(queue string) <-chan amqp.Delivery {
	deliveries, err := c.channel.Consume(
		queue, // name
		c.tag, // consumerTag,
		false, // noAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		fmt.Errorf("Deliveries: %s", err)
		return nil
	}
	return deliveries
}
