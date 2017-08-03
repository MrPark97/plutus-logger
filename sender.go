package main

import (
	"github.com/streadway/amqp"
)

// declare useful struct for writing metrics
type RabbitMQWriter struct {
	*amqp.Connection
	*amqp.Channel
}

// constructor for RabbitMQWriter
func NewRabbitMQWriter() *RabbitMQWriter {
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatal(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	return &RabbitMQWriter{conn, ch}
}

// declare method Write() to realize Writer interface
func (r *RabbitMQWriter) Write(p []byte) (n int, err error) {
	err = r.Channel.Publish(
		"plutus-metrics",                // exchange
		"plutus-metrics-service.system", // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        p,
		})
	return len(p), err
}

// declare initialization method
func (r *RabbitMQWriter) Init() {
	err := r.Channel.ExchangeDeclare(
		"plutus-metrics", // name
		"topic",          // type
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		log.Fatal(err)
	}
}
