package main

import (
	"github.com/bshuster-repo/logrus-logstash-hook"
	"github.com/rcrowley/go-metrics"
	"github.com/rubyist/circuitbreaker"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"net"
	"strings"
	"time"
)

// Init logstash and rabbit URLs for connection
var (
	rabbitMQURL    = "amqp://guest:guest@localhost:5672"
	logStashURL    = "127.0.0.1:9998"
	log            *logrus.Logger
	contextLogger  *logrus.Entry
	rabbitLogs     *logrus.Logger
	prevRoutingKey string
)

// Define rabbitMQ struct for receiving messages with consecutive circuit breaker
type RabbitMQ struct {
	rmq       *amqp.Connection // rabbitMQ connection
	host      string           // url for rabbitMQ connection
	timeout   time.Duration    // func execution time out for circuit breaker
	ErrChan   chan error       // errors consumer channel
	rmqc      *amqp.Channel    // rabbitMQ channel
	circBreak *circuit.Breaker // circuit breaker
}

// Create new rabbit struct init Exchange Queue and Consume messages
func NewRabbitMQ(host string, timeout time.Duration, cb *circuit.Breaker) (*RabbitMQ, error) {
	// init rabbitMQ struct
	rm := &RabbitMQ{
		host:      host,
		timeout:   timeout,
		circBreak: cb,
		ErrChan:   make(chan error),
	}
	// init errors consumer
	failOnError(rm.ErrChan)

	// try to connect and init RabbitMQ if circuit breaker is not tripped
	var err error
	if !rm.circBreak.Tripped() {
		err = rm.Connect()
		if err != nil {
			return rm, err
		}
		err = rm.Listen()
	}
	return rm, err
}

// connect to rabbitMQ
func (r *RabbitMQ) Connect() error {

	var (
		err  error
		conn *amqp.Connection
	)

	// Creates a connection to RabbitMQ
	r.circBreak.Call(func() error {
		conn, err = amqp.Dial(rabbitMQURL)
		r.rmq = conn
		if err != nil {
			return ErrConnect(err.Error())
		}
		return nil
	}, r.timeout)
	if err != nil {
		return ErrConnect(err.Error())
	}
	return err
}

// Declare exchange queue bind queue with exchange and consume messages from RabbitMQ
func (r *RabbitMQ) Listen() error {
	ch, err := r.rmq.Channel()
	r.rmqc = ch

	err = ch.ExchangeDeclare(
		"plutus-logger", // name
		"topic",         // type
		true,            // durable
		false,           // auto-deleted
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unsused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)

	err = ch.QueueBind(
		q.Name,          // queue name
		"*.*",           // routing key
		"plutus-logger", // exchange
		false,
		nil)

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)

	// Listen in new goroutine
	go func() {
		// create new consecutive breaker
		cb := circuit.NewConsecutiveBreaker(10)
		// init circuit breaker listener
		// Subscribe to the circuit breaker events
		events := cb.Subscribe()

		var CurrentMessage amqp.Delivery
		go func() {
			for {
				e := <-events
				switch e {
				case circuit.BreakerTripped:
					cb.Reset()
					contextLogger.Fatalln("[x] service stopped")
				case circuit.BreakerFail:
					err := writeLogsToTCP(CurrentMessage.Body, CurrentMessage.RoutingKey)
					if err != nil {
						r.ErrChan <- err
					}
					continue
				}
			}
		}()

		for d := range msgs {
			CurrentMessage = d

			// write metrics to TCP send error or acknnowledge
			err := writeLogsToTCP(d.Body, d.RoutingKey)
			if err != nil {
				r.ErrChan <- err
			} else {
				d.Ack(false)
			}
		}
	}()
	contextLogger.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	return err
}

// Describes new error type ErrConnect which used if failed to connect remote services
type ErrConnect string

func (e ErrConnect) Error() string {
	return string(e)
}

// initialize logger
func init() {
	// create two loggers for self and for others
	log = logrus.New()
	rabbitLogs = logrus.New()

	//init connection for logstash hook
	conn, err := net.Dial("tcp", logStashURL)
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		log.Fatal(err)
	}

	// create logstash hook with formatter (json logstash format) to self and others logs
	hook := logrustash.New(conn, logrustash.DefaultFormatter(logrus.Fields{"type": "logs"}))
	if err != nil {
		log.Fatal(err)
	}

	// add hook to loggers
	log.Hooks.Add(hook)
	rabbitLogs.Hooks.Add(hook)

	contextLogger = log.WithFields(logrus.Fields{
		"service": "plutus-logger",
	})
}

func main() {
	// send self log about service starting
	contextLogger.Warningln("service started")

	// init rabbitMQ circuit breaker
	// init event listener
	// Subscribe to the circuit breaker events
	rcb := circuit.NewConsecutiveBreaker(10)
	events := rcb.Subscribe()

	// The circuit breaker events handling
	go func() {
		for {
			e := <-events
			switch e {
			case circuit.BreakerTripped:
				rcb.Reset()
				contextLogger.Fatal("[x] service stopped")
			case circuit.BreakerFail:
				rmq, err := NewRabbitMQ(rabbitMQURL, time.Second*10, rcb)
				if err != nil && rmq.ErrChan != nil {
					rmq.ErrChan <- err
				}
				continue
			}
		}
	}()

	// try to create and init RabbitMQ with circuit breaker timeout 10 and threashold 10
	rmq, err := NewRabbitMQ(rabbitMQURL, time.Second*10, rcb)
	if err != nil {
		if rmq.ErrChan != nil {
			rmq.ErrChan <- err
		} else {
			contextLogger.Println(err.Error())
		}
	}
	// try to defer close rabbitMQ channel and connection if exist
	if rmq.rmq != nil {
		defer rmq.rmq.Close()
	}
	if rmq.rmqc != nil {
		defer rmq.rmqc.Close()
	}

	//init self system metrics
	sysMetrics := metrics.NewRegistry()
	metrics.RegisterDebugGCStats(sysMetrics)
	metrics.RegisterRuntimeMemStats(sysMetrics)

	// init rabbitmqwriter which will write this service metrics
	rabbitmqwriter := NewRabbitMQWriter()
	rabbitmqwriter.Init()

	// periodically capture metrics values and write to rabbitMQ
	metricsDuration := time.Second * 10
	go metrics.CaptureDebugGCStats(sysMetrics, metricsDuration)
	go metrics.CaptureRuntimeMemStats(sysMetrics, metricsDuration)
	go metrics.WriteJSON(sysMetrics, metricsDuration, rabbitmqwriter)

	// wait messages from rabbitMQ
	forever := make(chan bool)
	<-forever

}

// Is used for error handling
func failOnError(errChan <-chan error) {
	go func() {
		for {
			err := <-errChan
			if err != nil {
				switch err.(type) {
				case ErrConnect:
					contextLogger.Printf(err.Error())
				default:
					contextLogger.Fatalf(err.Error())
				}
			}
		}
	}()
}

// write logs to TCP (elastic logstash)
func writeLogsToTCP(jsonData []byte, routingKey string) error {

	// split rountingKey to get service-name and log level
	routingSubKeys := strings.Split(routingKey, `.`)
	serviceName := routingSubKeys[0]
	logLevel := routingSubKeys[1]

	// add service-name field
	rabbitContextLogger := rabbitLogs.WithFields(logrus.Fields{
		"service": serviceName,
	})

	// send log corresponding to the level
	switch logLevel {
	case "panic":
		rabbitContextLogger.Panicln(string(jsonData))
	case "fatal":
		rabbitContextLogger.Fatalln(string(jsonData))
	case "error":
		rabbitContextLogger.Errorln(string(jsonData))
	case "warn":
		rabbitContextLogger.Warnln(string(jsonData))
	case "info":
		rabbitContextLogger.Infoln(string(jsonData))
	case "debug":
		rabbitContextLogger.Debugln(string(jsonData))
	default:
		return nil
	}

	return nil
}
