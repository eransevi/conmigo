package providers

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

type RMQProvider struct {
	CommunicationProvider

	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue

	host		 string
	exchangeName string
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func NewRMQProvider(host string) *RMQProvider {
	return &RMQProvider{host: host}
}

func (cp *RMQProvider) Initialize(actionName string) {
	//conn, err := amqp.Dial(os.Getenv("RABBITMQ_HOST"))
	conn, err := amqp.Dial(cp.host)
	failOnError(err, "Failed to connect to RabbitMQ")

	cp.exchangeName = actionName
	cp.conn = conn

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	cp.channel = ch

	err = ch.ExchangeDeclare(
		cp.exchangeName, // name
		"direct",        // type
		true,            // durable
		false,           // auto-deleted
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",    // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	cp.queue = q

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")
}

func (cm *RMQProvider) Close() {
	if cm.channel != nil {
		cm.channel.Close()
	}
	if cm.conn != nil {
		cm.conn.Close()
	}
}

func (cm *RMQProvider) Subscribe(channel string, callback func(string)) error {
	if cm.channel == nil {
		return errors.New("Communication Provider is not initialized")
	}

	err := cm.channel.QueueBind(
		cm.queue.Name,   // queue name
		channel,         // routing key
		cm.exchangeName, // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	msgs, err := cm.channel.Consume(
		cm.queue.Name, // queue
		"",            // consumer
		true,          // auto ack
		false,         // exclusive
		false,         // no local
		false,         // no wait
		nil,           // args
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for d := range msgs {
			callback(fmt.Sprintf("%s", d.Body))
		}
	}()

	return err
}

func (cm *RMQProvider) Publish(channel string, eventData []byte) error {
	if cm.channel == nil {
		return errors.New("Communication Provider is not initialized")
	}

	err := cm.channel.Publish(
		cm.exchangeName, // exchange
		channel,         // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(eventData),
		})
	failOnError(err, "Failed to publish a message")

	return err
}
