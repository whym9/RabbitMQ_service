package servers

import (
	"context"
	"fmt"
	"log"

	"rabbitmq_service/internal/metrics"

	"github.com/streadway/amqp"
	"google.golang.org/grpc"
)

var client Client

func Serve(addr string) {

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")

	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	defer conn.Close()

	ch, err := conn.Channel()

	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	con, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	client = NewClient(con)
	defer ch.Close()

	msgs, err := ch.Consume(
		"Server",
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	namechan := make(chan string)

	go func() {
		for d := range msgs {

			namechan <- string(d.Body)
		}
	}()

	fmt.Println("RabbitMQ server has started")

	for {

		name := <-namechan
		go Uploader(ch, name)
	}
}

func Uploader(ch *amqp.Channel, name string) {
	metrics.RecordMetrics()
	msgs, err := ch.Consume(
		name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	rec := []byte{}
	var mes string
	for d := range msgs {
		if string(d.Body) == "Stop" {
			mes, err = client.Upload(context.Background(), rec)
			if err != nil {
				log.Fatal(err)
			}
			err = Publisher(ch, mes, name+"*")
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("Successfully stopped receiving file!")
			break
		}
		rec = append(rec, d.Body...)
	}

}

func Publisher(ch *amqp.Channel, mes, name string) error {
	_, err := ch.QueueDeclare(
		name,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}
	err = ch.Publish(
		"",
		name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(mes),
		},
	)
	return err
}
