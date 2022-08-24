package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	name := *flag.String("name", "client", "name of the client")
	fmt.Println("RabbitMq!")
	file, err := os.Open("text.txt")
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	defer file.Close()
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	defer conn.Close()

	fmt.Println("Successfully connected to RabbitMQ Instance")

	ch, err := conn.Channel()

	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"Server",
		false,
		false,
		false,
		false,
		nil,
	)

	fmt.Println(q)

	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	err = ch.Publish(
		"",
		"TestQueue",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(name),
		},
	)

	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	_, err = ch.QueueDeclare(
		name,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	bin := make([]byte, 1024)
	for {
		n, err := file.Read(bin)
		if err != nil {
			fmt.Println(err)
			panic(err)
		}
		if n < 1024 {
			err = ch.Publish(
				"",
				name,
				false,
				false,
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        bin[:n],
				},
			)
			if err != nil {
				fmt.Println(err)
				panic(err)
			}

			err = ch.Publish(
				"",
				name,
				false,
				false,
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte("Stop"),
				},
			)
			if err != nil {
				fmt.Println(err)
				panic(err)
			}

			break
		}
		err = ch.Publish(
			"",
			name,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        bin,
			},
		)

		if err != nil {
			fmt.Println(err)
			panic(err)
		}
	}

	fmt.Println("Successfully Published Message to Queue")
	ch2, err := conn.Channel()

	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	defer ch2.Close()
	time.Sleep(100 * time.Millisecond)
	msgs, err := ch2.Consume(
		name+"*",
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	forever := make(chan bool)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	go func() {
		for d := range msgs {
			fmt.Printf("Received message: %s\n", d.Body)
			fmt.Println("Successfully received messages")
			forever <- true
			break
		}
	}()

	<-forever
}
