package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var numProducers = 2
var numWorkers = 10 // per producer
var numMessages = 2 // per worker
var topic = "pref-test-topic"

func main() {
	finished := make(chan int)
	var messageCount int32

	createProducers(finished, numProducers, numWorkers, numMessages, &messageCount)

	done := 0
	for i := range finished {
		done = done + i
		if done == numProducers*numWorkers {
			fmt.Println("Finished")
			fmt.Printf("Total Messages: %d\n", messageCount)
			os.Exit(1)
		}
	}
}

func createProducers(f chan<- int, producers, workers, messages int, messageCount *int32) {
	for n := 0; n < producers; n++ {
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
		if err != nil {
			panic(err)
		}
		fmt.Printf("Created Producer %d\n", n)

		for i := 0; i < workers; i++ {
			go worker(p, f, messageCount, messages)
			fmt.Printf("Created Worker %d:%d\n", n, i)
		}
	}
}

func worker(p *kafka.Producer, f chan<- int, messageCount *int32, n int) {
	for i := 0; i < n; i++ {
		err := write(p, createMessage())
		if err != nil {
			log.Fatal(err)
		}
		atomic.AddInt32(messageCount, 1)
	}
	f <- 1
}

func write(p *kafka.Producer, msg message) error {
	fmt.Printf("%+v\n", "Writing Message")
	deliveryChan := make(chan kafka.Event)

	value, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
		// Headers:        []kafka.Header{{"myTestHeader", []byte("header values are binary")}},
	}, deliveryChan)
	if err != nil {
		return err
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		return m.TopicPartition.Error
		// } else {
		// 	fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
		// 		*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)
	fmt.Println("Wrote Message")
	return nil
}

type message struct {
	Name      string `json:"name"`
	Address   string `json:"address"`
	Timestamp int64  `json:"timestamp"`
}

func createMessage() message {
	return message{
		"This my name",
		"And of course my full address, yes it is",
		time.Now().Unix(),
	}
}
