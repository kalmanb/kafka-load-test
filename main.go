package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var numProducers = 1
var numWorkers = 1 // per producer
var secondsToRun = 10
var topic = "pref-test-topic"

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(secondsToRun)*time.Second)
	defer cancel()

	total := make(chan int, 10)

	go func() {
		count := 0
		start := time.Now()
		for i := range total {
			count = count + i
			if count > 1000 {
				count = 0
				elapsed := time.Since(start)
				fmt.Printf("messages: %.0f m/s\n", 1000/elapsed.Seconds())
				start = time.Now()
			}
		}
	}()

	createProducers(ctx, total, numProducers, numWorkers)
	<-ctx.Done()
}

func createProducers(ctx context.Context, total chan<- int, producers, workers int) {
	for n := 0; n < producers; n++ {
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
		if err != nil {
			panic(err)
		}
		fmt.Printf("Created Producer %d\n", n)

		for i := 0; i < workers; i++ {
			go worker(ctx, total, p)
			fmt.Printf("Created Worker %d:%d\n", n, i)
		}
	}
}

func worker(ctx context.Context, total chan<- int, p *kafka.Producer) error {
	for {
		err := write(p, createMessage())
		if err != nil {
			log.Fatal(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case total <- 1:
		}
	}
}

func write(p *kafka.Producer, msg message) error {
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
