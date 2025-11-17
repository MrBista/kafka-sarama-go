package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

// ----------------------------------------
// Consumer

// Consumer represents a Kafka consumer
type Consumer struct {
	consumer           sarama.Consumer
	partitionConsumers map[int32]sarama.PartitionConsumer
}

// NewConsumer creates a new Consumer.
func NewConsumer(brokers []string) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	log.Printf("Connecting consumer to Kafka brokers: %v", brokers)
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &Consumer{consumer: consumer, partitionConsumers: make(map[int32]sarama.PartitionConsumer)}, nil
}

// Consume starts consuming messages from a topic.
func (c *Consumer) Consume(topic string, partition int32) (
	<-chan *sarama.ConsumerMessage, <-chan *sarama.ConsumerError) {

	log.Printf("Consuming partition %d of topic %s", partition, topic)
	pc, err := c.consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Printf("Failed to start consumer for partition %d: %v\n", partition, err)
		return nil, nil
	}

	c.partitionConsumers[partition] = pc

	return pc.Messages(), pc.Errors()
}

// ClosePartition closes the partition consumer for a given partition.
func (c *Consumer) ClosePartition(partition int32) error {
	if pc, ok := c.partitionConsumers[partition]; ok {
		err := pc.Close()
		delete(c.partitionConsumers, partition)
		return err
	}
	return nil
}

// Close closes the consumer.
func (c *Consumer) Close() error {
	return c.consumer.Close()
}

type User struct {
	Event  string `json:"event"`
	UserId int64  `json:"userId"`
}

func main() {
	brokers := []string{"localhost:9092"}
	topic := "test-topic"
	partition := int32(0)

	consumer, err := NewConsumer(brokers)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	messages, errors := consumer.Consume(topic, partition)

	var wg sync.WaitGroup
	wg.Add(2)

	// Handle received messages
	go func() {
		defer wg.Done()
		for msg := range messages {
			log.Printf("Received message: partition=%d, "+
				"offset=%d, value=%s",
				msg.Partition, msg.Offset, string(msg.Value))
			jsonData := new(User)
			errParse := json.Unmarshal(msg.Value, &jsonData)
			if errParse != nil {
				log.Printf("Faild read json")
			}
			if jsonData != nil {
				fmt.Printf("Event received: ", jsonData.Event)

			}

		}
	}()

	// Handle errors
	go func() {
		defer wg.Done()
		for err := range errors {
			log.Printf("Consumer error: %v", err)
		}
	}()

	// Wait for a termination signal (Ctrl+C)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	<-signals
	log.Println("Received termination signal, shutting down...")

	// Close the partition consumer to stop consuming messages and errors
	err = consumer.ClosePartition(partition)
	if err != nil {
		log.Printf("Failed to close partition consumer: %v", err)
		return
	}

	// Wait for all goroutines to finish
	wg.Wait()
}
