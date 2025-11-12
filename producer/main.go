package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

type Producer struct {
	producer sarama.SyncProducer
}

func NewProducer(brokers []string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	log.Printf("Connecting producer to broker: %v", brokers)
	producer, err := sarama.NewSyncProducer(brokers, config)

	if err != nil {
		return nil, err
	}

	return &Producer{
			producer: producer,
		},
		nil

}

func (p *Producer) SendMessage(topic, message string) (int32, int64, error) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partion, ofset, err := p.producer.SendMessage(msg)

	if err != nil {
		return -1, -1, err
	}

	return partion, ofset, nil
}

func (p *Producer) Close() error {
	return p.producer.Close()
}

func main() {
	fmt.Println("Hello world")
	brokers := []string{"localhost:9092"}
	topic := "test-topic"

	producer, err := NewProducer(brokers)
	if err != nil {
		log.Fatal("Failed to create producer: %v", err)
	}
	defer producer.Close()

	message := "Hello, Kafka baru lagi mantap"

	partion, offset, err := producer.SendMessage(topic, message)

	if err != nil {
		log.Printf("faild to send message: %v", err)
	} else {
		log.Printf("Message sent: partion=%d, offset=%d", partion, offset)
	}
}
