package main

import (
	"encoding/json"
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

type User struct {
	Event  string `json:"event"`
	UserId int64  `json:"userId"`
}

func main() {
	fmt.Println("Hello world")
	brokers := []string{"localhost:9092"}
	topic := "test-topic"

	producer, err := NewProducer(brokers)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	userNew := User{
		Event:  "UserCreated",
		UserId: 1,
	}

	byteJson, err := json.Marshal(userNew)

	if err != nil {
		log.Fatalf("Failed to marshal json")
	}

	message := string(byteJson)

	partion, offset, err := producer.SendMessage(topic, message)

	if err != nil {
		log.Printf("faild to send message: %v", err)
	} else {
		log.Printf("Message sent: partion=%d, offset=%d", partion, offset)
	}
}
