package main

import (
	"fmt"

	"github.com/Shopify/sarama"
)

const (
	portaddress = "localhost:9092"
	topic       = "test"
)

func main() {
	fmt.Println("This is Message Producer")
	producer, err := producerInitlization()
	if err != nil {
		panic(err)
	}
	var msg1 string
	for {
		fmt.Println("Please Enter message you want to Send")
		fmt.Scanf("%s", &msg1)
		publishMessage(msg1, producer)
		defer func() {
			if err := producer.Close(); err != nil {
				panic(err)
			}
		}()

	}
}
func producerInitlization() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{portaddress}, config)
	return producer, err
}
func publishMessage(message string, producer sarama.SyncProducer) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
}
