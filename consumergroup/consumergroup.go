package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
)

func main() {
	fmt.Println("This is message consumer")
	topic := "test"
	consugroup := "consumergroup"
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = 10 * time.Second
	config.Consumer.Return.Errors = true
	brokers := []string{"localhost:2181", "localhost:2181"}
	congroup, err := consumergroup.JoinConsumerGroup(consugroup, []string{topic}, brokers, config)
	//master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := congroup.Close(); err != nil {
			panic(err)
		}
	}()

	// consumer, err := congroup.ConsumePartition(topic, 0, sarama.OffsetOldest)
	// if err != nil {
	// 	panic(err)
	// }
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	msgCount := 0
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-congroup.Errors():
				fmt.Println(err)
			case msg := <-congroup.Messages():
				if msg.Topic != topic {
					continue
				}
				fmt.Println("Topic :", msg.Topic)
				fmt.Println("value", string(msg.Value))
				err := congroup.CommitUpto(msg)
				if err != nil {
					fmt.Println("Error commit zookeeper: ", err.Error())
				}
				fmt.Println("Received messages", string(msg.Key), string(msg.Value))
			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
}
