package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
)

const (
	portaddress = "localhost:2181"
	topic       = "test"
	consugroup  = "consumergroup"
)

func main() {
	fmt.Println("This is message consumer 2")
	congroup, err := consumerInitilisation()
	defer func() {
		if err := congroup.Close(); err != nil {
			panic(err)
		}
	}()
	if err != nil {
		panic(err)
	}
	consumeMessage(congroup)
}
func consumerInitilisation() (*consumergroup.ConsumerGroup, error) {
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = 10 * time.Second
	config.Consumer.Return.Errors = true
	congroup, err := consumergroup.JoinConsumerGroup(consugroup, []string{topic}, []string{portaddress}, config)
	if err != nil {
		panic(err)
	}
	return congroup, err
}

func consumeMessage(congroup *consumergroup.ConsumerGroup) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
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
}
