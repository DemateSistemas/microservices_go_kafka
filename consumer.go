package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
)

func main() {
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalln("Failed to start Kafka consumer:", err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln("Failed to close Kafka consumer:", err)
		}
	}()

	topic := "meu-topico"
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalln("Failed to get partition list:", err)
	}

	var wg sync.WaitGroup
	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("Failed to start consumer for partition %d: %s\n", partition, err)
		}

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for msg := range pc.Messages() {
				log.Printf("Mensagem recebida: Partição: %d, Offset: %d, Valor: %s\n", msg.Partition, msg.Offset, string(msg.Value))
			}
		}(pc)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals

	wg.Wait()
}
