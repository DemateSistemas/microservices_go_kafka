package main

import (
	"log"

	"github.com/Shopify/sarama"
)

func main() {
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalln("Failed to start Kafka producer:", err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln("Failed to close Kafka producer:", err)
		}
	}()

	topic := "meu-topico"
	message := "Olá, Kafka!"

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalln("Failed to send message to Kafka:", err)
	}

	log.Printf("Mensagem enviada com sucesso! Partição: %d, Offset: %d\n", partition, offset)
}
