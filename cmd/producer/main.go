package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	data := readCsvFile()

	conf := kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	topic := "driver-location"
	p, err := kafka.NewProducer(&conf)

	if err != nil {
		fmt.Printf("Failed to create producer: %s", err)
		os.Exit(1)
	}

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	for _, record := range data {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(record[0]),
			Value:          []byte(record[0]),
		}, nil)
		time.Sleep(15 * time.Second)
	}

	// Wait for all messages to be delivered
	p.Flush(5 * 1000)
	p.Close()
}

func readCsvFile() [][]string {
	file, err := os.Open("cmd/producer/driverlogs.csv")

	// Checks for the error
	if err != nil {
		log.Fatal("Error while reading the file", err)
	}

	// Closes the file
	defer file.Close()

	reader := csv.NewReader(file)

	records, err := reader.ReadAll()

	if err != nil {
		fmt.Println("Error reading records")
	}

	for _, eachrecord := range records {
		fmt.Println(eachrecord)
	}
	return records
}
