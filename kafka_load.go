package main

import (
	"flag"
	"fmt"
	"log"

	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
)

var kafka_topic *string = flag.String("kafka-topic", "collector1", `kafka topic to use`)
var kafka = flag.String("kafka", "localhost:10092", `listof kafka brokers.  example: "localhost:10092,localhost:10093"`)

const kafka_msg_meter = "esdb.msg.meter"

func kafka_test(concurrency int) {

	//Start Writers
	wg := new(sync.WaitGroup)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go kafka_writer(i, wg)
	}

	go logMetrics(kafka_msg_meter, concurrency)

	wg.Wait()
}

func kafka_writer(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	start_time := time.Now().Unix()

	client, err := sarama.NewClient("test-client", strings.Split(*kafka, ","), sarama.NewClientConfig())
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer client.Close()

	producer, err := sarama.NewProducer(client, nil)
	if err != nil {
		log.Fatalf("failed to create kafka producer: %v", err)
	}
	defer producer.Close()

	gen := metrics.NewMeter()
	metrics.GetOrRegister(kafka_msg_meter, gen)

	for i := 1; i < 10000000; i++ {
		if time.Now().Unix() > start_time+120 {
			break
		}

		key := fmt.Sprintf("key-%d", i)
		value := createdata(key)

		select {
		case producer.Input() <- &sarama.MessageToSend{
			Topic: *kafka_topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.ByteEncoder(value),
		}:
			gen.Mark(1)
		case err := <-producer.Errors():
			panic(err.Err)
		}
	}
}
