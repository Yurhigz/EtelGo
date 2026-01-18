package main

import (
	"context"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	seeds := []string{"127.0.0.1:9094"}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		// kgo.ConsumerGroup("my-group-identifier"),
		kgo.ConsumeTopics("tests"),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ctx := context.Background()
	log.Println("Connecting to Kafka...")

	log.Println("Connected to Kafka, waiting for messages...")

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Printf("Fetch errors: %v", errs)
			continue
		}

		// // We can iterate through a record iterator...
		// iter := fetches.RecordIter()
		// for !iter.Done() {
		// 	record := iter.Next()
		// 	fmt.Println(string(record.Value), "from an iterator!")
		// }

		// or a callback function.
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, record := range p.Records {
				fmt.Println(string(record.Key), string(record.Value))
			}
		})
	}
}
