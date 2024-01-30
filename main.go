package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func main() {

	config := sarama.NewConfig()
	brokers := []string{"0.0.0.0:29092"}
	topic := "important"

	//Redis config
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	defer client.Close()

	http.HandleFunc("/api/post", func(w http.ResponseWriter, r *http.Request) {
		var data string
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&data); err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		go func() {
			config.Producer.RequiredAcks = sarama.WaitForAll
			config.Producer.Retry.Max = 5
			producer, errProd := sarama.NewSyncProducer(brokers, config)

			if errProd != nil {
				// Should not reach here
				panic(errProd)
			}

			defer func() {
				if err := producer.Close(); err != nil {
					// Should not reach here
					panic(err)
				}
			}()

			msg := &sarama.ProducerMessage{
				Topic: topic,
				Key: sarama.StringEncoder(uuid.New().String()),
				Value: sarama.StringEncoder(data),
			}

			producer.SendMessage(msg)
		}()

		go func() {
			config.Consumer.Return.Errors = true
			master, errSub := sarama.NewConsumer(brokers, config)
			if errSub != nil {
				panic(errSub)
			}

			defer func() {
				if err := master.Close(); err != nil {
					panic(err)
				}
			}()

			consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetOldest)
			if err != nil {
				panic(err)
			}

			signals := make(chan os.Signal, 1)
			signal.Notify(signals, os.Interrupt)

			// Count how many message processed
			msgCount := 0

			// Get signnal for finish
			doneCh := make(chan struct{})
			go func() {

				config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
				config.Consumer.Offsets.Initial = sarama.OffsetOldest
				for {
					select {
					case err := <-consumer.Errors():
						fmt.Println(err)
					case msg := <-consumer.Messages():
						msgCount++
						ctx := context.Background()
						err := client.Set(ctx, string(msg.Key), string(msg.Value), 0).Err()
						if err != nil {
							panic(err)
						}
					case <-signals:
						fmt.Println("Interrupt is detected")
						doneCh <- struct{}{}
					}
				}
			}()

			<-doneCh
			fmt.Println("Processed", msgCount, "messages")
		}()

	})

	http.HandleFunc("/api/get", func(w http.ResponseWriter, r *http.Request) {
		// Use the Scan function to iterate over all keys
		ctx := context.Background()
		iter := client.Scan(ctx, 0, "*", 0).Iterator()
		var results []string
		var wg sync.WaitGroup

		// Prepare a slice to hold the results

		// Use a WaitGroup to wait for all goroutines to finish
		//var wg sync.WaitGroup

		for iter.Next(ctx) {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				value, err := client.Get(ctx, key).Result()
				if err == nil {
					results = append(results, fmt.Sprintf("Key: %s, Value: %s", key, value))
				}
			}(iter.Val())
		}
		
			wg.Wait()
			closeResults(w, results)
		

		if err := iter.Err(); err != nil {
			http.Error(w, "Error during iteration", http.StatusInternalServerError)
			return
		}
	})

	fmt.Println("Server is running on http://localhost:8080")
	http.ListenAndServe(":8080", nil)
}

func closeResults(w http.ResponseWriter, results []string) {
	// Respond with the results
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Convert the results to JSON and write to the response
	json.NewEncoder(w).Encode(results)

}
