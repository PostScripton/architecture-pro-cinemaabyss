package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/IBM/sarama"
)

var producer sarama.SyncProducer

func main() {
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")

	var err error
	producer, err = newProducer(brokers)
	if err != nil {
		log.Printf("warning: failed to connect to Kafka producer: %v", err)
	} else {
		defer producer.Close()
		log.Printf("producer connected to Kafka at %v", brokers)

		topics := []string{"user-events", "payment-events", "movie-events"}
		go startConsumer(brokers, topics)
	}

	http.HandleFunc("/api/events/health", handleHealth)
	http.HandleFunc("/api/events/movie", makeEventHandler("movie", "movie-events"))
	http.HandleFunc("/api/events/user", makeEventHandler("user", "user-events"))
	http.HandleFunc("/api/events/payment", makeEventHandler("payment", "payment-events"))

	port := getEnv("PORT", "8082")
	log.Printf("starting events-service on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func newProducer(brokers []string) (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	return sarama.NewSyncProducer(brokers, cfg)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func makeEventHandler(eventType, topic string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		if producer == nil {
			http.Error(w, "kafka unavailable", http.StatusServiceUnavailable)
			return
		}

		data, _ := json.Marshal(map[string]interface{}{
			"type":    eventType,
			"payload": payload,
		})

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(data),
		}

		_, _, err := producer.SendMessage(msg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		log.Printf("[producer] published %s event to topic=%s payload=%s", eventType, topic, string(data))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"status": "success", "topic": topic})
	}
}

// eventConsumer implements sarama.ConsumerGroupHandler.
type eventConsumer struct{}

func (c *eventConsumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (c *eventConsumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (c *eventConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("[consumer] topic=%s partition=%d offset=%d value=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		session.MarkMessage(msg, "")
	}
	return nil
}

func startConsumer(brokers []string, topics []string) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup(brokers, "events-service-group", cfg)
	if err != nil {
		log.Printf("warning: failed to create consumer group: %v", err)
		return
	}
	defer group.Close()

	log.Printf("consumer started, listening on topics: %v", topics)
	handler := &eventConsumer{}

	for {
		if err := group.Consume(context.Background(), topics, handler); err != nil {
			log.Printf("consumer error: %v", err)
			return
		}
	}
}
