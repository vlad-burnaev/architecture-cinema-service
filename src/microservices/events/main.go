package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// Event models
type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

type EventRequest struct {
	Type string                 `json:"type"`
	Data map[string]interface{} `json:"data"`
}

type EventResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	EventID string `json:"event_id"`
}

var (
	kafkaProducer sarama.SyncProducer
	kafkaBrokers  []string
)

func main() {
	log.Println("Starting Events Service...")

	// Load configuration
	port := getEnv("PORT", "8082")
	kafkaBrokersStr := getEnv("KAFKA_BROKERS", "kafka:9092")
	kafkaBrokers = strings.Split(kafkaBrokersStr, ",")

	log.Printf("Kafka Brokers: %v", kafkaBrokers)

	// Initialize Kafka Producer
	initKafkaProducer()
	defer kafkaProducer.Close()

	// Start Kafka Consumer in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go startKafkaConsumer(ctx)

	// Setup HTTP routes
	http.HandleFunc("/api/events", handleEvents)
	http.HandleFunc("/api/events/health", handleHealth)
	http.HandleFunc("/api/events/user", handleUserEvent)
	http.HandleFunc("/api/events/movie", handleMovieEvent)
	http.HandleFunc("/api/events/payment", handlePaymentEvent)

	// Start HTTP server
	log.Printf("Events Service listening on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func initKafkaProducer() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_7_0_0

	var err error
	kafkaProducer, err = sarama.NewSyncProducer(kafkaBrokers, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}

	log.Println("Kafka Producer initialized successfully")
}

func startKafkaConsumer(ctx context.Context) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Version = sarama.V2_7_0_0

	consumer, err := sarama.NewConsumer(kafkaBrokers, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer consumer.Close()

	log.Println("Kafka Consumer initialized successfully")

	// Subscribe to all event topics
	topics := []string{"user-events", "movie-events", "payment-events"}

	for _, topic := range topics {
		go consumeTopic(ctx, consumer, topic)
	}

	// Wait for context cancellation
	<-ctx.Done()
	log.Println("Kafka Consumer shutting down")
}

func consumeTopic(ctx context.Context, consumer sarama.Consumer, topic string) {
	// Retry logic for topic subscription
	var partitionConsumer sarama.PartitionConsumer
	var err error

	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		partitionConsumer, err = consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
		if err == nil {
			break
		}

		log.Printf("Failed to start consumer for topic %s (attempt %d/%d): %v", topic, i+1, maxRetries, err)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		log.Printf("❌ Could not start consumer for topic %s after %d attempts", topic, maxRetries)
		return
	}

	defer partitionConsumer.Close()

	log.Printf("✅ Started consuming topic: %s", topic)

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			processEvent(msg)
		case err := <-partitionConsumer.Errors():
			log.Printf("Error consuming from topic %s: %v", topic, err)
		case <-ctx.Done():
			return
		}
	}
}

func processEvent(msg *sarama.ConsumerMessage) {
	var event Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("Failed to unmarshal event: %v", err)
		return
	}

	log.Printf("📩 [CONSUMED] Topic: %s | Event ID: %s | Type: %s | Timestamp: %s | Data: %v",
		msg.Topic, event.ID, event.Type, event.Timestamp.Format(time.RFC3339), event.Data)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  true,
		"service": "events-service",
	})
}

// Handler for user events (Postman compatibility)
func handleUserEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		log.Printf("Failed to decode request: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        generateEventID(),
		Type:      "user",
		Timestamp: time.Now(),
		Data:      data,
	}

	if err := publishEvent("user-events", event); err != nil {
		log.Printf("Failed to publish event: %v", err)
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	log.Printf("📤 [PRODUCED] Topic: user-events | Event ID: %s | Type: user", event.ID)

	response := map[string]interface{}{
		"status":   "success",
		"success":  true,
		"message":  "Event published to topic: user-events",
		"event_id": event.ID,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// Handler for movie events (Postman compatibility)
func handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		log.Printf("Failed to decode request: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        generateEventID(),
		Type:      "movie",
		Timestamp: time.Now(),
		Data:      data,
	}

	if err := publishEvent("movie-events", event); err != nil {
		log.Printf("Failed to publish event: %v", err)
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	log.Printf("📤 [PRODUCED] Topic: movie-events | Event ID: %s | Type: movie", event.ID)

	response := map[string]interface{}{
		"status":   "success",
		"success":  true,
		"message":  "Event published to topic: movie-events",
		"event_id": event.ID,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// Handler for payment events (Postman compatibility)
func handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		log.Printf("Failed to decode request: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        generateEventID(),
		Type:      "payment",
		Timestamp: time.Now(),
		Data:      data,
	}

	if err := publishEvent("payment-events", event); err != nil {
		log.Printf("Failed to publish event: %v", err)
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	log.Printf("📤 [PRODUCED] Topic: payment-events | Event ID: %s | Type: payment", event.ID)

	response := map[string]interface{}{
		"status":   "success",
		"success":  true,
		"message":  "Event published to topic: payment-events",
		"event_id": event.ID,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request
	var req EventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Failed to decode request: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate event type
	validTypes := map[string]string{
		"user":    "user-events",
		"movie":   "movie-events",
		"payment": "payment-events",
	}

	topic, ok := validTypes[req.Type]
	if !ok {
		http.Error(w, fmt.Sprintf("Invalid event type. Must be one of: user, movie, payment"), http.StatusBadRequest)
		return
	}

	// Create event
	event := Event{
		ID:        generateEventID(),
		Type:      req.Type,
		Timestamp: time.Now(),
		Data:      req.Data,
	}

	// Publish to Kafka
	if err := publishEvent(topic, event); err != nil {
		log.Printf("Failed to publish event: %v", err)
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	log.Printf("📤 [PRODUCED] Topic: %s | Event ID: %s | Type: %s", topic, event.ID, event.Type)

	// Send response
	response := EventResponse{
		Success: true,
		Message: fmt.Sprintf("Event published to topic: %s", topic),
		EventID: event.ID,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func publishEvent(topic string, event Event) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(eventJSON),
	}

	partition, offset, err := kafkaProducer.SendMessage(message)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	log.Printf("Event published to partition %d at offset %d", partition, offset)
	return nil
}

func generateEventID() string {
	return fmt.Sprintf("evt_%d", time.Now().UnixNano())
}

// Graceful shutdown
func init() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Shutting down Events Service...")
		os.Exit(0)
	}()
}
