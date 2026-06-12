// Command ingest-api is an HTTP producer that appends ad-click events to a
// Kafka topic. It owns no counting logic: it only guarantees every event
// carries a stable clickId so the downstream consumer can deduplicate.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// config holds the Kafka settings, every default committed so the service runs
// out-of-the-box under Docker Compose; Compose only overrides the broker list.
type config struct {
	brokers  []string
	topic    string
	clientID string
}

// loadConfig reads the config block from the environment with safe defaults.
func loadConfig() config {
	return config{
		brokers:  strings.Split(getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ","),
		topic:    getEnv("KAFKA_TOPIC", "ad-clicks"),
		clientID: getEnv("KAFKA_CLIENT_ID", "ingest-api"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// publishClickDto is the request body for POST /clicks. clickId is the
// idempotency key: re-sending the same clickId must be counted once downstream,
// so it is a first-class field rather than free-form data.
type publishClickDto struct {
	ClickID string                 `json:"clickId"`
	AdID    string                 `json:"adId"`
	Payload map[string]interface{} `json:"payload"`
}

// clickEnvelope is the value written to the log for each click.
type clickEnvelope struct {
	ClickID   string                 `json:"clickId"`
	AdID      string                 `json:"adId"`
	Payload   map[string]interface{} `json:"payload"`
	Timestamp string                 `json:"timestamp"`
}

// producer wraps a kafka.Writer and the topic it appends to.
type producer struct {
	writer *kafka.Writer
	topic  string
}

// newProducer builds a writer with the hash balancer so that key=adId routes
// every click of one ad to the same partition (mirrors KafkaJS default keyed
// partitioning).
func newProducer(cfg config) *producer {
	w := &kafka.Writer{
		Addr: kafka.TCP(cfg.brokers...),
		// Hash balancer => same key always maps to the same partition, keeping
		// the relative order of one ad's clicks.
		Balancer: &kafka.Hash{},
	}
	return &producer{writer: w, topic: cfg.topic}
}

// publish appends one click event to the topic and returns the ids. It is
// fire-and-forget at the API level: we confirm the append, not consumer
// processing.
func (p *producer) publish(ctx context.Context, dto publishClickDto) (clickEnvelope, error) {
	// Mint an idempotency key when the caller did not supply one.
	clickID := dto.ClickID
	if clickID == "" {
		clickID = uuid.NewString()
	}
	payload := dto.Payload
	if payload == nil {
		payload = map[string]interface{}{}
	}
	env := clickEnvelope{
		ClickID:   clickID,
		AdID:      dto.AdID,
		Payload:   payload,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
	value, err := json.Marshal(env)
	if err != nil {
		return clickEnvelope{}, err
	}
	// key = adId pins the partition; the clickId header lets the consumer
	// deduplicate without parsing the value body first.
	err = p.writer.WriteMessages(ctx, kafka.Message{
		Topic:   p.topic,
		Key:     []byte(dto.AdID),
		Value:   value,
		Headers: []kafka.Header{{Key: "clickId", Value: []byte(clickID)}},
	})
	if err != nil {
		return clickEnvelope{}, err
	}
	log.Printf("Produced click clickId=%s adId=%s", clickID, dto.AdID)
	return env, nil
}

func main() {
	cfg := loadConfig()
	prod := newProducer(cfg)
	defer prod.writer.Close()

	mux := http.NewServeMux()
	// POST /clicks: accept a click and append it to the log.
	mux.HandleFunc("/clicks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var dto publishClickDto
		if err := json.NewDecoder(r.Body).Decode(&dto); err != nil || dto.AdID == "" {
			http.Error(w, "adId is required", http.StatusBadRequest)
			return
		}
		env, err := prod.publish(r.Context(), dto)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// 202 makes the fire-and-forget contract explicit: accepted into the
		// log, not yet processed.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "accepted",
			"topic":   cfg.topic,
			"clickId": env.ClickID,
			"adId":    env.AdID,
		})
	})

	port := getEnv("PORT", "3000")
	log.Printf("ingest-api listening on :%s", port)
	// Bind on all interfaces so the host can reach the container.
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}
