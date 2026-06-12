// Command count-consumer reads ad-click events from Kafka, deduplicates by
// clickId and exposes the running counts over HTTP at GET /counts.
//
// The consumer uses a stable GroupId so that on restart Kafka hands back the
// group's committed offsets and the consumer resumes exactly where it left off
// — the offset-resume behaviour the lesson demonstrates.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type config struct {
	brokers  []string
	topic    string
	clientID string
	groupID  string
}

func loadConfig() config {
	return config{
		brokers:  strings.Split(getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ","),
		topic:    getEnv("KAFKA_TOPIC", "ad-clicks"),
		clientID: getEnv("KAFKA_CLIENT_ID", "count-consumer"),
		groupID:  getEnv("KAFKA_GROUP_ID", "ad-click-counter"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ensureTopic creates the topic with 3 partitions before the consumer reads.
// A consumer that subscribes to a missing topic stalls; creating it up-front
// makes the stack start deterministically regardless of whether the producer
// has run yet. It is a no-op if the topic already exists.
func ensureTopic(cfg config) error {
	conn, err := kafka.Dial("tcp", cfg.brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	ctrlConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer ctrlConn.Close()
	return ctrlConn.CreateTopics(kafka.TopicConfig{
		Topic:             cfg.topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
}

func main() {
	cfg := loadConfig()

	// Create the topic before reading so the consumer never races a missing
	// topic on a cold start. Retry while the broker is still coming up.
	for i := 0; i < 30; i++ {
		if err := ensureTopic(cfg); err == nil {
			break
		} else {
			log.Printf("waiting for broker: %v", err)
			time.Sleep(2 * time.Second)
		}
	}

	count := newCounter()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.brokers,
		Topic:   cfg.topic,
		// Same GroupId across restarts = offsets are committed per group, so a
		// restarted consumer resumes from the last committed offset.
		GroupID: cfg.groupID,
		// On the group's very first run (no committed offset) start from the
		// beginning of the log; afterwards the committed offset wins.
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	// Consume in the background; ReadMessage auto-commits the offset for the
	// group after each successfully read message.
	go func() {
		for {
			msg, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("read error: %v", err)
				return
			}
			var env clickEnvelope
			if err := json.Unmarshal(msg.Value, &env); err != nil {
				log.Printf("bad envelope: %v", err)
				continue
			}
			count.record(env)
		}
	}()

	mux := http.NewServeMux()
	// GET /counts returns the deduplicated totals without scraping logs.
	mux.HandleFunc("/counts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(count.snapshot())
	})

	port := getEnv("PORT", "3001")
	log.Printf("count-consumer listening on :%s group=%s", port, cfg.groupID)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}
