package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	h3 "github.com/uber/h3-go/v4"
)

func getenv(key, def string) string {
	value := os.Getenv(key)
	if value != "" {
		return value
	}
	return def
}

func testRedis(ctx context.Context, addr string) error {
	fmt.Println("Redis test")
	client := redis.NewClient(&redis.Options{
		Addr:        addr,
		DialTimeout: 2 * time.Second,
	})
	defer func() { _ = client.Close() }()

	pingErr := client.Ping(ctx).Err()
	if pingErr != nil {
		return fmt.Errorf("redis ping: %w", pingErr)
	}

	setErr := client.Set(ctx, "hello", "world", 30*time.Second).Err()
	if setErr != nil {
		return fmt.Errorf("redis set: %w", setErr)
	}

	val, err := client.Get(ctx, "hello").Result()
	if err != nil {
		return fmt.Errorf("redis get: %w", err)
	}

	fmt.Println("redis GET hello: ", val)
	return nil
}

func testGeoServerWFS(baseURL string) error {
	fmt.Println("Geoserver wfs test")

	// Sample wfs request for demo:places layer (comes with default demo data)
	wfsURL := fmt.Sprintf("%s/ows?service=WFS&version=2.0.0&request=GetFeature&typeNames=demo:places&outputFormat=application/json&count=2",
		strings.TrimRight(baseURL, "/"))
	u, err := url.Parse(wfsURL)
	if err != nil {
		return fmt.Errorf("bad WFS URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
	resp, err := http.Get(u.String())
	if err != nil {
		return fmt.Errorf("http get WFS: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		// Only read a small part of body (because it can be large)
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("WFS status %d: %s", resp.StatusCode, string(b))
	}

	// Only read a small part of body (because it can be large)
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	fmt.Println("WFS sample:")
	fmt.Println(string(body))
	return nil
}

func testKafka(brokers []string, topic string) error {
	fmt.Println("Kafka test")

	// Configure sarama and produce a message
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Version = sarama.V3_6_0_0
	prod, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return fmt.Errorf("producer create: %w", err)
	}
	defer func() { _ = prod.Close() }()

	payload := map[string]any{
		"layer": "demo:places",
		"op":    "update",
		"geom":  "POINT(18.0686 59.3293)",
		"ts":    time.Now().UTC().Format(time.RFC3339Nano),
	}

	// Convert to json and send
	msgBytes, _ := json.Marshal(payload)
	_, _, err = prod.SendMessage(&sarama.ProducerMessage{
		Topic: topic, Value: sarama.ByteEncoder(msgBytes),
	})
	if err != nil {
		return fmt.Errorf("send message: %w", err)
	}
	fmt.Println("produced one message")

	// Consume the message
	consumer, err := sarama.NewConsumer(brokers, cfg)
	if err != nil {
		return fmt.Errorf("consumer create: %w", err)
	}
	defer func() { _ = consumer.Close() }()

	pc, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		pc, err = consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
		if err != nil {
			return fmt.Errorf("consume partition: %w", err)
		}
	}
	defer func() { _ = pc.Close() }()

	select {
	case m := <-pc.Messages():
		fmt.Println("consumed:", string(m.Value))
	case <-time.After(5 * time.Second):
		fmt.Println("no message consumed (timeout)")
	}

	return nil
}

func demoH3() {
	fmt.Println("H3 demo")
	lat, lon := 59.3293, 18.0686
	ll := h3.NewLatLng(lat, lon)
	// Convert to h3 cell at resolution 8
	cell := h3.LatLngToCell(ll, 8)
	neighbors := h3.GridDisk(cell, 1)
	fmt.Printf("H3 center: %s, neighbors: %d\n", cell.String(), len(neighbors))
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	redisAddr := getenv("REDIS_ADDR", "localhost:6379")
	geoserver := getenv("GEOSERVER_URL", "http://localhost:8080/geoserver")
	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := getenv("KAFKA_TOPIC", "spatial-updates")

	if err := testRedis(ctx, redisAddr); err != nil {
		fmt.Println("Redis error:", err)
		return
	}
	if err := testGeoServerWFS(geoserver); err != nil {
		fmt.Println("Geoserver error:", err)
		return
	}
	if err := testKafka(brokers, topic); err != nil {
		fmt.Println("Kafka error:", err)
		return
	}
	demoH3()
	fmt.Println("All tests completed")
}
