// Package hitevents provides a Kafka publisher for hit events.
package hitevents

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type Event struct {
	Layer    string    `json:"layer"`
	Lon      float64   `json:"lon"`
	Lat      float64   `json:"lat"`
	TS       time.Time `json:"ts"`
	Scenario string    `json:"scenario,omitempty"`
}

type Publisher struct {
	topic   string
	events  chan Event
	prod    sarama.AsyncProducer
	stopCh  chan struct{}
	stopped chan struct{}
}

func NewPublisher(brokers []string, topic string, queueSize int) (*Publisher, error) {
	if queueSize <= 0 {
		queueSize = 1024
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_5_0_0
	cfg.Producer.Return.Errors = true
	cfg.Producer.Return.Successes = false

	prod, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("hitevents: create async producer: %w", err)
	}

	p := &Publisher{
		topic:   topic,
		events:  make(chan Event, queueSize),
		prod:    prod,
		stopCh:  make(chan struct{}),
		stopped: make(chan struct{}),
	}

	go func() {
		defer close(p.stopped)
		for ev := range p.events {
			b, err := json.Marshal(ev)
			if err != nil {
				log.Printf("hitevents: marshal error: %v", err)
				continue
			}
			msg := &sarama.ProducerMessage{
				Topic: p.topic,
				Value: sarama.ByteEncoder(b),
			}
			p.prod.Input() <- msg
		}
	}()

	go func() {
		for err := range p.prod.Errors() {
			if err != nil {
				log.Printf("hitevents: producer error: %v", err)
			}
		}
	}()

	return p, nil
}

func (p *Publisher) Publish(ev Event) {
	select {
	case p.events <- ev:
	default:
		// queue full → drop silently (do NOT block request path)
	}
}

func (p *Publisher) Close() error {
	close(p.events)
	<-p.stopped

	if err := p.prod.Close(); err != nil {
		return fmt.Errorf("hitevents: close producer: %w", err)
	}

	return nil
}

var global *Publisher

func InitGlobal(p *Publisher) {
	global = p
}

func Publish(ev Event) {
	if global == nil {
		return
	}
	global.Publish(ev)
}

func CloseGlobal() error {
	if global == nil {
		return nil
	}
	return global.Close()
}
