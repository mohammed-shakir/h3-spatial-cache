package kafkaconsumer

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type messageProcessor func(context.Context, *sarama.ConsumerMessage) error

type groupHandler struct {
	process messageProcessor
}

func (h *groupHandler) Setup(s sarama.ConsumerGroupSession) error   { return nil }
func (h *groupHandler) Cleanup(s sarama.ConsumerGroupSession) error { return nil }

func (h *groupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := sess.Context()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("claim context done: %w", ctx.Err())
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			start := time.Now()
			if err := h.process(ctx, msg); err != nil {
				_ = start
				return fmt.Errorf("process failed (topic=%s, part=%d, off=%d): %w",
					msg.Topic, msg.Partition, msg.Offset, err)
			}
			sess.MarkMessage(msg, "")
		}
	}
}
