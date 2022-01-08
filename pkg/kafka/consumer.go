package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/patriciabonaldy/big_queue/pkg"

	"github.com/segmentio/kafka-go"
)

type consumer struct {
	reader *kafka.Reader
}

func (c *consumer) Ack(queueURL string, messages interface{}) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (c *consumer) Read(chMsg chan interface{}, chErr chan error) {
	defer c.reader.Close()
	for {

		m, err := c.reader.ReadMessage(context.Background())
		if err != nil {
			chErr <- errors.New(fmt.Sprintf("error while reading a message: %v", err))
			continue
		}

		var message pkg.Message
		err = json.Unmarshal(m.Value, &message)
		if err != nil {
			chErr <- err
		}

		chMsg <- message
	}
}

func NewConsumer(brokers []string, topic string) pkg.Consumer {
	c := kafka.ReaderConfig{
		Brokers:         brokers,
		Topic:           topic,
		MinBytes:        10e3,            // 10KB
		MaxBytes:        10e6,            // 10MB
		MaxWait:         1 * time.Second, // Maximum amount of time to wait for new data to come when fetching batches of messages from kafka.
		ReadLagInterval: -1,
		GroupID:         pkg.Ulid(),
		StartOffset:     kafka.LastOffset,
	}

	return &consumer{kafka.NewReader(c)}
}
