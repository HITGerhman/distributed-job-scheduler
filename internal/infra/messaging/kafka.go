package messaging

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Record struct {
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
}

type Producer interface {
	Publish(ctx context.Context, record Record) error
	Close() error
}

type KafkaProducer struct {
	client  *kgo.Client
	timeout time.Duration
}

func NewKafkaProducer(brokers []string, timeout time.Duration) (*KafkaProducer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer failed: %w", err)
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &KafkaProducer{client: client, timeout: timeout}, nil
}

func (p *KafkaProducer) Publish(ctx context.Context, record Record) error {
	if p == nil || p.client == nil {
		return nil
	}

	publishCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	headers := make([]kgo.RecordHeader, 0, len(record.Headers))
	for key, value := range record.Headers {
		headers = append(headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
	}

	if err := p.client.ProduceSync(publishCtx, &kgo.Record{
		Topic:   record.Topic,
		Key:     []byte(record.Key),
		Value:   record.Value,
		Headers: headers,
	}).FirstErr(); err != nil {
		return fmt.Errorf("publish kafka record failed: %w", err)
	}
	return nil
}

func (p *KafkaProducer) Close() error {
	if p == nil || p.client == nil {
		return nil
	}
	p.client.Close()
	return nil
}
