package trb_nats

import (
	"os"

	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/nats-io/nats.go"
	"gopkg.in/yaml.v3"
)

type streams struct {
	Streams map[string]nats.StreamConfig `yaml:"Streams" json:"streams"`
}

// consumers — формат YAML: Consumers -> [stream name] -> [consumer name] -> ConsumerConfig
type consumers struct {
	Consumers map[string]map[string]nats.ConsumerConfig `yaml:"Consumers" json:"consumers"`
}

type NatsConfig struct {
	URL       string
	Streams   map[string]nats.StreamConfig
	Consumers map[string]map[string]nats.ConsumerConfig // stream name -> consumer name -> config
}

func LoadNatsConfig() (NatsConfig, error) {
	streams, err := LoadStreamsConfig()
	if err != nil {
		return NatsConfig{}, err
	}
	consumers, err := LoadConsumersConfig()
	if err != nil {
		return NatsConfig{}, err
	}
	config := NatsConfig{
		URL:       env.Addr("NATS_URL", "NATS_URL_DOCKER"),
		Streams:   streams,
		Consumers: consumers,
	}
	return config, nil
}

func LoadStreamsConfig() (map[string]nats.StreamConfig, error) {
	data, err := os.ReadFile("./configs/nats-server/streams.yaml")
	if err != nil {
		return map[string]nats.StreamConfig{}, err
	}
	var config streams
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return map[string]nats.StreamConfig{}, err
	}
	return config.Streams, nil
}

// LoadConsumersConfig читает consumers.yaml. Формат: Consumers -> [stream name] -> [consumer name] -> ConsumerConfig.
func LoadConsumersConfig() (map[string]map[string]nats.ConsumerConfig, error) {
	data, err := os.ReadFile("./configs/nats-server/consumers.yaml")
	if err != nil {
		return nil, err
	}
	var config consumers
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	if config.Consumers == nil {
		return map[string]map[string]nats.ConsumerConfig{}, nil
	}
	return config.Consumers, nil
}

func (n Nats) CreateNatsStreams(stream nats.StreamConfig) (*nats.StreamInfo, error) {
	streamInfo, err := n.Jsc.StreamInfo(stream.Name)
	switch {
	case err == nil && streamInfo != nil:
		streamInfo, err = n.Jsc.UpdateStream(&stream)
	case err != nil && streamInfo == nil:
		streamInfo, err = n.Jsc.AddStream(&stream)
	}
	if err != nil {
		return &nats.StreamInfo{}, err
	}
	return streamInfo, nil
}

func (n Nats) CreateNatsConsumers(stream string, consumer nats.ConsumerConfig) (nats.ConsumerInfo, error) {
	consumerInfo, err := n.Jsc.ConsumerInfo(stream, consumer.Durable)
	switch {
	case err == nil && consumerInfo != nil:
		consumerInfo, err = n.Jsc.UpdateConsumer(stream, &consumer)
	case err != nil && consumerInfo == nil:
		consumerInfo, err = n.Jsc.AddConsumer(stream, &consumer)
	}
	if err != nil {
		return nats.ConsumerInfo{}, err
	}

	return *consumerInfo, nil
}
