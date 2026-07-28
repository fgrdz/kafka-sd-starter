package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Profile           string     `yaml:"profile"`
	Brokers           []string   `yaml:"brokers"`
	Topic             string     `yaml:"topic"`
	ConsumerGroup     string     `yaml:"consumer_group"`
	Partitions        int32      `yaml:"partitions"`
	ReplicationFactor int16      `yaml:"replication_factor"`
	MinISR            int        `yaml:"min_insync_replicas"`
	Producer          Producer   `yaml:"producer"`
	Message           Message    `yaml:"message"`
	Metrics           Metrics    `yaml:"metrics"`
	Experiment        Experiment `yaml:"experiment"`
}

type Producer struct {
	RatePerSecond int    `yaml:"rate_per_second"`
	Acks          string `yaml:"acks"`
	Idempotent    bool   `yaml:"idempotent"`
	Retries       int    `yaml:"retries"`
}

type Message struct {
	PayloadBytes int `yaml:"payload_bytes"`
}

type Metrics struct {
	ProducerAddress string `yaml:"producer_address"`
	ConsumerAddress string `yaml:"consumer_address"`
}

type Experiment struct {
	Warmup            Duration `yaml:"warmup"`
	Baseline          Duration `yaml:"baseline"`
	Timeout           Duration `yaml:"timeout"`
	ApplicationStable Duration `yaml:"application_stable"`
	PerformanceWindow Duration `yaml:"performance_window"`
}

type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	value, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", node.Value, err)
	}
	*d = Duration(value)
	return nil
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("decode config %q: %w", path, err)
	}
	applyEnvironment(&cfg)
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate config %q: %w", path, err)
	}
	return cfg, nil
}

func (c Config) Validate() error {
	var errs []error
	if c.Profile != "A" && c.Profile != "B" {
		errs = append(errs, errors.New("profile must be A or B"))
	}
	if len(c.Brokers) == 0 {
		errs = append(errs, errors.New("at least one broker is required"))
	}
	if c.Topic == "" {
		errs = append(errs, errors.New("topic is required"))
	}
	if c.ConsumerGroup == "" {
		errs = append(errs, errors.New("consumer_group is required"))
	}
	if c.Partitions <= 0 || c.Producer.RatePerSecond <= 0 || c.Message.PayloadBytes < 0 {
		errs = append(errs, errors.New("partitions and rate must be positive and payload_bytes non-negative"))
	}
	if c.Profile == "A" && (c.ReplicationFactor != 1 || c.MinISR != 1 || c.Producer.Acks != "leader" || c.Producer.Idempotent) {
		errs = append(errs, errors.New("profile A requires replication=1, min ISR=1, acks=leader, idempotence=false"))
	}
	if c.Profile == "B" && (c.ReplicationFactor != 3 || c.MinISR != 2 || c.Producer.Acks != "all" || !c.Producer.Idempotent) {
		errs = append(errs, errors.New("profile B requires replication=3, min ISR=2, acks=all, idempotence=true"))
	}
	if time.Duration(c.Experiment.PerformanceWindow) != 60*time.Second {
		errs = append(errs, errors.New("performance_window must be 60s"))
	}
	return errors.Join(errs...)
}

func applyEnvironment(c *Config) {
	if value := os.Getenv("EXPERIMENT_BROKERS"); value != "" {
		c.Brokers = strings.Split(value, ",")
	}
	if value := os.Getenv("EXPERIMENT_TOPIC"); value != "" {
		c.Topic = value
	}
	if value := os.Getenv("EXPERIMENT_CONSUMER_GROUP"); value != "" {
		c.ConsumerGroup = value
	}
}
