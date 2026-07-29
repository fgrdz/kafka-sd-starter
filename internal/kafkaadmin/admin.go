package kafkaadmin

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Leader struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	BrokerID  int32  `json:"broker_id"`
}

type PartitionState struct {
	Topic       string  `json:"topic"`
	Partition   int32   `json:"partition"`
	Leader      int32   `json:"leader"`
	Replicas    []int32 `json:"replicas"`
	ISR         []int32 `json:"isr"`
	LeaderEpoch int32   `json:"leader_epoch"`
}

type TopicState struct {
	Topic                 string           `json:"topic"`
	Partitions            []PartitionState `json:"partitions"`
	LeaderlessPartitions  int              `json:"leaderless_partitions"`
	UnderReplicated       int              `json:"under_replicated_partitions"`
	ExpectedReplication   int              `json:"expected_replication"`
	AllPartitionsHaveLead bool             `json:"all_partitions_have_leader"`
}

type Client interface {
	Ping(context.Context) error
	CreateTopic(context.Context, string, int32, int16, int) error
	PartitionLeader(context.Context, string, int32) (Leader, error)
	TopicState(context.Context, string) (TopicState, error)
	Close()
}

type Admin struct {
	client *kgo.Client
	admin  *kadm.Client
}

func New(brokers []string) (*Admin, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("create Kafka admin client: %w", err)
	}
	return &Admin{client: client, admin: kadm.NewClient(client)}, nil
}

func (a *Admin) PartitionLeader(ctx context.Context, topic string, partition int32) (Leader, error) {
	metadata, err := a.admin.ListTopics(ctx, topic)
	if err != nil {
		return Leader{}, fmt.Errorf("list topic metadata: %w", err)
	}
	detail, ok := metadata[topic]
	if !ok {
		return Leader{}, fmt.Errorf("topic %q not found", topic)
	}
	part, ok := detail.Partitions[partition]
	if !ok {
		return Leader{}, fmt.Errorf("partition %d not found in topic %q", partition, topic)
	}
	if part.Leader < 0 {
		return Leader{}, fmt.Errorf("partition %d has no leader", partition)
	}
	return Leader{Topic: topic, Partition: partition, BrokerID: part.Leader}, nil
}

func (a *Admin) TopicState(ctx context.Context, topic string) (TopicState, error) {
	metadata, err := a.admin.ListTopics(ctx, topic)
	if err != nil {
		return TopicState{}, fmt.Errorf("list topic metadata: %w", err)
	}
	detail, ok := metadata[topic]
	if !ok {
		return TopicState{}, fmt.Errorf("topic %q not found", topic)
	}
	state := TopicState{Topic: topic, AllPartitionsHaveLead: true}
	for id, partition := range detail.Partitions {
		item := PartitionState{
			Topic: topic, Partition: id, Leader: partition.Leader, LeaderEpoch: partition.LeaderEpoch,
			Replicas: append([]int32(nil), partition.Replicas...), ISR: append([]int32(nil), partition.ISR...),
		}
		state.Partitions = append(state.Partitions, item)
		if partition.Leader < 0 {
			state.LeaderlessPartitions++
			state.AllPartitionsHaveLead = false
		}
		if len(partition.ISR) < len(partition.Replicas) {
			state.UnderReplicated++
		}
		if len(partition.Replicas) > state.ExpectedReplication {
			state.ExpectedReplication = len(partition.Replicas)
		}
	}
	if len(state.Partitions) == 0 {
		return TopicState{}, fmt.Errorf("topic %q has no partitions", topic)
	}
	return state, nil
}

func (a *Admin) CreateTopic(ctx context.Context, topic string, partitions int32, replicationFactor int16, minISR int) error {
	minISRValue := fmt.Sprintf("%d", minISR)
	_, err := a.admin.CreateTopic(ctx, partitions, replicationFactor, map[string]*string{
		"min.insync.replicas": &minISRValue,
	}, topic)
	if err != nil {
		return fmt.Errorf("create topic %q: %w", topic, err)
	}
	return nil
}

func (a *Admin) Ping(ctx context.Context) error {
	if err := a.client.Ping(ctx); err != nil {
		return fmt.Errorf("ping Kafka: %w", err)
	}
	return nil
}

func (a *Admin) Close() {
	a.admin.Close()
	a.client.Close()
}
