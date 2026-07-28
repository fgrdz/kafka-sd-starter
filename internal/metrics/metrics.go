package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Producer struct {
	Attempted    prometheus.Counter
	Acknowledged prometheus.Counter
	Failed       prometheus.Counter
	Latency      prometheus.Histogram
	LastAck      prometheus.Gauge
}

func NewProducer(reg prometheus.Registerer) *Producer {
	m := &Producer{
		Attempted:    prometheus.NewCounter(prometheus.CounterOpts{Name: "experiment_producer_attempted_total", Help: "Messages submitted to the Kafka client."}),
		Acknowledged: prometheus.NewCounter(prometheus.CounterOpts{Name: "experiment_producer_acknowledged_total", Help: "Messages acknowledged by Kafka."}),
		Failed:       prometheus.NewCounter(prometheus.CounterOpts{Name: "experiment_producer_failed_total", Help: "Messages whose delivery completed with error."}),
		Latency:      prometheus.NewHistogram(prometheus.HistogramOpts{Name: "experiment_producer_delivery_latency_seconds", Help: "Kafka delivery latency.", Buckets: prometheus.DefBuckets}),
		LastAck:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "experiment_producer_last_ack_unixtime", Help: "Unix timestamp of the latest acknowledgement."}),
	}
	reg.MustRegister(m.Attempted, m.Acknowledged, m.Failed, m.Latency, m.LastAck)
	return m
}

type Consumer struct {
	Messages   prometheus.Counter
	Duplicates prometheus.Counter
	OutOfOrder prometheus.Counter
	Latency    prometheus.Histogram
	Last       prometheus.Gauge
}

func NewConsumer(reg prometheus.Registerer) *Consumer {
	m := &Consumer{
		Messages:   prometheus.NewCounter(prometheus.CounterOpts{Name: "experiment_consumer_messages_total", Help: "Messages processed."}),
		Duplicates: prometheus.NewCounter(prometheus.CounterOpts{Name: "experiment_consumer_duplicates_total", Help: "Duplicate messages observed."}),
		OutOfOrder: prometheus.NewCounter(prometheus.CounterOpts{Name: "experiment_consumer_out_of_order_total", Help: "Partition order violations observed."}),
		Latency:    prometheus.NewHistogram(prometheus.HistogramOpts{Name: "experiment_consumer_end_to_end_latency_seconds", Help: "Production-to-consumption latency.", Buckets: prometheus.DefBuckets}),
		Last:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "experiment_consumer_last_message_unixtime", Help: "Unix timestamp of the latest consumed message."}),
	}
	reg.MustRegister(m.Messages, m.Duplicates, m.OutOfOrder, m.Latency, m.Last)
	return m
}

func Server(address string, gatherer prometheus.Gatherer) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	return &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

func Shutdown(server *http.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown metrics server: %w", err)
	}
	return nil
}
