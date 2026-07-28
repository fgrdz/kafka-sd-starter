package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/fgrdz/kafka-sd-starter/internal/config"
	appmetrics "github.com/fgrdz/kafka-sd-starter/internal/metrics"
	"github.com/fgrdz/kafka-sd-starter/internal/output"
	"github.com/fgrdz/kafka-sd-starter/internal/producer"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	if err := run(); err != nil {
		slog.Error("producer stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "configs/profile-a.yaml", "YAML configuration")
	runID := flag.String("run-id", "", "experiment run ID")
	outputPath := flag.String("output", "producer.jsonl", "producer JSONL path")
	flag.Parse()
	if *runID == "" {
		return errors.New("--run-id is required")
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	writer, err := output.NewJSONLWriter(*outputPath)
	if err != nil {
		return err
	}
	defer writer.Close()
	registry := prometheus.NewRegistry()
	m := appmetrics.NewProducer(registry)
	server := appmetrics.Server(cfg.Metrics.ProducerAddress, registry)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server", "error", err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runner, err := producer.New(cfg, *runID, writer, m, slog.Default())
	if err != nil {
		return err
	}
	defer runner.Close()
	if err := runner.Run(ctx); err != nil {
		return err
	}
	return appmetrics.Shutdown(server)
}
