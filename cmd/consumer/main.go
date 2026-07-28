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
	"github.com/fgrdz/kafka-sd-starter/internal/consumer"
	appmetrics "github.com/fgrdz/kafka-sd-starter/internal/metrics"
	"github.com/fgrdz/kafka-sd-starter/internal/output"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	if err := run(); err != nil {
		slog.Error("consumer stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "configs/profile-a.yaml", "YAML configuration")
	outputPath := flag.String("output", "consumer.jsonl", "consumer JSONL path")
	flag.Parse()
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
	m := appmetrics.NewConsumer(registry)
	server := appmetrics.Server(cfg.Metrics.ConsumerAddress, registry)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server", "error", err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runner, err := consumer.New(cfg, writer, m, slog.Default())
	if err != nil {
		return err
	}
	defer runner.Close()
	if err := runner.Run(ctx); err != nil {
		return err
	}
	return appmetrics.Shutdown(server)
}
