package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/fgrdz/kafka-sd-starter/internal/config"
	"github.com/fgrdz/kafka-sd-starter/internal/experiment"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: experiment-runner validate|plan|run")
	}
	switch args[0] {
	case "validate":
		flags := flag.NewFlagSet("validate", flag.ContinueOnError)
		path := flags.String("config", "configs/profile-a.yaml", "configuration path")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		cfg, err := config.Load(*path)
		if err != nil {
			return err
		}
		fmt.Printf("configuration for profile %s is valid\n", cfg.Profile)
		return nil
	case "plan", "run":
		return planOrRun(args[0], args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func planOrRun(command string, args []string) error {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	profile := flags.String("profile", "A", "A or B")
	scenario := flags.String("scenario", "baseline", "baseline or fault")
	repetition := flags.Int("repetition", 1, "repetition number")
	dryRun := flags.Bool("dry-run", false, "write plan without external mutations")
	confirmDelete := flags.Bool("confirm-delete", false, "authorize deletion of the selected leader pod")
	outputRoot := flags.String("output-root", "", "output root; OS temporary directory by default for dry-run")
	configPath := flags.String("config", "", "configuration path")
	runPrefix := flags.String("run-prefix", "", "optional run ID prefix (for example smoke)")
	runID := flags.String("run-id", "", "exact run ID; mutually exclusive with --run-prefix")
	versionsPath := flags.String("versions", "versions.env", "version metadata file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *profile != "A" && *profile != "B" {
		return errors.New("--profile must be A or B")
	}
	if *configPath == "" {
		*configPath = "configs/profile-" + map[string]string{"A": "a", "B": "b"}[*profile] + ".yaml"
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	options := experiment.RunOptions{Profile: *profile, Scenario: *scenario, Repetition: *repetition, DryRun: *dryRun, ConfirmDelete: *confirmDelete, OutputRoot: *outputRoot, RunPrefix: *runPrefix, RunID: *runID}
	if command == "plan" {
		steps, err := experiment.Plan(cfg, options)
		if err != nil {
			return err
		}
		data, err := json.MarshalIndent(steps, "", "  ")
		if err != nil {
			return fmt.Errorf("encode plan: %w", err)
		}
		fmt.Println(string(data))
		return nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var runDir string
	if options.Scenario == "fault" {
		if options.DryRun {
			runDir, err = experiment.DryRunFault(ctx, cfg, options, *versionsPath)
		} else {
			runDir, err = experiment.RunFault(ctx, cfg, options, *versionsPath)
		}
	} else {
		runDir, err = experiment.RunBaseline(ctx, cfg, options, *versionsPath)
	}
	if err != nil {
		return err
	}
	fmt.Println(runDir)
	return nil
}
