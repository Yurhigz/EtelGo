package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
)

const Version = "1.0.0"

func main() {

	command := os.Args[1]

	switch command {
	case "run":
		runCommand()
	case "validate":
		validateCommand()
	case "version":
		fmt.Println(Version)
	case "help":
		printUsage()
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}

}

// Logger function to create a new logger based on log level
func newLogger(logLevel string) *slog.Logger {
	logLevelMap := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
	level, ok := logLevelMap[logLevel]
	if !ok {
		level = slog.LevelInfo
		fmt.Printf("Unknown log level: %s, defaulting to info\n", logLevel)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)
	return logger
}

// runCommand stats the pipeline based on the provided configuration with the flags.
func runCommand() {
	fs := flag.NewFlagSet("run", flag.ExitOnError)

	configFile := fs.String("config", "config.yml", "Configuration file path")
	logLevel := fs.String("loglevel", "info", "Log level (debug, info, warn, error)")
	dryRun := fs.Bool("dry-run", false, "Run without writing to output (validation only)")

	fs.Parse(os.Args[2:])

	logger := newLogger(*logLevel)

	config, err := LoadConfig(*configFile, logger)
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logger.Info("Starting pipeline",
		"topic_in", config.Input.Topic,
		"topic_out", config.Output.Topic,
		"dry_run", *dryRun,
	)

	// Suite de la logique à implémenter et à appeler dans le run
}

// validateCommand checks the configuration file to insure it's valid
func validateCommand() {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	configFile := fs.String("config", "config.yml", "Configuration file path")
	logLevel := fs.String("loglevel", "info", "Log level (debug, info, warn, error)")

	fs.Parse(os.Args[2:])

	logger := newLogger(*logLevel)

	config, err := LoadConfig(*configFile, logger)
	if err != nil {
		logger.Error("validation failed", "error", err)
		os.Exit(1)
	}

	logger.Info("configuration is valid")
	logger.Info("Input", "topic", config.Input.Topic, "brokers", len(config.Input.Brokers))
	logger.Info("Output", "topic", config.Output.Topic, "brokers", len(config.Output.Brokers))
}

// printUsage displays the usage information for the CLI application.
func printUsage() {
	fmt.Println(`EtelGo - Kafka data pipeline processor

Usage:
  etelgo <command> [flags]

Commands:
  run       Start the Kafka pipeline
  validate  Validate the configuration file
  version   Show version information
  help      Show this help message

Global flags:
  -config string
        Configuration file path (default "config.yml")
  -loglevel string
        Log level: debug, info, warn, error (default "info")

Run-specific flags:
  -dry-run
        Run without writing to output (validation only)

Examples:
  etelgo run -config config.yml
  etelgo run -config config.yml -loglevel debug
  etelgo run -config config.yml -dry-run -metrics-interval 10s
  etelgo validate -config config.yml`)
}
