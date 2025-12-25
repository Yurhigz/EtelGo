package main

import (
	"flag"
	"fmt"
	"os"
)

type CLIConfig struct {
	Configuration string
	LogLevel      string
	DryRun        bool
}

func main() {

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "run":
		runCommand()
	case "validate":
		validateCommand()
	case "version":
		fmt.Println("EtelGo v1.0.0")
	case "help":
		printUsage()
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}

	config, err := LoadConfig("example.yml", nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v", config.Input)

	// Partie CLI pour l'usage d'options
	// Déclaration de l'ensemble de mes flags

	configuration := flag.String("config", "/config/*.yml", "Path to the configuration file")
	loglevel := flag.String("loglevel", "info", "Log level (debug, info, warn, error)")
	dryrun := flag.Bool("dryrun", false, "Enable dry run mode")

	flag.Parse()
}

// func runCommand() {
// 	fs := flag.NewFlagSet("run", flag.ExitOnError)

// 	// Flags essentiels (basés sur votre config)
// 	configFile := fs.String("config", "config.yml", "Configuration file path")
// 	logLevel := fs.String("loglevel", "info", "Log level (debug, info, warn, error)")

// 	// Flags d'observabilité (du README)
// 	metricsInterval := fs.String("metrics-interval", "30s", "Interval for metrics reporting")

// 	// Flags de robustesse (du README - graceful shutdown)
// 	shutdownTimeout := fs.String("shutdown-timeout", "30s", "Graceful shutdown timeout")

// 	// Flag pour dry-run (utile pour tester sans écrire)
// 	dryRun := fs.Bool("dry-run", false, "Run without writing to output (validation only)")

// 	fs.Parse(os.Args[2:])

// 	// Initialiser le logger
// 	logLevelMap := map[string]slog.Level{
// 		"debug": slog.LevelDebug,
// 		"info":  slog.LevelInfo,
// 		"warn":  slog.LevelWarn,
// 		"error": slog.LevelError,
// 	}
// 	level, ok := logLevelMap[*logLevel]
// 	if !ok {
// 		level = slog.LevelInfo
// 	}

// 	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
// 	slog.SetDefault(logger)

// 	// Charger la config
// 	config, err := LoadConfig(*configFile, logger)
// 	if err != nil {
// 		logger.Error("Failed to load config", "error", err)
// 		os.Exit(1)
// 	}

// 	logger.Info("Starting pipeline",
// 		"topic_in", config.Input.Topic,
// 		"topic_out", config.Output.Topic,
// 		"dry_run", *dryRun,
// 		"metrics_interval", *metricsInterval,
// 	)

// 	// Votre logique d'exécution
// 	// runPipeline(config, logger, *dryRun, *metricsInterval, *shutdownTimeout)
// }

// func validateCommand() {
// 	fs := flag.NewFlagSet("validate", flag.ExitOnError)
// 	configFile := fs.String("config", "config.yml", "Configuration file path")
// 	logLevel := fs.String("loglevel", "info", "Log level (debug, info, warn, error)")

// 	fs.Parse(os.Args[2:])

// 	logLevelMap := map[string]slog.Level{
// 		"debug": slog.LevelDebug,
// 		"info":  slog.LevelInfo,
// 		"warn":  slog.LevelWarn,
// 		"error": slog.LevelError,
// 	}
// 	level, ok := logLevelMap[*logLevel]
// 	if !ok {
// 		level = slog.LevelInfo
// 	}

// 	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

// 	config, err := LoadConfig(*configFile, logger)
// 	if err != nil {
// 		logger.Error("Validation failed", "error", err)
// 		os.Exit(1)
// 	}

// 	logger.Info("✓ Configuration is valid")
// 	logger.Info("Input", "topic", config.Input.Topic, "brokers", len(config.Input.Brokers))
// 	logger.Info("Output", "topic", config.Output.Topic, "brokers", len(config.Output.Brokers))
// }

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
  -metrics-interval string
        Interval for metrics reporting (default "30s")
  -shutdown-timeout string
        Graceful shutdown timeout (default "30s")
  -dry-run
        Run without writing to output (validation only)

Examples:
  etelgo run -config config.yml
  etelgo run -config config.yml -loglevel debug
  etelgo run -config config.yml -dry-run -metrics-interval 10s
  etelgo validate -config config.yml`)
}
