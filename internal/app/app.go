package app

import (
	"context"
	"errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"net/http"
	"os"
	"os/signal"
	"reversedns/internal/config"
	"reversedns/internal/infrastructure/dns"
	"reversedns/internal/infrastructure/repository"
	"reversedns/internal/path"
	"reversedns/internal/service"
	"reversedns/internal/service/scheduler"
	"reversedns/internal/transport/http/handler"
	"reversedns/internal/transport/http/server"
	"reversedns/pkg/logger"
	"syscall"
	"time"
)

func Run() {
	// Init logger.
	logsDir := "tmp/logs"
	var tops = []logger.TeeOption{
		{
			Filename: logsDir + "/access.log",
			Ropt: logger.RotateOptions{
				MaxSize:    1,
				MaxAge:     1,
				MaxBackups: 3,
				Compress:   true,
			},
			Lef: func(lvl logger.Level) bool {
				return lvl <= logger.InfoLevel
			},
		},
		{
			Filename: logsDir + "/error.log",
			Ropt: logger.RotateOptions{
				MaxSize:    1,
				MaxAge:     1,
				MaxBackups: 3,
				Compress:   true,
			},
			Lef: func(lvl logger.Level) bool {
				return lvl > logger.InfoLevel
			},
		},
	}

	logger.ResetDefault(logger.NewTeeWithRotate(tops))
	defer logger.Sync()

	// Init cfg from file.
	cfg := config.NewConfig()
	if err := cfg.Init(path.ConfigFile); err != nil {
		logger.Error("Init cfg", logger.NamedError("error", err))
		return
	}

	logger.Info("Config",
		logger.Any("cfg", cfg),
	)

	// Connect to database
	ctx := context.Background()
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(cfg.DB.DSN).SetServerAPIOptions(serverAPI)
	// Create a new client and connect to the server
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		logger.Error("Can't connect to database!", logger.NamedError("error", err))
		return
	}

	// Ping the MongoDB server and check for errors
	err = client.Ping(ctx, nil)
	if err != nil {
		logger.Error("Can't connect to database: "+cfg.DB.DSN, logger.NamedError("error", err))
		return
	}

	logger.Info("Connected to database: " + cfg.DB.DSN)

	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			logger.Error("Can't connect to database!", logger.NamedError("error", err))
		}
	}()

	// Init dependencies.
	dnsRepo := repository.NewDNSRepo(client, &cfg.DB)
	dsnScheduler := scheduler.NewScheduler()
	dnsClient := dns.NewDNSClient()
	dnsService := service.NewDNSService(dnsClient, dnsRepo, dsnScheduler, cfg)
	dsnScheduler.SetHandler(dnsService.ProcessDNSDataSchedulerCallback)
	dsnScheduler.SetPostHandler(dnsService.SchedulerUpdateTrigger)

	// Setup scheduler.
	empty, err := dnsRepo.IsCollectionEmpty(ctx)
	if err != nil {
		logger.Error("Check is collection empty, " + err.Error())
		return
	}

	if !empty {
		err := dnsService.SchedulerUpdateTrigger(ctx)
		if err != nil {
			logger.Error("Scheduler update trigger " + err.Error())
			return
		}

		logger.Info("The collection is NOT empty. The scheduler is installed.")
	} else {
		err := dnsRepo.InitDataBaseStructure(ctx)
		if err != nil {
			logger.Error("Can't create collection" + err.Error())
			return
		}

		logger.Info("The collection is empty. The scheduler is not installed.")
	}

	services := service.NewService(
		cfg,
		dnsService,
	)

	// Init HTTP handlers.
	handlerHTTP := handler.NewHandler(services)

	// Init HTTP server.
	serverHTTP := server.NewServer(cfg, handlerHTTP.Init())

	// For graceful shutdown.
	doneChan := make(chan os.Signal, 1)
	signal.Notify(doneChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Run HTTP server in new goroutine.
	go func() {
		if err := serverHTTP.Run(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serverHttp.Run()",
				logger.NamedError("error", err),
			)
		}
	}()

	logger.Info("Services started")

	// Graceful shutdown.
	<-doneChan

	logger.Info("Services stopped")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		// Extra handling here.
		// Close database, redis, truncate message queues, etc.
		cancel()
	}()

	if err := serverHTTP.Stop(ctx); err != nil {
		logger.Fatal("Services shutdown failed",
			logger.NamedError("error", err),
		)
	}

	dnsService.CancelWorkers()

	logger.Info("Services exited properly")
}
