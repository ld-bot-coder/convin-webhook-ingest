package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/httpapi"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

const shutdownTimeout = 30 * time.Second

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

// run se lifecycle chalti hai taki har cleanup defer actually chale. Pehle
// listener goroutine se hi os.Exit ho jata tha, jo saare defers skip kar deta.
func run(log *slog.Logger) error {
	cfg := config.Load()
	ctx := context.Background()

	st, err := store.New(ctx, cfg.PostgresDSN, cfg.DBMaxConns)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer st.Close()

	rdb, err := redisclient.New(ctx, cfg.RedisAddr)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() { _ = rdb.Close() }()

	svc := ingest.New(st, stats.NewCache(), rdb, log)

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: httpapi.NewRouter(svc, log)}

	serverErr := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		return err
	case <-stop:
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// Order matters: pehle deliveries lena band karo, phir jo work chal raha
	// hai use drain karo. Ulta karne par in-flight request drain ke baad bhi
	// naya work laga sakti hai.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	if err := svc.Shutdown(shutdownCtx); err != nil {
		log.Error("drain in-flight work", "err", err)
	}

	log.Info("stopped")
	return nil
}
