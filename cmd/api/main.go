package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"token-transfer-monitor/internal/config"
	httpapi "token-transfer-monitor/internal/http"
	pgrepo "token-transfer-monitor/internal/repository/postgres"
	"token-transfer-monitor/internal/shutdown"
	"token-transfer-monitor/internal/storage"
)

func main() {
	ctx, stop := shutdown.NewSignalContext()
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	pool, err := storage.NewPostgresPool(ctx, cfg.Postgres)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	queryRepo := pgrepo.NewQueryRepo(pool)
	trackedTokenRepo := pgrepo.NewTrackedTokenRepo(pool)
	watchlistRepo := pgrepo.NewWalletWatchlistRepo(pool)

	handler := httpapi.NewHandler(
		queryRepo,
		trackedTokenRepo,
		watchlistRepo,
	)

	router := httpapi.NewRouter(handler)

	addr := fmt.Sprintf(":%d", cfg.App.Port)

	server := &http.Server{
		Addr:              addr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      20 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErr := make(chan error, 1)

	go func() {
		log.Printf("api server started; addr=%s", addr)

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}

		serverErr <- nil
	}()

	select {
	case <-ctx.Done():
		log.Println("api shutdown requested")

		shutdownCtx, cancel := shutdown.WithTimeout(context.Background(), cfg.Shutdown.Timeout)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Fatalf("api graceful shutdown failed: %v", err)
		}

		log.Println("api stopped cleanly")

	case err := <-serverErr:
		if err != nil {
			log.Fatalf("api server error: %v", err)
		}
	}
}
