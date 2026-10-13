// Command postgres demonstrates the PostgreSQL KV adapter.
//
// Start the local database and apply the adapter migration first; see README.md
// in this directory. The built-in DSN is intentionally for local development
// only. Set DATABASE_URL to use another database.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nexssp/storage"
	"github.com/nexssp/storage/postgres"
)

func main() {
	if err := run(); err != nil {
		slog.Error("PostgreSQL demo failed", "err", err)
		os.Exit(1)
	}
}

func run() (err error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return storage.BadRequest("DATABASE_URL must be set to the local demo DSN or another PostgreSQL connection string")
	}

	registry := storage.NewRegistry()
	if registerErr := postgres.Register(registry); registerErr != nil {
		return registerErr
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = errors.Join(err, registry.Close(closeCtx))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	instance, err := (storage.Declaration{
		Name:    "demo-cache",
		Backend: "postgres",
		Kind:    storage.KindKV,
		Options: map[string]string{
			"dsn":       dsn,
			"namespace": "demo",
			"ttl":       "5m",
		},
	}).Open(registry)
	if err != nil {
		return fmt.Errorf("open PostgreSQL storage: %w", err)
	}

	cache, err := storage.AssertKV(instance)
	if err != nil {
		return err
	}
	if setErr := cache.Set(ctx, "greeting", map[string]string{"message": "hello from PostgreSQL"}, 0); setErr != nil {
		return fmt.Errorf("set key: %w", setErr)
	}

	value, hit, err := cache.Get(ctx, "greeting")
	if err != nil {
		return fmt.Errorf("get key: %w", err)
	}
	fmt.Printf("hit=%t value=%v\n", hit, value)
	return nil
}
