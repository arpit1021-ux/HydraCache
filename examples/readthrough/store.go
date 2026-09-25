package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Product is the record this demo caches. It's deliberately small — the
// point of the demo is the cache-aside pattern around it, not the schema.
type Product struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	PriceCent int64     `json:"price_cents"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ErrNotFound is returned by a Store when the requested product doesn't
// exist, distinct from a connection/query error.
var ErrNotFound = errors.New("product not found")

// Store is the system of record. PostgresStore is the real implementation;
// tests use a fake in-memory implementation instead of a live database,
// so the read-through/cache-invalidation logic in Service is fully unit
// tested without requiring Postgres to be running.
type Store interface {
	GetProduct(ctx context.Context, id int64) (Product, error)
	CreateProduct(ctx context.Context, p Product) (Product, error)
	UpdateProduct(ctx context.Context, p Product) (Product, error)
}

// PostgresStore is the real, Postgres-backed Store. simulatedLatency adds a
// fixed delay before every query — real production databases have real
// network and query latency that a cache meaningfully saves; on a
// localhost demo Postgres instance that latency is close to zero, which
// would make the whole point of the demo invisible. This is an explicit,
// labeled stand-in for that latency, not a claim about Postgres's actual
// performance — see the -simulate-db-latency flag in main.go.
type PostgresStore struct {
	db               *sql.DB
	simulatedLatency time.Duration
}

func NewPostgresStore(db *sql.DB, simulatedLatency time.Duration) *PostgresStore {
	return &PostgresStore{db: db, simulatedLatency: simulatedLatency}
}

func (s *PostgresStore) delay(ctx context.Context) error {
	if s.simulatedLatency <= 0 {
		return nil
	}
	t := time.NewTimer(s.simulatedLatency)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *PostgresStore) GetProduct(ctx context.Context, id int64) (Product, error) {
	if err := s.delay(ctx); err != nil {
		return Product{}, err
	}
	var p Product
	row := s.db.QueryRowContext(ctx,
		`SELECT id, name, price_cents, updated_at FROM products WHERE id = $1`, id)
	if err := row.Scan(&p.ID, &p.Name, &p.PriceCent, &p.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Product{}, ErrNotFound
		}
		return Product{}, fmt.Errorf("query product %d: %w", id, err)
	}
	return p, nil
}

func (s *PostgresStore) CreateProduct(ctx context.Context, p Product) (Product, error) {
	if err := s.delay(ctx); err != nil {
		return Product{}, err
	}
	row := s.db.QueryRowContext(ctx,
		`INSERT INTO products (name, price_cents, updated_at) VALUES ($1, $2, now())
		 RETURNING id, name, price_cents, updated_at`,
		p.Name, p.PriceCent)
	var out Product
	if err := row.Scan(&out.ID, &out.Name, &out.PriceCent, &out.UpdatedAt); err != nil {
		return Product{}, fmt.Errorf("insert product: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) UpdateProduct(ctx context.Context, p Product) (Product, error) {
	if err := s.delay(ctx); err != nil {
		return Product{}, err
	}
	row := s.db.QueryRowContext(ctx,
		`UPDATE products SET name = $1, price_cents = $2, updated_at = now()
		 WHERE id = $3
		 RETURNING id, name, price_cents, updated_at`,
		p.Name, p.PriceCent, p.ID)
	var out Product
	if err := row.Scan(&out.ID, &out.Name, &out.PriceCent, &out.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Product{}, ErrNotFound
		}
		return Product{}, fmt.Errorf("update product %d: %w", p.ID, err)
	}
	return out, nil
}
