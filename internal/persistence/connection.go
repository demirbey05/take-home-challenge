package persistence

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

// NewDB opens a connection to the PostgreSQL database.
func NewDB(databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	if err := db.PingContext(context.Background()); err != nil {
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	return db, nil
}
