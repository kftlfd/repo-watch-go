package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const (
	defaultMaxOpenConns    = 10
	defaultMaxIdleConns    = 5
	defaultConnMaxLifetime = 30 * time.Minute
	defaultConnMaxIdleTime = 5 * time.Minute
)

// PoolConfig controls the connection pool used by a service. Zero values are
// replaced with shared defaults.
type PoolConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// Validate checks the pool settings after applying defaults.
func (p PoolConfig) Validate() error {
	p = p.withDefaults()

	if p.MaxOpenConns < 0 {
		return errors.New("MaxOpenConns must not be negative")
	}
	if p.MaxIdleConns < 0 {
		return errors.New("MaxIdleConns must not be negative")
	}
	if p.MaxIdleConns > p.MaxOpenConns {
		return fmt.Errorf(
			"MaxIdleConns (%d) must not exceed MaxOpenConns (%d)",
			p.MaxIdleConns,
			p.MaxOpenConns,
		)
	}
	if p.ConnMaxLifetime < 0 {
		return errors.New("ConnMaxLifetime must not be negative")
	}
	if p.ConnMaxIdleTime < 0 {
		return errors.New("ConnMaxIdleTime must not be negative")
	}

	return nil
}

func (p PoolConfig) withDefaults() PoolConfig {
	if p.MaxOpenConns == 0 {
		p.MaxOpenConns = defaultMaxOpenConns
	}
	if p.MaxIdleConns == 0 {
		p.MaxIdleConns = defaultMaxIdleConns
		// Keep the default idle count valid for a smaller explicit open limit.
		if p.MaxOpenConns > 0 && p.MaxIdleConns > p.MaxOpenConns {
			p.MaxIdleConns = p.MaxOpenConns
		}
	}
	if p.ConnMaxLifetime == 0 {
		p.ConnMaxLifetime = defaultConnMaxLifetime
	}
	if p.ConnMaxIdleTime == 0 {
		p.ConnMaxIdleTime = defaultConnMaxIdleTime
	}

	return p
}

func (p PoolConfig) apply(db *sql.DB) {
	p = p.withDefaults()

	db.SetMaxOpenConns(p.MaxOpenConns)
	db.SetMaxIdleConns(p.MaxIdleConns)
	db.SetConnMaxLifetime(p.ConnMaxLifetime)
	db.SetConnMaxIdleTime(p.ConnMaxIdleTime)
}
