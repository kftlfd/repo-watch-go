package ready

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// defaultTimeout bounds a readiness probe so a stalled dependency cannot hang
// the admin server's healthcheck.
const defaultTimeout = 2 * time.Second

// Check is one named readiness dependency.
type Check struct {
	Name  string
	Check func(context.Context) error
}

// All combines checks into one readiness function. Every check runs even when
// an earlier one fails, and the returned error names each failed dependency.
func All(checks ...Check) func(context.Context) error {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
		defer cancel()

		var errs []error
		for _, check := range checks {
			if check.Check == nil {
				continue
			}
			if err := check.Check(ctx); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", check.Name, err))
			}
		}

		return errors.Join(errs...)
	}
}

// Database checks that the PostgreSQL connection accepts a round trip.
func Database(db *sql.DB) Check {
	return Check{
		Name:  "database",
		Check: db.PingContext,
	}
}

// Func adapts an existing readiness method, such as a broker's Ready call.
func Func(name string, fn func() error) Check {
	return Check{
		Name: name,
		Check: func(context.Context) error {
			return fn()
		},
	}
}
