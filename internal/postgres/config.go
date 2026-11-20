package postgres

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode"
)

var validSSLModes = map[string]struct{}{
	"disable":     {},
	"allow":       {},
	"prefer":      {},
	"require":     {},
	"verify-ca":   {},
	"verify-full": {},
}

// Config describes how to reach a service database.
//
// URL is required. If it does not contain a database name, Database is used.
// SSLMode is only applied when URL does not already specify sslmode.
type Config struct {
	URL      string
	Database string
	SSLMode  string

	Pool PoolConfig

	AutoCreate  bool
	AutoMigrate bool
}

// Resolve validates the configuration and returns a complete database URL.
func (c Config) Resolve() (string, error) {
	if err := c.Pool.Validate(); err != nil {
		return "", fmt.Errorf("invalid DB pool config: %w", err)
	}

	u, err := parseURL(c.URL)
	if err != nil {
		return "", err
	}

	dbName, err := resolveDatabaseName(u, c.Database)
	if err != nil {
		return "", err
	}

	// The database name is always taken from the URL path (or fallback), so
	// a conflicting dbname query parameter must not leak into the final DSN.
	q := u.Query()
	if q.Has("dbname") {
		q.Del("dbname")
		u.RawQuery = q.Encode()
	}

	if err := applySSLMode(u, c.SSLMode); err != nil {
		return "", err
	}

	u.Path = "/" + dbName
	u.RawPath = ""
	u.Fragment = ""

	return u.String(), nil
}

func parseURL(raw string) (*url.URL, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("DB_URL is empty")
	}

	u, err := url.Parse(raw)
	if err != nil {
		// url.Error includes the original URL, which may contain credentials.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			return nil, fmt.Errorf("parse DB_URL: %s: %w", urlErr.Op, urlErr.Err)
		}
		return nil, fmt.Errorf("parse DB_URL: %w", err)
	}

	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, errors.New("DB_URL must use the postgres:// or postgresql:// scheme")
	}
	if u.Opaque != "" {
		return nil, errors.New("DB_URL must be a hierarchical URL")
	}

	return u, nil
}

func resolveDatabaseName(u *url.URL, fallback string) (string, error) {
	name := strings.Trim(u.Path, "/")
	if name == "" {
		name = fallback
	} else if strings.Contains(name, "/") {
		return "", errors.New("DB_URL path must contain at most one database name")
	}

	if err := validateDatabaseName(name); err != nil {
		return "", err
	}

	return name, nil
}

func validateDatabaseName(name string) error {
	if name == "" {
		return errors.New("database name is empty")
	}
	if len(name) > 63 {
		return fmt.Errorf("database name %q exceeds 63 bytes", name)
	}
	if strings.Contains(name, "/") {
		return fmt.Errorf("database name %q contains a slash", name)
	}
	for _, r := range name {
		if r == 0 || unicode.IsControl(r) {
			return fmt.Errorf("database name %q contains an invalid character", name)
		}
	}

	return nil
}

func applySSLMode(u *url.URL, sslMode string) error {
	q := u.Query()
	if q.Get("sslmode") != "" {
		return nil
	}
	if sslMode == "" {
		return nil
	}
	if _, ok := validSSLModes[sslMode]; !ok {
		return fmt.Errorf("invalid DB_SSL_MODE %q", sslMode)
	}

	q.Set("sslmode", sslMode)
	u.RawQuery = q.Encode()

	return nil
}
