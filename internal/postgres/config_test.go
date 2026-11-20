package postgres

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigResolve(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		database string
		sslMode  string
		want     string
	}{
		{
			name:     "inserts fallback database",
			url:      "postgres://user:pass@localhost:5432",
			database: "subscription_db",
			want:     "postgres://user:pass@localhost:5432/subscription_db",
		},
		{
			name:     "inserts fallback database after trailing slash",
			url:      "postgres://user:pass@localhost:5432/",
			database: "subscription_db",
			want:     "postgres://user:pass@localhost:5432/subscription_db",
		},
		{
			name:     "keeps database from URL",
			url:      "postgres://user:pass@localhost:5432/custom_db",
			database: "subscription_db",
			want:     "postgres://user:pass@localhost:5432/custom_db",
		},
		{
			name:     "keeps sslmode from URL",
			url:      "postgres://user:pass@localhost:5432/custom_db?sslmode=require",
			database: "subscription_db",
			sslMode:  "disable",
			want:     "postgres://user:pass@localhost:5432/custom_db?sslmode=require",
		},
		{
			name:     "applies sslmode when URL has none",
			url:      "postgres://user:pass@localhost:5432",
			database: "subscription_db",
			sslMode:  "verify-full",
			want:     "postgres://user:pass@localhost:5432/subscription_db?sslmode=verify-full",
		},
		{
			name:     "preserves other query parameters",
			url:      "postgres://user:pass@localhost:5432/custom_db?application_name=repo-watch",
			database: "subscription_db",
			sslMode:  "disable",
			want:     "postgres://user:pass@localhost:5432/custom_db?application_name=repo-watch&sslmode=disable",
		},
		{
			name:     "drops conflicting dbname query parameter",
			url:      "postgres://user:pass@localhost:5432/custom_db?dbname=other&sslmode=require",
			database: "subscription_db",
			want:     "postgres://user:pass@localhost:5432/custom_db?sslmode=require",
		},
		{
			name:     "preserves escaped credentials",
			url:      "postgres://user:p%40ss@localhost:5432",
			database: "subscription_db",
			want:     "postgres://user:p%40ss@localhost:5432/subscription_db",
		},
		{
			name:     "accepts postgresql scheme",
			url:      "postgresql://user:pass@localhost:5432/custom_db",
			database: "subscription_db",
			want:     "postgresql://user:pass@localhost:5432/custom_db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Config{URL: tt.url, Database: tt.database, SSLMode: tt.sslMode}.Resolve()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestConfigResolveErrors(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		database string
		sslMode  string
		wantErr  string
	}{
		{
			name:     "missing URL",
			database: "subscription_db",
			wantErr:  "DB_URL is empty",
		},
		{
			name:     "invalid scheme",
			url:      "mysql://user:pass@localhost:5432",
			database: "subscription_db",
			wantErr:  "scheme",
		},
		{
			name:     "invalid URL",
			url:      "postgres://user:pass@localhost:not-a-port",
			database: "subscription_db",
			wantErr:  "parse DB_URL",
		},
		{
			name:     "multiple path segments",
			url:      "postgres://user:pass@localhost:5432/one/two",
			database: "subscription_db",
			wantErr:  "at most one database name",
		},
		{
			name:    "missing fallback database",
			url:     "postgres://user:pass@localhost:5432",
			wantErr: "database name is empty",
		},
		{
			name:     "database name too long",
			url:      "postgres://user:pass@localhost:5432",
			database: strings.Repeat("a", 64),
			wantErr:  "exceeds 63 bytes",
		},
		{
			name:     "invalid ssl mode",
			url:      "postgres://user:pass@localhost:5432",
			database: "subscription_db",
			sslMode:  "bogus",
			wantErr:  "invalid DB_SSL_MODE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Config{URL: tt.url, Database: tt.database, SSLMode: tt.sslMode}.Resolve()
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestMaintenanceInfo(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantURL string
		wantDB  string
		wantErr string
	}{
		{
			name:    "replaces database with maintenance database",
			url:     "postgres://user:pass@localhost:5432/custom_db?sslmode=require",
			wantURL: "postgres://user:pass@localhost:5432/postgres?sslmode=require",
			wantDB:  "custom_db",
		},
		{
			name:    "drops conflicting dbname parameter",
			url:     "postgres://user:pass@localhost:5432/custom_db?dbname=other&sslmode=require",
			wantURL: "postgres://user:pass@localhost:5432/postgres?sslmode=require",
			wantDB:  "custom_db",
		},
		{
			name:    "missing database name",
			url:     "postgres://user:pass@localhost:5432/",
			wantErr: "database name is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotURL, gotDB, err := maintenanceInfo(tt.url)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantURL, gotURL)
			require.Equal(t, tt.wantDB, gotDB)
		})
	}
}
