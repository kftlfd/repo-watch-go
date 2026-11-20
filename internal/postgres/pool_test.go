package postgres

import (
	"database/sql"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPoolConfigDefaults(t *testing.T) {
	got := (PoolConfig{}).withDefaults()

	require.Equal(t, defaultMaxOpenConns, got.MaxOpenConns)
	require.Equal(t, defaultMaxIdleConns, got.MaxIdleConns)
	require.Equal(t, defaultConnMaxLifetime, got.ConnMaxLifetime)
	require.Equal(t, defaultConnMaxIdleTime, got.ConnMaxIdleTime)
}

func TestPoolConfigDefaultsIdleToOpen(t *testing.T) {
	got := PoolConfig{MaxOpenConns: 3}.withDefaults()

	require.Equal(t, 3, got.MaxOpenConns)
	require.Equal(t, 3, got.MaxIdleConns)
}

func TestPoolConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     PoolConfig
		wantErr string
	}{
		{
			name: "zero values use defaults",
		},
		{
			name: "custom values",
			cfg: PoolConfig{
				MaxOpenConns:    4,
				MaxIdleConns:    2,
				ConnMaxLifetime: time.Minute,
				ConnMaxIdleTime: time.Second,
			},
		},
		{
			name:    "negative max open connections",
			cfg:     PoolConfig{MaxOpenConns: -1},
			wantErr: "MaxOpenConns",
		},
		{
			name:    "negative max idle connections",
			cfg:     PoolConfig{MaxIdleConns: -1},
			wantErr: "MaxIdleConns",
		},
		{
			name:    "idle exceeds open",
			cfg:     PoolConfig{MaxOpenConns: 3, MaxIdleConns: 4},
			wantErr: "MaxIdleConns",
		},
		{
			name:    "negative connection lifetime",
			cfg:     PoolConfig{ConnMaxLifetime: -time.Second},
			wantErr: "ConnMaxLifetime",
		},
		{
			name:    "negative connection idle time",
			cfg:     PoolConfig{ConnMaxIdleTime: -time.Second},
			wantErr: "ConnMaxIdleTime",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestPoolConfigApply(t *testing.T) {
	db, err := sql.Open("postgres", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	PoolConfig{
		MaxOpenConns:    7,
		MaxIdleConns:    3,
		ConnMaxLifetime: time.Minute,
		ConnMaxIdleTime: time.Second,
	}.apply(db)

	require.Equal(t, 7, db.Stats().MaxOpenConnections)
}

func TestPoolConfigApplyDefaults(t *testing.T) {
	db, err := sql.Open("postgres", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	(PoolConfig{}).apply(db)

	require.Equal(t, defaultMaxOpenConns, db.Stats().MaxOpenConnections)
}

func TestConnectRejectsSingleConnectionForMigrations(t *testing.T) {
	_, err := Connect(
		t.Context(),
		Config{
			URL:  "postgres://user:pass@localhost:5432/test",
			Pool: PoolConfig{MaxOpenConns: 1, MaxIdleConns: 1},
		},
		false,
		true,
		fstest.MapFS{},
	)

	require.ErrorContains(t, err, "at least 2 connections")
}
