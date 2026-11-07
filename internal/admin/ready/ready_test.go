package ready

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllRunsEveryCheck(t *testing.T) {
	called := 0

	check := All(
		Check{Name: "database", Check: func(context.Context) error {
			called++
			return errors.New("connection refused")
		}},
		Check{Name: "rabbitmq", Check: func(context.Context) error {
			called++
			return errors.New("not connected")
		}},
		Check{Name: "healthy", Check: func(context.Context) error {
			called++
			return nil
		}},
	)

	err := check(context.Background())
	require.ErrorContains(t, err, "database: connection refused")
	require.ErrorContains(t, err, "rabbitmq: not connected")
	require.Equal(t, 3, called)
}

func TestAllReportsReady(t *testing.T) {
	check := All(
		Check{Name: "database", Check: func(context.Context) error { return nil }},
		Check{Name: "rabbitmq", Check: func(context.Context) error { return nil }},
	)

	require.NoError(t, check(context.Background()))
}

func TestAllSkipsNilChecks(t *testing.T) {
	require.NoError(t, All(Check{Name: "unused"})(context.Background()))
}

func TestFuncDelegates(t *testing.T) {
	want := errors.New("not ready")
	check := Func("broker", func() error { return want })

	require.ErrorIs(t, check.Check(context.Background()), want)
}
