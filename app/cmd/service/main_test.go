package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestDependencies(t *testing.T) {
	err := fx.ValidateApp(Opts...)
	require.NoError(t, err, "fx dependency graph is missing dependencies")
}
