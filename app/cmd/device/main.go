package main

import (
	"app/config"
	"app/log"
	"app/mqtt"
	"app/router"
	"app/service"

	"go.uber.org/fx"
)

func main() {
	fx.New(Opts...).Run()
}

// Put Options in a variable so we can validate the dependency graph in a test
var Opts = []fx.Option{
	fx.Invoke(EntryPoint),
	fx.Provide(
		service.NewDevice,
		mqtt.NewClient,
		log.NewLogger,
		config.NewDeviceConfig,
		router.NewRouter,
		// Make embedded Config available
		func(s config.DeviceConfig) config.Config { return s.Config },
	),
	fx.WithLogger(log.NewFxLogger),
}

func EntryPoint(d *service.Device) {
	// Require Device to kick off dependency graph
}
