package service

import (
	"app/router"
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
)

type Device struct {
	interval time.Duration
	handle   router.PublishHandle[TemperatureMessage]
}

func NewDevice(r *router.Router, ns DeviceNamespace) *Device {
	return &Device{
		handle: r.GetPublishHandle(TemperatureTopic{}.WithNamespace(ns)),
	}
}

func (d *Device) Run(ctx context.Context) error {
	tick := time.Tick(d.interval)

	for {
		select {
		case <-tick:
			m := TemperatureMessage{
				TemperatureCelcius: rand.Float64(),
			}
			if err := d.handle.Publish(ctx, m); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type Service struct {
	logger *slog.Logger
	router *router.Router
}

func NewService(l *slog.Logger, r *router.Router) *Service {
	return &Service{
		logger: l,
		router: r,
	}
}

// Start enables the service by registering all of its routes.  If the context deadline is exceeded or the context is canceled,
// an error is returned, wrapping the context's error.
func (s Service) Start(ctx context.Context) error {
	err := s.router.HandleSubscription(ctx, TemperatureTopic{}, s.handleTemperature)
	if err != nil {
		return fmt.Errorf("subscribing to TemperatureTopic: %w", err)
	}

	return nil
}

func (s Service) handleTemperature(ns DeviceNamespace, msg TemperatureMessage) {
	s.logger.Info("temperature_received", "ns", ns, "temp_celcius", msg.TemperatureCelcius)
}
