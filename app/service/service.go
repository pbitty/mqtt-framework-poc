package service

import (
	"app/router"
	"context"
	"fmt"
	"log/slog"

	"go.uber.org/fx"
)

// Service represents a process running on a computer,
// receiving temperature readings from an MQTT broker (published by [Device]).
type Service struct {
	logger *slog.Logger
	router *router.Router
}

func NewService(l *slog.Logger, r *router.Router, lc fx.Lifecycle) *Service {
	s := &Service{
		logger: l,
		router: r,
	}

	lc.Append(fx.StartHook(s.start))

	return s
}

// Start enables the service by registering all of its routes.  If the context deadline is exceeded or the context is canceled,
// an error is returned, wrapping the context's error.
func (s Service) start(ctx context.Context) error {
	err := s.router.HandleSubscription(ctx, TemperatureTopic{}, s.handleTemperature)
	if err != nil {
		return fmt.Errorf("subscribing to TemperatureTopic: %w", err)
	}

	s.logger.Debug("service_started")

	return nil
}

func (s Service) handleTemperature(ns DeviceNamespace, msg TemperatureMessage) {
	s.logger.Info("temperature_received", "namespace", ns, "temp_celcius", fmt.Sprintf("%0.2f", msg.TemperatureCelcius))
}
