package service

import (
	"app/router"
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

type (
	DeviceNamespace struct {
		Region string
		Zone   string
		ID     string
	}

	TemperatureMessage struct {
		TemperatureCelcius float64
	}

	TemperatureTopic = router.TopicDef[DeviceNamespace, TemperatureMessage]

	Device struct {
		interval time.Duration
		handle   router.PublishHandle[TemperatureMessage]
	}

	Service struct {
		ns DeviceNamespace
	}
)

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

func NewService(ns DeviceNamespace) *Service {
	return &Service{
		ns: ns,
	}
}

func (s Service) RegisterRoutes(ctx context.Context, r *router.Router) error {
	err := r.GetSubscription(ctx, TemperatureTopic{}.WithNamespace(s.ns), s.handleTemperature)
	if err != nil {
		return fmt.Errorf("subscribing to TemperatureTopic with namespace %+v", s.ns)
	}

	return nil
}

func (s Service) handleTemperature(ns DeviceNamespace, msg TemperatureMessage) {
	fmt.Println(ns, msg)
}
