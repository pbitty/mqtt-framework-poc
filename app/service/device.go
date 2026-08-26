package service

import (
	"app/config"
	"app/router"
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

type Device struct {
	temperature router.PublishHandle[TemperatureMessage]
	interval    time.Duration
	logger      *slog.Logger
	timeout     time.Duration
	startOnce   sync.Once
}

func NewDevice(cfg config.DeviceConfig, r *router.Router, l *slog.Logger) *Device {
	ns := DeviceNamespace{
		Region: cfg.Device.Region,
		Zone:   cfg.Device.Zone,
		ID:     cfg.Device.ID,
	}

	return &Device{
		temperature: r.GetPublishHandle(TemperatureTopic{}.WithNamespace(ns)),
		interval:    time.Second,
		logger:      l,
		timeout:     5 * time.Second, // TODO make this adjustable
	}
}

func (d *Device) Start() {
	d.startOnce.Do(func() {
		go d.runLoop()
	})
}

func (d *Device) runLoop() {
	for range time.Tick(d.interval) {
		d.publishTemperature()
	}
}

func (d *Device) publishTemperature() {
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()

	msg := TemperatureMessage{
		TemperatureCelcius: 30 + (10 * rand.Float64()),
	}

	err := d.temperature.Publish(ctx, msg)
	if errors.Is(err, context.Canceled) {
		return
	} else if err != nil {
		d.logger.Error("error_publishing_temperature", "error", err)
	}

	d.logger.Debug("published_temperature", "msg", msg)
}
