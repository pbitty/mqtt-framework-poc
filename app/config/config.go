package config

import (
	"fmt"
	"log/slog"
	"net/url"
	"reflect"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	BrokerUrl URLValue `envconfig:"BROKER_URL" required:"true"`
}

type DeviceConfig struct {
	Config

	Device struct {
		Region string `envconfig:"REGION" required:"true"`
		Zone   string `envconfig:"ZONE" required:"true"`
		ID     string `envconfig:"ID" required:"true"`
	} `envconfig:"DEVICE" required:"true"`
}

func NewDeviceConfig(l *slog.Logger) (DeviceConfig, error) {
	return newConfig[DeviceConfig](l)
}

type ServiceConfig struct {
	Config
}

func NewServiceConfig(l *slog.Logger) (ServiceConfig, error) {
	return newConfig[ServiceConfig](l)
}

func newConfig[T any](logger *slog.Logger) (T, error) {
	typeName := reflect.TypeFor[T]().Name()

	var c T
	if err := envconfig.Process("", &c); err != nil {
		return c, fmt.Errorf("parsing env vars for %s: %w", typeName, err)
	}

	logger.Info("config_loaded", "type", typeName, "config", c)

	return c, nil
}

type URLValue struct {
	URL *url.URL
}

var _ envconfig.Decoder = (*URLValue)(nil)

// Decode implements [envconfig.Decoder].
func (u *URLValue) Decode(value string) error {
	parsed, err := url.Parse(value)
	if err != nil {
		return fmt.Errorf("parsing url (%v): %w", value, err)
	}
	*u = URLValue{URL: parsed}

	return nil
}
