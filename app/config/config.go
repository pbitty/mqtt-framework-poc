package config

import (
	"fmt"
	"net/url"

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

func NewDeviceConfig() (DeviceConfig, error) {
	var c DeviceConfig
	if err := envconfig.Process("", &c); err != nil {
		return c, fmt.Errorf("parsing env vars for DeviceConfig: %w", err)
	}

	return c, nil
}

type ServiceConfig struct {
	Config
}

func NewServiceConfig() (ServiceConfig, error) {
	var c ServiceConfig
	if err := envconfig.Process("", &c); err != nil {
		return c, fmt.Errorf("parsing env vars for ServiceConfig: %w", err)
	}

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
	*u.URL = *parsed

	return nil
}
