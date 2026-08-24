package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"

	"charm.land/log/v2"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/eclipse/paho.golang/paho/session/state"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

type Config struct {
	ClientID  string `envconfig:"CLIENT_ID"   default:"my-client-id"`
	BrokerUrl string `envconfig:"BROKER_URL"  default:"mqtt://127.0.0.1:1883"`
	Topic     string `envconfig:"TOPIC"       default:"/my-topic/#"`
}

func NewConfig() (Config, error) {
	var c Config
	if err := envconfig.Process("", &c); err != nil {
		return Config{}, fmt.Errorf("reading config from env: %w", err)
	}
	return c, nil
}

func main() {
	fx.New(
		fx.Invoke(EntryPoint),
		fx.Provide(NewMqttClient),
		fx.Provide(NewLogger),
		fx.Provide(NewConfig),
		fx.WithLogger(NewFxLogger),
	).Run()
}

func EntryPoint(cfg Config, conn *autopaho.ConnectionManager, logger *slog.Logger) error {
	logger.Info("config", "config", cfg)

	topic := cfg.Topic

	logger.Info("subscribing", "topic", topic)

	subAck, err := conn.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{
			paho.SubscribeOptions{
				Topic:             topic,
				QoS:               0,
				RetainHandling:    0,
				NoLocal:           false,
				RetainAsPublished: false,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("subscribing to %s: %w", topic, err)
	}

	if subAck.Properties != nil {
		logger.Debug("suback_received", "reason", subAck.Properties.ReasonString)
	}

	conn.AddOnPublishReceived(func(pr autopaho.PublishReceived) (bool, error) {
		logger.Info("publish_received", "topic", pr.Packet.Topic, "payload", string(pr.Packet.Payload), "pr", pr.PublishReceived)
		return false, nil
	})

	return nil
}

func NewMqttClient(cfg Config, logger *slog.Logger) (*autopaho.ConnectionManager, error) {
	u, err := url.Parse(cfg.BrokerUrl)
	if err != nil {
		return nil, fmt.Errorf("parsing URL: %w", err)
	}

	conn, err := autopaho.NewConnection(context.Background(), autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     5,
		CleanStartOnInitialConnection: true,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, ca *paho.Connack) {
			logger.Info("connection_up", "conn", ca.String())
		},
		OnConnectError: func(err error) {
			logger.Error("connection_error", "error", err)
		},
		// ConnectUsername: "",
		// ConnectPassword: []byte{},
		ClientConfig: paho.ClientConfig{
			ClientID: "my-client-id",
			Session:  state.NewInMemory(),
			OnServerDisconnect: func(d *paho.Disconnect) {
				logger.Info("server_disconnect", "reason", d.Properties.ReasonString)
			},
			OnClientError: func(err error) {
				logger.Error("client_error", "error", err)
			},
		},
	})
	if err != nil {
		return nil, err
	}

	if err := conn.AwaitConnection(context.Background()); err != nil {
		return nil, fmt.Errorf("error connecting: %w", err)
	}

	return conn, nil
}

func NewLogger() *slog.Logger {
	handler := log.New(os.Stderr)
	handler.SetReportTimestamp(true)
	return slog.New(handler)
}

func NewFxLogger(l *slog.Logger) fxevent.Logger {
	return &fxevent.SlogLogger{
		Logger: l,
	}
}
