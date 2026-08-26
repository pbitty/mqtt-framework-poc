package mqtt

import (
	"app/config"
	"context"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/eclipse/paho.golang/paho/session/state"
	"go.uber.org/fx"
)

func NewClient(cfg config.Config, logger *slog.Logger, lc fx.Lifecycle, sd fx.Shutdowner) (*autopaho.ConnectionManager, error) {
	conn, err := autopaho.NewConnection(context.Background(), autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{cfg.BrokerUrl.URL},
		KeepAlive:                     5,
		CleanStartOnInitialConnection: true,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, ca *paho.Connack) {
			logger.Info("connection_up", "conn", ca.String())
		},
		OnConnectError: func(err error) {
			logger.Error("connection_error", "error", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: cfg.ClientID,
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

	lc.Append(fx.StartHook(func(ctx context.Context) error {
		if err := conn.AwaitConnection(context.Background()); err != nil {
			return fmt.Errorf("error connecting: %w", err)
		}
		return nil
	}))

	return conn, nil
}
