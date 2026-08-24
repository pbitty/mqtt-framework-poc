package mqtt

import (
	"math/rand/v2"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/eclipse/paho.golang/paho/session/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// TestThatItCompiles does not make any assertions
// but it uses the types and methods to ensure that the type constraints work
func TestThatItCompiles(*testing.T) {
	type DeviceNamespace struct {
		Building string
		Sector   string
		DeviceID string
	}

	type Temperature struct {
		Value float64
	}

	type DeviceTemperatureTopic = TopicDef[DeviceNamespace, Temperature]

	t := DeviceTemperatureTopic{
		DeviceNamespace{
			Building: "BuildingA",
			Sector:   "SectorA",
			DeviceID: "DeviceFoo",
		},
	}

	r := Router{}

	sub, _ := r.GetSubscription(t)

	var m Temperature
	m, _ = sub.GetMessage()
	runtime.KeepAlive(m)

	ph, _ := r.GetPublishHandle(t)
	ph.Publish(Temperature{Value: 5.0})
}

func TestGetPublishTopic(t *testing.T) {
	type ns struct {
		FieldA string
		FieldB string
		FieldC string
	}

	type msg struct{}

	def := TopicDef[ns, msg]{
		Namespace: ns{
			FieldA: "ValueA",
			FieldB: "ValueB",
			FieldC: "ValueC",
		},
	}

	assert.Equal(t, "FieldA/ValueA/FieldB/ValueB/FieldC/ValueC/msg", def.GetPublishTopic())
}

func TestGetSubscribeFilter(t *testing.T) {
	type ns struct {
		FieldA string
		FieldB string
		FieldC string
	}

	type msg struct{}

	def := TopicDef[ns, msg]{
		Namespace: ns{
			FieldA: "ValueA",
			FieldB: "",
			FieldC: "",
		},
	}

	assert.Equal(t, "FieldA/ValueA/FieldB/+/FieldC/+/msg", def.GetSubscribeFilter())
}

type RouterTestSuite struct {
	suite.Suite
	cm *autopaho.ConnectionManager
}

var _ suite.SetupTestSuite = (*RouterTestSuite)(nil)

const DefaultBrokerUrl = "mqtt://localhost:1883"

func TestRouterTestSuite(t *testing.T) {
	suite.Run(t, &RouterTestSuite{})
}

func (ts *RouterTestSuite) SetupTest() {
	clientId := ts.T().Name() +
		"-" + time.Now().Format("20060102150405") +
		"-" + strconv.Itoa(rand.Int())

	brokerUrl := os.Getenv("BROKER_URL")
	if brokerUrl == "" {
		brokerUrl = DefaultBrokerUrl
	}

	u, err := url.Parse(brokerUrl)
	ts.Require().NoError(err, "error parsing BROKER_URL: %s", brokerUrl)

	ts.T().Log("connecting to brokerUrl: ", brokerUrl)

	ctx := ts.T().Context()

	cm, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     5,
		CleanStartOnInitialConnection: true,
		OnConnectError:                func(err error) { ts.T().Log("connection_error: ", err) },
		ClientConfig: paho.ClientConfig{
			ClientID:           clientId,
			Session:            state.NewInMemory(),
			OnServerDisconnect: func(d *paho.Disconnect) { ts.T().Log("server_disconnect: ", d.Properties.ReasonString) },
			OnClientError:      func(err error) { ts.T().Log("client_error", err) },
		},
	})
	ts.Require().NoError(err, "error creating ConnectionManager")

	err = cm.AwaitConnection(ctx)
	ts.Require().NoError(err, "error starting connection to MQTT broker")

	ts.cm = cm
}

func (ts *RouterTestSuite) TestBasic() {
	r := NewRouter(ts.cm)
	ts.Require().NotNil(r)
}
