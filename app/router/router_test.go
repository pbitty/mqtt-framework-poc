package router_test

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/url"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"app/router"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/eclipse/paho.golang/paho/session/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx/fxtest"
)

type RouterTestSuite struct {
	suite.Suite
	router *router.Router
	logger *slog.Logger
}

var _ suite.SetupTestSuite = (*RouterTestSuite)(nil)

const DefaultBrokerUrl = "mqtt://localhost:1883"

func TestRouterTestSuite(t *testing.T) {
	suite.Run(t, &RouterTestSuite{})
}

func (ts *RouterTestSuite) SetupTest() {
	logger := slog.New(slog.NewTextHandler(ts.T().Output(), &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	}))

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
		ConnectTimeout:                5 * time.Second,
		ClientConfig: paho.ClientConfig{
			ClientID:           clientId,
			Session:            state.NewInMemory(),
			OnServerDisconnect: func(d *paho.Disconnect) { ts.T().Log("server_disconnect: ", d.Properties.ReasonString) },
			OnClientError:      func(err error) { ts.T().Log("client_error", err) },
			OnPublishReceived: []func(pr paho.PublishReceived) (bool, error){func(pr paho.PublishReceived) (bool, error) {
				logger.Debug("publish_received", "packet", pr.Packet)
				return false, nil
			}},
		},
	})
	ts.Require().NoError(err, "error creating ConnectionManager")

	err = cm.AwaitConnection(ctx)
	ts.Require().NoError(err, "error starting connection to MQTT broker")

	ts.logger = logger

	lc := fxtest.NewLifecycle(ts.T())
	ts.router = router.NewRouter(cm, ts.logger, lc)

	lc.RequireStart()
}

func (ts *RouterTestSuite) TestPublishAndSubscribe() {
	type MyNamespace struct {
		Section    string
		SubSection string
		DeviceId   string
	}

	type MyMessage struct {
		Value string
	}

	type MyTopic = router.TopicDef[MyNamespace, MyMessage]

	type result struct {
		n MyNamespace
		m MyMessage
	}

	var (
		ctx = ts.T().Context()
		r   = ts.router

		t1 = MyTopic{}.WithNamespace(MyNamespace{Section: "A", SubSection: "B", DeviceId: "device-1"})
		t2 = MyTopic{}.WithNamespace(MyNamespace{Section: "A"})
		t3 = MyTopic{}.WithNamespace(MyNamespace{SubSection: "B"})

		result1 atomic.Value
		result2 atomic.Value
		result3 atomic.Value

		timeout = 5 * time.Second
		tick    = 100 * time.Millisecond
	)

	r.HandleSubscription(ctx, t1, func(n MyNamespace, m MyMessage) { result1.Store(result{n, m}) })
	r.HandleSubscription(ctx, t2, func(n MyNamespace, m MyMessage) { result2.Store(result{n, m}) })
	r.HandleSubscription(ctx, t3, func(n MyNamespace, m MyMessage) { result3.Store(result{n, m}) })

	r.GetPublishHandle(t1).Publish(ctx, MyMessage{Value: "hello world!"})

	ts.Assert().EventuallyWithT(func(collect *assert.CollectT) {
		assert.Equal(collect,
			result{
				MyNamespace{Section: "A", SubSection: "B", DeviceId: "device-1"},
				MyMessage{"hello world!"},
			},
			result1.Load(),
			"no message received on topic %+v", t1,
		)

		assert.Equal(collect,
			result{
				MyNamespace{Section: "A", SubSection: "B", DeviceId: "device-1"},
				MyMessage{"hello world!"},
			},
			result2.Load(),
			"no message received on topic %+v", t2,
		)

		assert.Equal(collect,
			result{
				MyNamespace{Section: "A", SubSection: "B", DeviceId: "device-1"},
				MyMessage{"hello world!"},
			},
			result3.Load(),
			"no message received on topic %+v", t3,
		)
	}, timeout, tick)
}

func (ts *RouterTestSuite) TestRequestResponse() {
	type MyNamespace struct {
		Section    string
		SubSection string
		DeviceId   string
	}

	type MyRequest struct {
		Value string
	}

	type MyResponse struct {
		Value string
	}

	type MyEndpoint = router.Endpoint[MyNamespace, MyRequest, MyResponse]

	type result struct {
		n MyNamespace
		m MyRequest
	}

	var (
		ctx = ts.T().Context()
		r   = ts.router
		e   = MyEndpoint{}.WithNamespace(MyNamespace{Section: "A", SubSection: "B", DeviceId: "device-1"})
	)

	r.HandleRequest(ctx, e, func(_ MyNamespace, r MyRequest) MyResponse {
		return MyResponse{
			Value: "response for " + r.Value,
		}
	})

	res, err := r.SendRequest(ctx, e, MyRequest{Value: "request 1"})
	ts.Require().NoError(err)

	ts.Assert().Equal("response for request 1", res.Value)
}

func ExampleTopicDef() {
	type Namespace struct {
		FirstField  string
		SecondField string
		ThirdField  string
	}
	type Message struct {
		Value1 string
		Value2 float64
	}

	type Topic = router.TopicDef[Namespace, Message]

	td := Topic{}.WithNamespace(Namespace{FirstField: "first", SecondField: "second", ThirdField: "third"})
	fmt.Printf("%+v", td)
	// Output: TopicDef:FirstField/first/SecondField/second/ThirdField/third/Message
}
