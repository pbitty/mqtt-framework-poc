package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"go.uber.org/fx"
)

// Router enables type-safe publishing and handling of messages in MQTT.
//
// See the main README for more details.
type Router struct {
	cm           *autopaho.ConnectionManager
	deregister   func()
	logger       *slog.Logger
	subs         map[string]*subInfo
	subsMu       sync.RWMutex
	subHandlerId atomic.Int64

	marshal   func(any) ([]byte, error)
	unmarshal func([]byte, any) error
}

func NewRouter(cm *autopaho.ConnectionManager, logger *slog.Logger, lc fx.Lifecycle) *Router {
	r := &Router{
		cm:     cm,
		logger: logger,
		subs:   make(map[string]*subInfo),

		marshal:   json.Marshal,
		unmarshal: json.Unmarshal,

		deregister: func() {}, // no-op until started
	}

	lc.Append(fx.StartHook(func() {
		// Register our callback in start hook so that ConnectionManager is guaranteed to be connected
		deregister := cm.AddOnPublishReceived(r.onPublishReceived)

		var once sync.Once
		r.deregister = func() { once.Do(deregister) }
	}))

	return r
}

func (r *Router) HandleSubscription[N, M any](ctx context.Context, route TopicDef[N, M], h SubscriptionHandler[N, M]) error {
	_, err := r.registerRouteHandler(ctx, route,
		func(_ *paho.Publish, ns N, msg M) {
			// For subscriptions we don't need anything on *paho.Publish
			h(ns, msg)
		},
	)

	return err
}

func (r *Router) GetPublishHandle[N, M any](t TopicDef[N, M]) PublishHandle[M] {
	return PublishHandle[M]{
		router: r,
		topic:  t.getPublishTopic(),
	}
}

func (r *Router) HandleRequest[Ns, Req, Res any](ctx context.Context, e Endpoint[Ns, Req, Res], h EndpointHandler[Ns, Req, Res]) error {
	_, err := r.registerRouteHandler(ctx, e.route,
		func(p *paho.Publish, n Ns, req Req) {
			logger := r.logger.With("publish_topic", p.Topic)

			if p.Properties == nil {
				logger.Error("missing_publish_properties")
				return
			}

			rt := p.Properties.ResponseTopic
			if rt == "" {
				logger.Error("missing_response_topic")
				return
			}

			res := h(n, req)

			if err := r.publish(ctx, rt, "", res); err != nil {
				logger.Error("publishing_response", "response_topic", rt)
			}
		},
	)

	return err
}

func (r *Router) SendRequest[Ns, Req, Res any](ctx context.Context, e Endpoint[Ns, Req, Res], req Req) (Res, error) {
	res, close, err := r.SendBroadcastRequest(ctx, e, req)
	if err != nil {
		var r Res
		return r, err
	}

	defer close()

	select {
	case r := <-res:
		return r, nil
	case <-ctx.Done():
		var r Res
		return r, ctx.Err()
	}
}

type ResponseClose func()

func (r *Router) SendBroadcastRequest[Ns, Req, Res any](
	ctx context.Context,
	e Endpoint[Ns, Req, Res],
	req Req,
) (<-chan Res, ResponseClose, error) {
	publishTopic := e.route.getPublishTopic()
	responseTopic := TopicDef[Ns, Res]{}.WithNamespace(e.route.namespace)

	responses := make(chan Res)

	deregister, err := r.registerRouteHandler(ctx, responseTopic,
		func(_ *paho.Publish, _ Ns, res Res) {
			// TODO Handle CorrelationID here - skip response if it does not match our
			responses <- res
		},
	)
	if err != nil {
		return nil, nil, err
	}

	out := make(chan Res)
	abort := make(chan struct{})

	go func() {
		// We buffer responses to avoid blocking the routeHandler above if our caller does not drain `out` fast enough.
		// If our caller is ready to receive, then `buf` does not allocates any memory.
		var buf []Res

		for {
			if len(buf) == 0 {
				// Send or buffer
				select {
				case r := <-responses:
					select {
					// If our caller is ready and the buffer is empty, we can send the response directly without disturbing ordering
					case out <- r:
					default:
						buf = append(buf, r)
					}
				case <-abort:
					close(out)
					return
				}
			} else {
				// Drain or buffer
				select {
				case r := <-responses:
					buf = append(buf, r)
				case out <- buf[0]:
					buf = buf[1:]
				case <-abort:
					close(out)
					return
				}
			}
		}
	}()

	cleanUp := func() {
		deregister()
		close(abort)
	}

	// TODO Add correlation ID to request
	if err := r.publish(ctx, publishTopic, responseTopic.getPublishTopic(), req); err != nil {
		cleanUp()
		return nil, nil, err
	}

	return out, cleanUp, nil
}

func (r *Router) Close() {
	// TODO: Is there a way to clean up subscriptions without potentially removing overlapping subscriptions from a different router?
	r.deregister()
}

func (r *Router) registerRouteHandler[N, M any](ctx context.Context, route TopicDef[N, M], rh func(*paho.Publish, N, M)) (func(), error) {
	r.subsMu.Lock()
	defer r.subsMu.Unlock()

	topic := route.getSubscribeTopic()

	if _, ok := r.subs[topic]; !ok {
		_, err := r.cm.Subscribe(ctx, &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{{Topic: topic}},
		})
		if err != nil {
			return nil, fmt.Errorf("subscribing to topic %s: %w", topic, err)
		}

		r.logger.Debug("subscribed_to_topic", "topic", topic)

		r.subs[topic] = &subInfo{
			routePath: route.getSubscribeSegments(),
			handlers:  make(map[int64]func(*paho.Publish)),
		}
	}

	// This gives us a unique identifier for each registration
	hid := r.subHandlerId.Add(1)

	r.subs[topic].handlers[hid] = r.newRouteHandler(route, rh)
	r.logger.Debug("subscription_handler_registered", "route", route)

	deregister := func() {
		r.subsMu.Lock()
		defer r.subsMu.Unlock()
		delete(r.subs[topic].handlers, hid)
		// TODO implement removal of subscription when there are no handlers
		// Consider waiting for a quiescence period before removing to avoid thrashing the broker with SUBCRIBE/UNSUBSCRIBE packets
	}

	return deregister, nil
}

func (r *Router) publish[M any](ctx context.Context, topic string, responseTopic string, m M) error {
	payload, err := r.marshal(m)
	if err != nil {
		return fmt.Errorf("encoding payload for topic (%s): %w", topic, err)
	}

	_, err = r.cm.Publish(ctx, &paho.Publish{
		Topic:   topic,
		Payload: payload,
		Properties: &paho.PublishProperties{
			ResponseTopic: responseTopic,
		},
	})
	if err != nil {
		return fmt.Errorf("publishing message to topic (%s): %w", topic, err)
	}
	r.logger.Debug("published_to_topic", "topic", topic)

	return nil
}

func (r *Router) onPublishReceived(pr autopaho.PublishReceived) (bool, error) {
	r.subsMu.RLock()
	defer r.subsMu.RUnlock()

	topic := pr.Packet.Topic
	r.logger.Debug("message_received", "topic", topic, "payload", pr.Packet.Payload)

	for _, s := range r.subs {
		if s.routePath.matchesTopic(topic) {
			r.logger.Debug("route_matched", "route", s.routePath, "topic", topic)
			for _, h := range s.handlers {
				h(pr.Packet)
			}
		}
	}

	return false, nil
}

func (r *Router) newRouteHandler[Ns, Req any](route TopicDef[Ns, Req], h func(*paho.Publish, Ns, Req)) func(*paho.Publish) {
	return func(pb *paho.Publish) {
		topic := pb.Topic

		var msg Req
		if err := r.unmarshal(pb.Payload, &msg); err != nil {
			r.logger.Error("error_unmarshalling_message", "route", route.getSubscribeTopic(), "topic", topic)
			return
		}

		ns := namespaceFromTopic[Ns](topic)
		h(pb, ns, msg)
	}
}

type subInfo struct {
	routePath topicSegments
	handlers  map[int64]func(*paho.Publish)
}

type SubscriptionHandler[Namespace, Message any] func(Namespace, Message)

type PublishHandle[M any] struct {
	router *Router
	topic  string
}

func (p PublishHandle[M]) Publish(ctx context.Context, m M) error {
	return p.router.publish(ctx, p.topic, "", m)
}

type Endpoint[Namespace, Request, Response any] struct {
	route TopicDef[Namespace, Request]
}

func (e Endpoint[Ns, Req, Res]) WithNamespace(n Ns) Endpoint[Ns, Req, Res] {
	return Endpoint[Ns, Req, Res]{route: e.route.WithNamespace(n)}
}

type EndpointHandler[Ns, Req, Res any] func(Ns, Req) Res
