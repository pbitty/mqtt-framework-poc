package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"go.uber.org/fx"
)

type Router struct {
	cm         *autopaho.ConnectionManager
	deregister func()
	logger     *slog.Logger
	subs       map[string]*subInfo
	subsMu     sync.RWMutex

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
	r.subsMu.Lock()
	defer r.subsMu.Unlock()

	topic := route.getSubscribeTopic()

	if _, ok := r.subs[topic]; !ok {
		_, err := r.cm.Subscribe(ctx, &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{{Topic: topic}},
		})
		if err != nil {
			return fmt.Errorf("subscribing to topic %s: %w", topic, err)
		}

		r.logger.Debug("subscribed_to_topic", "topic", topic)

		r.subs[topic] = &subInfo{
			routePath: route.getSubscribeSegments(),
			handlers:  make([]func(*paho.Publish), 0),
		}
	}

	r.subs[topic].handlers = append(r.subs[topic].handlers, r.newRouteHandler(route, h))

	r.logger.Debug("subscription_handler_registered", "route", route)

	return nil
}

func (r *Router) GetPublishHandle[N, M any](t TopicDef[N, M]) PublishHandle[M] {
	return PublishHandle[M]{
		cm:        r.cm,
		marshalFn: r.marshal,
		topic:     t.getPublishTopic(),
		logger:    r.logger,
	}
}

func (r *Router) Close() {
	// TODO: Is there a way to clean up subscriptions without potentially removing overlapping subscriptions from a different router?
	r.deregister()
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

func (r *Router) newRouteHandler[N, M any](route TopicDef[N, M], h SubscriptionHandler[N, M]) func(*paho.Publish) {
	return func(pb *paho.Publish) {
		topic := pb.Topic

		var msg M
		if err := r.unmarshal(pb.Payload, &msg); err != nil {
			r.logger.Error("error_unmarshalling_message", "route", route.getSubscribeTopic(), "topic", topic)
			return
		}

		ns := route.namespaceFromTopic(topic)
		h(ns, msg)
	}
}

type subInfo struct {
	routePath topicSegments
	handlers  []func(*paho.Publish)
}

type SubscriptionHandler[Namespace, Message any] func(Namespace, Message)

type PublishHandle[M any] struct {
	cm        *autopaho.ConnectionManager
	marshalFn func(any) ([]byte, error)
	topic     string

	logger *slog.Logger
}

func (p PublishHandle[M]) Publish(ctx context.Context, m M) error {
	topic := p.topic

	payload, err := p.marshalFn(m)
	if err != nil {
		return fmt.Errorf("encoding payload for topic (%s): %w", topic, err)
	}

	_, err = p.cm.Publish(ctx, &paho.Publish{
		Topic:   topic,
		Payload: payload,
	})
	if err != nil {
		return fmt.Errorf("publishing message to topic (%s): %w", topic, err)
	}
	p.logger.Debug("published_to_topic", "topic", topic)

	return nil
}
