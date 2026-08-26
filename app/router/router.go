package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

// TopicDef represents typed a Message that can be sent/received under a Namespace.
//
// Namespace must be a struct type with only string fields.  Field names and values
// are used to generate the topic name, in the order the fields are defined in the struct.
//
// Message must be a struct type and all fields must be Marshalable by the Router's marshaler.
// Currently only JSON is supported.
//
// A typical usage defines the namespace and message types, defines the topic using a type alias,
// and then uses [TopicDef.WithNamespace] to create an instance of the topic for subscribing to
// or publishing messages
//
// Example:
//
//	type MyNamespace struct {
//		FirstField  string
//		SecondField string
//		ThirdField  string
//	}
//
//	type MyMessage struct {
//		Value string
//	}
//
//	type MyTopic = mqtt.TopicDef[MyNamespace, MyMessage]
//
//	t := MyTopic{}.WithNamespace(MyNamespace{FirstField: "A", SecondField: "B", ThirdField: "C"})
//
//	ph := router.GetPublishHandle(t)
//
//	ph.Publish(ctx, Message{Value: "hello"})
type TopicDef[Namespace any, Message any] struct {
	namespace Namespace
}

func (t TopicDef[N, M]) Namespace() N {
	return t.namespace
}

func (t TopicDef[N, M]) WithNamespace(n N) TopicDef[N, M] {
	validateTypes[N, M]()
	return TopicDef[N, M]{namespace: n}
}

func (t TopicDef[Namespace, Message]) String() string {
	return "TopicDef:" + t.getPublishTopic()
}

func (t TopicDef[P, M]) getPublishTopic() string {
	return t.generateTopicPath(false)
}

func (t TopicDef[P, M]) getSubscribeTopic() string {
	return t.generateTopicPath(true)
}

func (t TopicDef[P, M]) getSubscribeSegments() topicSegments {
	return topicSegments(t.generateTopicSegments(true))
}

func (t TopicDef[P, M]) generateTopicPath(emptyValuesAsWildcards bool) string {
	parts := t.generateTopicSegments(emptyValuesAsWildcards)
	return strings.Join(parts, "/")
}

func validateTypes[N, M any]() {
	ns := reflect.TypeFor[N]()
	if ns.Kind() != reflect.Struct {
		panic(fmt.Sprintf("Namespace must be a struct, found %s", ns))
	}

	msg := reflect.TypeFor[M]()
	if msg.Kind() != reflect.Struct {
		panic(fmt.Sprintf("Message type (M) must be a struct, found %s", msg.Kind()))
	}
	if msg.Name() == "" {
		panic(fmt.Sprintf("Message struct must be named (cannot be anonymous)"))
	}
}

func (t TopicDef[N, M]) generateTopicSegments(emptyValuesAsWildcards bool) []string {
	ns := reflect.ValueOf(t.namespace)
	parts := make([]string, 0,
		// namespace field+value pairs, plus the name of M
		ns.NumField()*2+1,
	)

	for t, v := range ns.Fields() {
		val := v.String()
		if val == "" {
			if !emptyValuesAsWildcards {
				panic(fmt.Sprintf("Value was empty for field %s", t.Name))
			}
			val = "+"
		}

		parts = append(parts, t.Name, val)
	}

	parts = append(parts, reflect.TypeFor[M]().Name())
	return parts
}

func (t TopicDef[N, M]) namespaceFromTopic(topic string) N {
	// copy value so as not to mutate original
	nsv := t.namespace
	ns := reflect.ValueOf(&nsv).Elem()

	parts := strings.Split(topic, "/")

	// Topic should have two segments per field, plus the message type at the end
	// e.g. Field1/Value1/Field2/Value2/MessageType
	expectedPartsLen := (ns.NumField()*2 + 1)
	partsLen := len(parts)

	if partsLen != expectedPartsLen {
		panic(fmt.Sprintf("topic should have %d parts for namespace %s, but has %d",
			expectedPartsLen,
			ns.Type().Name(),
			partsLen,
		))
	}

	for i := 1; i < partsLen; i += 2 {
		f := ns.Field(i / 2)
		v := reflect.ValueOf(parts[i])
		f.Set(v)
	}

	return ns.Interface().(N)
}

type topicSegments []string

func (r topicSegments) matchesTopic(topic string) bool {
	partsA := r
	partsB := strings.Split(topic, "/")

	if len(partsA) != len(partsB) {
		return false
	}

	for i := 0; i < len(partsA); i++ {
		a := partsA[i]
		b := partsB[i]

		if a != "+" && a != b {
			return false
		}
	}
	return true
}

type SubscriptionHandler[Namespace, Message any] func(Namespace, Message)

type PublishHandle[M any] struct {
	cm        *autopaho.ConnectionManager
	marshalFn func(any) ([]byte, error)
	topic     string
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

	return nil
}

type subInfo struct {
	routePath topicSegments
	handlers  []func(*paho.Publish)
}

type Router struct {
	cm         *autopaho.ConnectionManager
	deregister func()
	logger     *slog.Logger
	subs       map[string]*subInfo
	subsMu     sync.RWMutex

	marshal   func(any) ([]byte, error)
	unmarshal func([]byte, any) error
}

func NewRouter(cm *autopaho.ConnectionManager, logger *slog.Logger) *Router {
	r := &Router{
		cm:     cm,
		logger: logger,
		subs:   make(map[string]*subInfo),

		marshal:   json.Marshal,
		unmarshal: json.Unmarshal,
	}

	var once sync.Once
	deregister := cm.AddOnPublishReceived(r.onPublishReceived)
	r.deregister = func() { once.Do(deregister) }

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

		r.subs[topic] = &subInfo{
			routePath: route.getSubscribeSegments(),
			handlers:  make([]func(*paho.Publish), 0),
		}
	}

	r.subs[topic].handlers = append(r.subs[topic].handlers, r.newRouteHandler(route, h))

	return nil
}

func (r *Router) GetPublishHandle[N, M any](t TopicDef[N, M]) PublishHandle[M] {
	return PublishHandle[M]{
		cm:        r.cm,
		marshalFn: r.marshal,
		topic:     t.getPublishTopic(),
	}
}

func (r *Router) Close() {
	// TODO: Is there a way to clean up subscriptions without potentially removing overlapping subscriptions from a different router?
	r.deregister()
}

func (r *Router) onPublishReceived(pr autopaho.PublishReceived) (bool, error) {
	r.subsMu.RLock()
	defer r.subsMu.RUnlock()

	for _, s := range r.subs {
		if s.routePath.matchesTopic(pr.Packet.Topic) {
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
