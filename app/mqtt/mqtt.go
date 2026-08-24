package mqtt

import (
	"fmt"
	"reflect"
	"strings"
)

type TopicDef[N any, M any] struct {
	Namespace N
}

func (t TopicDef[P, M]) GetPublishTopic() string {
	return t.generateTopicPath(false)
}

func (t TopicDef[P, M]) GetSubscribeFilter() string {
	return t.generateTopicPath(true)
}

func (t TopicDef[P, M]) generateTopicPath(emptyValuesAreWildcards bool) string {
	ns := reflect.ValueOf(t.Namespace)
	if ns.Kind() != reflect.Struct {
		panic(fmt.Sprintf("Namespace must be a struct, found %s", ns))
	}

	// K/V pairs Namespace fields, plus the name of M
	parts := make([]string, ns.NumField()*2+1)

	idx := 0
	for t, v := range ns.Fields() {
		if t.Type.Kind() != reflect.String {
			panic(fmt.Sprintf("Namespace fields must be strings, found %s for field %s", t.Type, t.Name))
		}

		val := v.String()
		if val == "" {
			if !emptyValuesAreWildcards {
				panic(fmt.Sprintf("Value was empty for field %s", t.Name))
			}
			val = "+"
		}

		parts[idx] = t.Name
		parts[idx+1] = val

		idx += 2
	}

	// idx is the last segment
	parts[idx] = reflect.TypeFor[M]().Name()

	return strings.Join(parts, "/")
}

type Subscription[M any] struct{}

func (s *Subscription[M]) GetMessage() (M, error) {
	var m M
	return m, nil
}

type PublishHandle[M any] struct{}

func (p *PublishHandle[M]) Publish(m M) error {
	return nil
}

type Router struct {
}

func (r *Router) GetSubscription[N, M any](t TopicDef[N, M]) Subscription[M] {
	var s Subscription[M]
	return s
}

func (r *Router) GetPublishHandle[N, M any](t TopicDef[N, M]) PublishHandle[M] {
	var p PublishHandle[M]
	return p
}
