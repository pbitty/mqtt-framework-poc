package router

import (
	"fmt"
	"reflect"
	"strings"
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
	return "TopicDef:" + t.getSubscribeTopic()
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
