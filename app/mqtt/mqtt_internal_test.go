package mqtt

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetPublishTopic(t *testing.T) {
	type ns struct {
		FieldA string
		FieldB string
		FieldC string
	}

	type msg struct{}

	def := NewTopicDef(ns{
		FieldA: "ValueA",
		FieldB: "ValueB",
		FieldC: "ValueC",
	},
		msg{},
	)

	assert.Equal(t, "FieldA/ValueA/FieldB/ValueB/FieldC/ValueC/msg", def.getPublishTopic())
}

func TestTopicMatches(t *testing.T) {
	type Ns struct {
		A string
		B string
		C string
	}

	type Msg struct {
		Value string
	}

	tcs := []struct {
		left, right TopicDef[Ns, Msg]
		matches     bool
	}{
		{
			left:    NewTopicDef(Ns{A: "a", B: "b", C: "c"}, Msg{}),
			right:   NewTopicDef(Ns{A: "a", B: "b", C: "c"}, Msg{}),
			matches: true,
		},
		{
			left:    NewTopicDef(Ns{A: "a", B: "b", C: "c"}, Msg{}),
			right:   NewTopicDef(Ns{A: "a", B: "b", C: "another_value"}, Msg{}),
			matches: false,
		},
		{
			left:    NewTopicDef(Ns{A: "a", B: "b", C: "c"}, Msg{}),
			right:   NewTopicDef(Ns{A: "a", B: "another_value", C: "c"}, Msg{}),
			matches: false,
		},
		{
			left:    NewTopicDef(Ns{A: "a", B: "", C: "c"}, Msg{}),
			right:   NewTopicDef(Ns{A: "a", B: "b", C: "c"}, Msg{}),
			matches: true,
		},
		{
			left:    NewTopicDef(Ns{A: "a", B: "", C: "c"}, Msg{}),
			right:   NewTopicDef(Ns{A: "a", B: "anything_goes", C: "c"}, Msg{}),
			matches: true,
		},
		{
			left:    NewTopicDef(Ns{A: "a", B: "", C: "c"}, Msg{}),
			right:   NewTopicDef(Ns{A: "a", B: "b", C: "another_value"}, Msg{}),
			matches: false,
		},
		{
			left:    NewTopicDef(Ns{A: "a", B: "b", C: ""}, Msg{}),
			right:   NewTopicDef(Ns{A: "a", B: "b", C: "c"}, Msg{}),
			matches: true,
		},
		{
			left:    NewTopicDef(Ns{A: "a", B: "b", C: ""}, Msg{}),
			right:   NewTopicDef(Ns{A: "a", B: "b", C: "anything_goes"}, Msg{}),
			matches: true,
		},
		{
			left:    NewTopicDef(Ns{A: "a", B: "b", C: ""}, Msg{}),
			right:   NewTopicDef(Ns{A: "a", B: "another_value", C: "c"}, Msg{}),
			matches: false,
		},
	}

	for i, tc := range tcs {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			matches := tc.left.getSubscribeSegments().matchesTopic(tc.right.getPublishTopic())
			assert.Equal(t, tc.matches, matches)
		})
	}
}

func TestGetSubscribeTopic(t *testing.T) {
	type Ns struct {
		FieldA string
		FieldB string
		FieldC string
	}

	type Msg struct{}

	def := NewTopicDef(
		Ns{
			FieldA: "ValueA",
			FieldB: "",
			FieldC: "",
		},
		Msg{},
	)

	assert.Equal(t, "FieldA/ValueA/FieldB/+/FieldC/+/msg", def.getSubscribeTopic())
}
