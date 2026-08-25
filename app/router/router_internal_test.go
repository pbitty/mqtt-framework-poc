package router

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type (
	TestNs struct {
		FieldA string
		FieldB string
		FieldC string
	}

	TestMsg struct {
		Value string
	}

	TestTopic = TopicDef[TestNs, TestMsg]
)

func TestGetPublishTopic(t *testing.T) {
	def := TestTopic{}.WithNamespace(TestNs{
		FieldA: "ValueA",
		FieldB: "ValueB",
		FieldC: "ValueC",
	})

	assert.Equal(t, "FieldA/ValueA/FieldB/ValueB/FieldC/ValueC/TestMsg", def.getPublishTopic())
}

func TestGetSubscribeTopic(t *testing.T) {
	def := TestTopic{}.WithNamespace(TestNs{
		FieldA: "ValueA",
		FieldB: "",
		FieldC: "",
	})

	assert.Equal(t, "FieldA/ValueA/FieldB/+/FieldC/+/TestMsg", def.getSubscribeTopic())
}

func TestTopicMatches(t *testing.T) {
	tcs := []struct {
		left, right TopicDef[TestNs, TestMsg]
		matches     bool
	}{
		{
			left:    TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "b", FieldC: "c"}),
			right:   TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "b", FieldC: "c"}),
			matches: true,
		},
		{
			left:    TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "b", FieldC: "c"}),
			right:   TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "b", FieldC: "another_value"}),
			matches: false,
		},
		{
			left:    TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "b", FieldC: "c"}),
			right:   TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "another_value", FieldC: "c"}),
			matches: false,
		},
		{
			left:    TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "", FieldC: "c"}),
			right:   TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "b", FieldC: "c"}),
			matches: true,
		},
		{
			left:    TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "", FieldC: "c"}),
			right:   TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "anything_goes", FieldC: "c"}),
			matches: true,
		},
		{
			left:    TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "", FieldC: "c"}),
			right:   TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "b", FieldC: "another_value"}),
			matches: false,
		},
		{
			left:    TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "b", FieldC: ""}),
			right:   TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "b", FieldC: "c"}),
			matches: true,
		},
		{
			left:    TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "b", FieldC: ""}),
			right:   TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "b", FieldC: "anything_goes"}),
			matches: true,
		},
		{
			left:    TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "b", FieldC: ""}),
			right:   TestTopic{}.WithNamespace(TestNs{FieldA: "a", FieldB: "another_value", FieldC: "c"}),
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
