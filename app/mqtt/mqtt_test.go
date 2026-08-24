package mqtt

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
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

	sub := r.GetSubscription(t)

	var m Temperature
	m, _ = sub.GetMessage()
	runtime.KeepAlive(m)

	ph := r.GetPublishHandle(t)
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
