# mqtt-framework-poc

> [!NOTE]
> This project is a PoC based on my ongoing learning of MQTT.  Any observations made about MQTT are based 
> on limited experience.
> 
> The APIs explored here are not based on years of experience and might be misguided.  Feedback is welcome.

## Overview

In this repo I am exploring an API for defining a typed topic/message schema in Go, such that MQTT send/receive can be done in a type-safe manner using Go types, and the framework will handle topic mapping and message serialization/deserialization.

## Pub/Sub API

The general API is as follows:

```go
// Define your topic/message types using structs
type (
    // Define a namespace for each unique device
	Device struct {
		Region string
		Zone   string
		ID     string
	}
    // Define a message type for serializing/deserializing the payload
	TemperatureMessage struct {
		TemperatureCelcius float64
	}
    // Define the topic+message mapping
	TemperatureTopic = router.TopicDef[Device, TemperatureMessage]
)

//
// Publish messages via the router API
//

// Define a unique namespace for the device.  Fields names+values determine the unique topic for each device
dev := Device{Region: "A", Zone: "B", ID: "1234"}
// Define the topic for this specific Device
topic := TemperatureTopic{}.WithNamespace(dev)
// Get a publish handle that can be re-used.
h := router.GetPublishHandle(topic)
// Publish a message
h.Publish(ctx, TemperatureMessage{TemperatureCelcius: 23.0})


//
// Handle subscriptions via the router API
//

// Subscribe to all Devices with an empty namespace in TemperatureTopic{}
router.HandleSubscription(ctx, TemperatureTopic{},
    func(dev Device, msg TemperatureMessage) {
        // Handle message here
    },
)
// Subscribe to a subset of Devices by constraining some fields of the namespace
router.HandleSubscription(ctx, TemperatureTopic{}.WithNamespace(Device{Region: "A"}),
    func(dev Device, msg TemperatureMessage) {
            // Handle only messages from Region "A" here
    },
)
```

For more details see the [router](./app/router/doc.go) package.

## Future concerns to explore

* Request/Response API
* Different encodings
* Schema Versioning

## Example application

An example application is implemented in the [service](./app/service/) package, with executables in [cmd](./app/cmd).

To run the examples:

1. Ensure [Docker Compose](https://docs.docker.com/compose/install/) and [Task](https://taskfile.dev/docs/installation) are installed.

2. Start the `service` process

       task run-service

   Without `task` you can run Docker Compose directly:

       docker compose up service

3. In another shell, start the `device` processes, which simulate publishing temperature readings

       task run-devices

   Without `task` you can run Docker Compose directly:

       docker compose up device-1 device-2 device-3



----------------------------------------------------------------------------------------------------

## Framework Design Notes

MQTT is a very flexible protocol, allowing for a wide range of communication patterns between systems (e.g point-to-point, broadcast, fan-in, etc.).  Topic names are strings, and message payloads are raw bytes with no opinion on data format.  Features like subscription wildcards and response topics allow for arbitrary higher-order functionality to be built.

As such, MQTT is a low-level protocol, and an application/system to be maintainable would need to layer its own semantics on top of it.  This brings the question: what kinds of frameworks can support common communication patterns at an application/language level?

Such a framework might have the following capabilities:

* Type-safe encoding/decoding of messages
* Type-safe mapping between messages and topics
* Type-safe API for request/response
* Type-safe API for pub/sub
* Encoding topic structures/hierarchies as types

### Type-safe machine-managed topic names and subscriptions

A topic can be represented by ordered K/V pairs in the form `KeyA/ValueA/KeyB/ValueB/KeyC/ValueC`.  If we know all of the keys, we can subscribe to specific values of any key, or all values using the `+` wildcard.  For example, if we want all topics with `KeyB=ValueB` we can use the subscription filter `KeyA/+/KeyB/ValueB/KeyC/+`.

This starts to give us a schema for defining topics.  A given topic can be defined as a struct (e.g in Go, or a Tuple in Python), and since the keys are in the topic path, structs with different key sets will never overlap.

For example, the following struct could represent a device's topic in Go:
```go
// Device represents the topic hierarchy for a device in a facility
//
// The field names are arbitrary and determined by the specific use-case.
// The field names and values are used by the framework for generating topic names for publishing/subscribing.
type Device struct {
    Type     string
    Zone     string
    Building string
    Level    string
    Room     string
    ID       string
}

d := Device{
    Type:     "A",
    Zone:     "B",
    Building: "C",
    Level:    "D",
    Room:     "E",
    ID:       "1234-abcd",
}
```

Such a struct would generate the topic `Type/A/Zone/B/Building/C/Level/D/Room/E/ID/1234-abcd`.


### Type-safe messages

A message for a given topic can also be defined with a type-safe schema, and the message name can be be the last segment of the topic name.

e.g. `KeyA/ValueA/KeyB/ValueB/KeyC/ValueC/message_type_name`

This allows the framework to take a schema consisting of topic+message and deterministically generate a topic name, and encode/decode the message payload.

Following the example above in Go:
```go
type TemperatureReading struct {
    TemperatureCelcius float64
}
```

Joined with the `Device` topic above, the topic name would be `Type/A/Zone/B/Building/C/Level/D/Room/E/ID/1234-abcd/TemperatureReading`.

## Work log

### 2026-08-21

Open questions:

* How does one structure high-level communication patterns using MQTT?
* How does one do request/response with a given device?
* How does a device report current state?
* How does a server report state to a device?
* How can topic names be used to manage communication patterns?

[Designing MQTT Topics for AWS IoT Core - AWS Whitepaper](https://docs.aws.amazon.com/pdfs/whitepapers/latest/designing-mqtt-topics-aws-iot-core/designing-mqtt-topics-aws-iot-core.pdf#designing-mqtt-topics-aws-iot-core)

