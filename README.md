# mqtt-framework-poc

This is a PoC project to do hands-on learn on IoT programming.  Currently with a focus on the MQTT protocol.

> [!NOTE]
> This project is a PoC based on my ongoing learning of MQTT.  Any assertive statements based about MQTT are done so
> for convenience, and are based on limited experience.  This is to avoid saying _"MQTT seems to be ..."_ and instead
> say _"MQTT is ..."_.
> 
> The APIs explored here are not based on years of experience and might be misguided.  Feedback is welcome.

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

### Router API

The [router](./app/router/doc.go) package contains a evolving PoC of the ideas above with APIs for publishing and subscribing to messages in a type-safe manner using Go structs to define the topic+message schemas.


## Work log

### 2026-08-21

Open questions:

* How does one structure high-level communication patterns using MQTT?
* How does one do request/response with a given device?
* How does a device report current state?
* How does a server report state to a device?
* How can topic names be used to manage communication patterns?

[Designing MQTT Topics for AWS IoT Core - AWS Whitepaper](https://docs.aws.amazon.com/pdfs/whitepapers/latest/designing-mqtt-topics-aws-iot-core/designing-mqtt-topics-aws-iot-core.pdf#designing-mqtt-topics-aws-iot-core)

