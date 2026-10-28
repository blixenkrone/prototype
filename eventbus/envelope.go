package eventbus

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	ce "github.com/corticph/gocomo/eventbus/prototype/events/cloudevents/v1"
)

// TODO(gocomo): extract once a second consumer appears: the events packages
// and these envelope helpers are the prototype of a centralized events
// repository and should move to their own module or repo when a second
// service consumes them.

var (
	errNilEvent         = errors.New("event must not be nil")
	errNoProtoData      = errors.New("envelope has no proto data")
	errSpecVersion      = errors.New("unexpected CloudEvents specversion")
	errMissingContext   = errors.New("CloudEvent missing required context attribute")
	errPayloadUnmarshal = errors.New("could not unmarshal envelope payload")
)

// EnvelopeSubject derives the subject for a marshaled event: the lowercased
// CloudEvent type, which by convention is the fully qualified proto message
// name of the event, e.g. message BookmarkAdded in proto package bookmarks.v1
// becomes "bookmarks.v1.bookmarkadded".
func EnvelopeSubject(cev *ce.CloudEvent) string {
	return strings.ToLower(cev.GetType())
}

// MarshalEvent wraps event in a CloudEvents proto envelope (id, source,
// specversion, time and type set from the event's proto message name) and
// marshals the envelope. The result is the eventbus wire format.
func MarshalEvent(
	event proto.Message,
	source string,
) ([]byte, *ce.CloudEvent, error) {
	if event == nil || !event.ProtoReflect().IsValid() {
		return nil, nil, errNilEvent
	}

	payload, err := anypb.New(event)
	if err != nil {
		return nil, nil, fmt.Errorf("could not pack event as any: %w", err)
	}

	cev := &ce.CloudEvent{
		Id:          uuid.NewString(),
		Source:      source,
		SpecVersion: "1.0",
		Type:        string(event.ProtoReflect().Descriptor().FullName()),
		Attributes: map[string]*ce.CloudEvent_CloudEventAttributeValue{
			"time": {Attr: &ce.CloudEvent_CloudEventAttributeValue_CeTimestamp{
				CeTimestamp: timestamppb.New(time.Now().UTC()),
			}},
		},
		Data: &ce.CloudEvent_ProtoData{ProtoData: payload},
	}

	data, err := proto.Marshal(cev)
	if err != nil {
		return nil, nil, fmt.Errorf("could not marshal envelope: %w", err)
	}

	return data, cev, nil
}

// UnmarshalEnvelope unmarshals a CloudEvent envelope from its wire format.
func UnmarshalEnvelope(data []byte) (*ce.CloudEvent, error) {
	cev := &ce.CloudEvent{}

	err := proto.Unmarshal(data, cev)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal envelope: %w", err)
	}

	if cev.GetSpecVersion() != "1.0" {
		// An unexpected spec version means a producer with different
		// semantics: refuse to process rather than guess.
		return nil, fmt.Errorf("%w: %q", errSpecVersion, cev.GetSpecVersion())
	}

	for name, value := range map[string]string{
		"id":     cev.GetId(),
		"source": cev.GetSource(),
		"type":   cev.GetType(),
	} {
		if value == "" {
			return nil, fmt.Errorf("%w: %s", errMissingContext, name)
		}
	}

	return cev, nil
}

// EnvelopePayload extracts the event proto message from a CloudEvent envelope.
func EnvelopePayload(cev *ce.CloudEvent) (proto.Message, error) {
	payload := cev.GetProtoData()
	if payload == nil {
		// The event is not a proto event: text or binary payloads are
		// outside this eventbus' contract.
		return nil, errNoProtoData
	}

	event, err := payload.UnmarshalNew()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errPayloadUnmarshal, err)
	}

	return event, nil
}
