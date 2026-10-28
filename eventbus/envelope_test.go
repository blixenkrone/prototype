package eventbus_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/corticph/gocomo/eventbus/prototype/eventbus"
	ce "github.com/corticph/gocomo/eventbus/prototype/events/cloudevents/v1"
	bmkv1 "github.com/corticph/gocomo/eventbus/prototype/events/v1"
)

const (
	bookmarkAddedType = "corti.events.v1.BookmarkAdded"
	bookmarkID        = "b-1"
	encounterID       = "e-1"
)

func TestMarshalEventEnvelope(t *testing.T) {
	event := &bmkv1.BookmarkAdded{BookmarkId: bookmarkID, EncounterId: encounterID}

	data, cev, err := eventbus.MarshalEvent(event, "https://corti.example/bookmarks-svc")
	if err != nil {
		t.Fatalf("could not marshal event: %v", err)
	}

	if cev.GetId() == "" {
		t.Error("envelope id is empty")
	}

	if cev.GetSource() != "https://corti.example/bookmarks-svc" {
		t.Errorf("source = %q", cev.GetSource())
	}

	if cev.GetSpecVersion() != "1.0" {
		t.Errorf("specversion = %q", cev.GetSpecVersion())
	}

	if cev.GetType() != bookmarkAddedType {
		t.Errorf("type = %q", cev.GetType())
	}

	if cev.GetAttributes()["time"].GetCeTimestamp().AsTime().IsZero() {
		t.Error("time attribute missing")
	}

	if !strings.HasPrefix(cev.GetProtoData().GetTypeUrl(), "type.googleapis.com/corti.events.v1.BookmarkAdded") {
		t.Errorf("proto_data type_url = %q", cev.GetProtoData().GetTypeUrl())
	}

	if len(data) == 0 {
		t.Error("marshaled data is empty")
	}
}

func TestMarshalEventNilRejected(t *testing.T) {
	_, _, err := eventbus.MarshalEvent(nil, "source")
	if err == nil {
		t.Error("expected error for nil event")
	}
}

func TestMarshalEventTypedNilRejected(t *testing.T) {
	var event *bmkv1.BookmarkAdded

	_, _, err := eventbus.MarshalEvent(event, "source")
	if err == nil {
		t.Error("expected error for typed nil event")
	}
}

func TestUnmarshalEnvelope(t *testing.T) {
	event := &bmkv1.BookmarkAdded{BookmarkId: bookmarkID, EncounterId: encounterID}

	data, _, err := eventbus.MarshalEvent(event, "https://corti.example/bookmarks-svc")
	if err != nil {
		t.Fatalf("could not marshal event: %v", err)
	}

	cev, err := eventbus.UnmarshalEnvelope(data)
	if err != nil {
		t.Fatalf("could not unmarshal envelope: %v", err)
	}

	if cev.GetType() != bookmarkAddedType {
		t.Errorf("type = %q", cev.GetType())
	}
}

func TestUnmarshalEnvelopeWrongSpecVersion(t *testing.T) {
	cev := &ce.CloudEvent{
		Id:          "id-1",
		Source:      "source",
		SpecVersion: "0.3",
		Type:        bookmarkAddedType,
	}

	data, err := proto.Marshal(cev)
	if err != nil {
		t.Fatalf("could not marshal envelope: %v", err)
	}

	_, err = eventbus.UnmarshalEnvelope(data)
	if err == nil {
		t.Error("expected error for unexpected specversion")
	}
}

func TestUnmarshalEnvelopeMissingRequiredContext(t *testing.T) {
	cases := map[string]*ce.CloudEvent{
		"id": {
			Source:      "source",
			SpecVersion: "1.0",
			Type:        bookmarkAddedType,
		},
		"source": {
			Id:          "id-1",
			SpecVersion: "1.0",
			Type:        bookmarkAddedType,
		},
		"type": {
			Id:          "id-1",
			Source:      "source",
			SpecVersion: "1.0",
		},
	}

	for name, envelope := range cases {
		t.Run(name, func(t *testing.T) {
			data, err := proto.Marshal(envelope)
			if err != nil {
				t.Fatalf("could not marshal envelope: %v", err)
			}

			_, err = eventbus.UnmarshalEnvelope(data)
			if err == nil {
				t.Error("expected error for missing required context")
			}
		})
	}
}

func TestUnmarshalEnvelopeGarbage(t *testing.T) {
	_, err := eventbus.UnmarshalEnvelope([]byte("not-a-proto"))
	if err == nil {
		t.Error("expected error for garbage data")
	}
}

func TestEnvelopePayload(t *testing.T) {
	event := &bmkv1.BookmarkAdded{BookmarkId: bookmarkID, EncounterId: encounterID}

	_, cev, err := eventbus.MarshalEvent(event, "https://corti.example/bookmarks-svc")
	if err != nil {
		t.Fatalf("could not marshal event: %v", err)
	}

	payload, err := eventbus.EnvelopePayload(cev)
	if err != nil {
		t.Fatalf("could not extract payload: %v", err)
	}

	got, ok := payload.(*bmkv1.BookmarkAdded)
	if !ok {
		t.Fatalf("expected *bmkv1.BookmarkAdded, got %T", payload)
	}

	if got.GetBookmarkId() != bookmarkID || got.GetEncounterId() != encounterID {
		t.Errorf("payload = %+v", got)
	}
}

func TestEnvelopePayloadNotProto(t *testing.T) {
	cev := &ce.CloudEvent{
		Id:          "id-1",
		Source:      "source",
		SpecVersion: "1.0",
		Type:        bookmarkAddedType,
		Data:        &ce.CloudEvent_TextData{TextData: "text"},
	}

	_, err := eventbus.EnvelopePayload(cev)
	if err == nil {
		t.Error("expected error for text payload")
	}
}

func TestEnvelopeSubject(t *testing.T) {
	cev := &ce.CloudEvent{Type: bookmarkAddedType}
	if got := eventbus.EnvelopeSubject(cev); got != "corti.events.v1.bookmarkadded" {
		t.Errorf("subject = %q, want corti.events.v1.bookmarkadded", got)
	}
}
