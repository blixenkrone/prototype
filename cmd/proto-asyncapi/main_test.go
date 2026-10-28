package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	eventsv1 "github.com/corticph/gocomo/eventbus/prototype/events/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestRenderProducesDeterministicAsyncAPI(t *testing.T) {
	descriptors, err := proto.Marshal(&descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(eventsv1.File_events_v1_events_proto),
			protodesc.ToFileDescriptorProto(eventsv1.File_events_v1_events_1_proto),
		},
	})
	if err != nil {
		t.Fatalf("could not marshal event descriptor: %v", err)
	}
	var set descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(descriptors, &set); err != nil {
		t.Fatalf("could not unmarshal event descriptor: %v", err)
	}

	cloudEventProto, err := os.ReadFile("../../events/proto/third_party/cloudevents/v1/spec.proto")
	if err != nil {
		t.Fatalf("could not read vendored CloudEvents proto: %v", err)
	}

	first, err := render(&set, string(cloudEventProto))
	if err != nil {
		t.Fatalf("could not render AsyncAPI: %v", err)
	}
	second, err := render(&set, string(cloudEventProto))
	if err != nil {
		t.Fatalf("could not render AsyncAPI a second time: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("AsyncAPI generation is not deterministic")
	}

	checkedIn, err := os.ReadFile("../../docs/asyncapi.yaml")
	if err != nil {
		t.Fatalf("could not read generated AsyncAPI document: %v", err)
	}
	if !bytes.Equal(first, checkedIn) {
		t.Fatal("generated AsyncAPI differs from docs/asyncapi.yaml")
	}

	for _, want := range []string{
		"asyncapi: 3.1.0",
		"address: corti.events.v1.bookmarkadded",
		"schemaFormat: application/vnd.google.protobuf;version=3",
		"x-protobuf-data-type: corti.events.v1.BookmarkAdded",
		"string bookmark_id = 1;",
		"string encounter_id = 2;",
	} {
		if !strings.Contains(string(first), want) {
			t.Errorf("generated document missing %q", want)
		}
	}
}
