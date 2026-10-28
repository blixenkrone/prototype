package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	eventPackage       = "corti.events.v1"
	protobufSchemaType = "application/vnd.google.protobuf;version=3"
)

func main() {
	descriptorPath := flag.String("descriptor", "descriptor.bin", "Buf FileDescriptorSet path")
	cloudEventPath := flag.String("cloud-event-proto", "proto/third_party/cloudevents/v1/spec.proto", "CloudEvents spec.proto path")
	outputPath := flag.String("output", "docs/asyncapi.yaml", "output AsyncAPI document path")
	flag.Parse()

	descriptorData, err := os.ReadFile(*descriptorPath)
	if err != nil {
		fatal(err)
	}
	cloudEventProto, err := os.ReadFile(*cloudEventPath)
	if err != nil {
		fatal(err)
	}

	var descriptors descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(descriptorData, &descriptors); err != nil {
		fatal(fmt.Errorf("decode descriptor set: %w", err))
	}

	output, err := render(&descriptors, string(cloudEventProto))
	if err != nil {
		fatal(err)
	}
	if err := os.WriteFile(*outputPath, output, 0o644); err != nil {
		fatal(fmt.Errorf("write AsyncAPI document: %w", err))
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func render(descriptors *descriptorpb.FileDescriptorSet, cloudEventProto string) ([]byte, error) {
	if strings.TrimSpace(cloudEventProto) == "" {
		return nil, fmt.Errorf("CloudEvents spec.proto is empty")
	}

	events := make([]*descriptorpb.DescriptorProto, 0)
	for _, file := range descriptors.GetFile() {
		if file.GetPackage() != eventPackage {
			continue
		}
		events = append(events, file.GetMessageType()...)
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("descriptor set has no messages in package %s", eventPackage)
	}
	sort.Slice(events, func(i, j int) bool { return events[i].GetName() < events[j].GetName() })

	var output strings.Builder
	output.WriteString("asyncapi: 3.1.0\ninfo:\n  title: Corti events\n  version: '1.0.0'\nchannels:\n")
	for _, event := range events {
		fullName := eventPackage + "." + event.GetName()
		subject := strings.ToLower(fullName)
		channelID := strings.ReplaceAll(subject, ".", "_")
		payloadSchema, err := renderMessage(event)
		if err != nil {
			return nil, fmt.Errorf("render %s: %w", fullName, err)
		}

		fmt.Fprintf(&output, "  %s:\n    address: %s\n    messages:\n      %s:\n        name: %s\n        title: %s\n        contentType: application/protobuf\n        payload:\n          schemaFormat: %s\n          schema: |\n", channelID, subject, event.GetName(), fullName, event.GetName(), protobufSchemaType)
		writeIndented(&output, cloudEventProto, 12)
		fmt.Fprintf(&output, "        x-protobuf-data-type: %s\n        x-protobuf-data-schema:\n          schemaFormat: %s\n          schema: |\n", fullName, protobufSchemaType)
		writeIndented(&output, payloadSchema, 12)
	}

	output.WriteString("operations:\n")
	for _, event := range events {
		fullName := eventPackage + "." + event.GetName()
		channelID := strings.ReplaceAll(strings.ToLower(fullName), ".", "_")
		fmt.Fprintf(&output, "  publish_%s:\n    action: send\n    channel:\n      $ref: '#/channels/%s'\n", channelID, channelID)
	}

	return []byte(output.String()), nil
}

func renderMessage(message *descriptorpb.DescriptorProto) (string, error) {
	if len(message.GetNestedType()) != 0 || len(message.GetEnumType()) != 0 || len(message.GetOneofDecl()) != 0 {
		return "", fmt.Errorf("nested messages, enums, and oneofs are not supported in event payload schemas")
	}

	var output strings.Builder
	fmt.Fprintf(&output, "message %s {\n", message.GetName())
	for _, field := range message.GetField() {
		fieldType, ok := protobufType(field.GetType())
		if !ok {
			return "", fmt.Errorf("field %s has unsupported protobuf type %s", field.GetName(), field.GetType())
		}
		if field.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
			fieldType = "repeated " + fieldType
		} else if field.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL {
			return "", fmt.Errorf("field %s has unsupported label %s", field.GetName(), field.GetLabel())
		}
		fmt.Fprintf(&output, "  %s %s = %d;\n", fieldType, field.GetName(), field.GetNumber())
	}
	output.WriteString("}")
	return output.String(), nil
}

func protobufType(fieldType descriptorpb.FieldDescriptorProto_Type) (string, bool) {
	switch fieldType {
	case descriptorpb.FieldDescriptorProto_TYPE_DOUBLE:
		return "double", true
	case descriptorpb.FieldDescriptorProto_TYPE_FLOAT:
		return "float", true
	case descriptorpb.FieldDescriptorProto_TYPE_INT64:
		return "int64", true
	case descriptorpb.FieldDescriptorProto_TYPE_UINT64:
		return "uint64", true
	case descriptorpb.FieldDescriptorProto_TYPE_INT32:
		return "int32", true
	case descriptorpb.FieldDescriptorProto_TYPE_FIXED64:
		return "fixed64", true
	case descriptorpb.FieldDescriptorProto_TYPE_FIXED32:
		return "fixed32", true
	case descriptorpb.FieldDescriptorProto_TYPE_BOOL:
		return "bool", true
	case descriptorpb.FieldDescriptorProto_TYPE_STRING:
		return "string", true
	case descriptorpb.FieldDescriptorProto_TYPE_BYTES:
		return "bytes", true
	case descriptorpb.FieldDescriptorProto_TYPE_UINT32:
		return "uint32", true
	case descriptorpb.FieldDescriptorProto_TYPE_SFIXED32:
		return "sfixed32", true
	case descriptorpb.FieldDescriptorProto_TYPE_SFIXED64:
		return "sfixed64", true
	case descriptorpb.FieldDescriptorProto_TYPE_SINT32:
		return "sint32", true
	case descriptorpb.FieldDescriptorProto_TYPE_SINT64:
		return "sint64", true
	default:
		return "", false
	}
}

func writeIndented(output *strings.Builder, text string, spaces int) {
	indent := strings.Repeat(" ", spaces)
	for _, line := range strings.Split(strings.TrimSuffix(text, "\n"), "\n") {
		if line != "" {
			output.WriteString(indent)
			output.WriteString(line)
		}
		output.WriteByte('\n')
	}
}
