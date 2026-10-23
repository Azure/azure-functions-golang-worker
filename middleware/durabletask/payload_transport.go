package durabletask

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/microsoft/durabletask-go/api"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Functions parses activity inputs as JSON before dispatch. Its Azure Storage
// backend also treats any raw absolute URI as a backend-owned message reference.
// A reserved JSON object transports the upstream token without either ambiguity.
// A plain JSON string cannot do this without colliding with application strings.
const payloadEnvelopeKey = "$durabletaskPayload"

type functionsPayloadReference struct {
	Version   int    `json:"version"`
	Reference string `json:"reference"`
}

func hasPayloadEnvelope(value string) bool {
	var object map[string]json.RawMessage
	if json.Unmarshal([]byte(value), &object) != nil {
		return false
	}
	_, present := object[payloadEnvelopeKey]
	return present
}

func rejectPayloadEnvelope(value string) error {
	if hasPayloadEnvelope(value) {
		return fmt.Errorf("%w: %s is reserved for Functions payload references", api.ErrLargePayloadReference, payloadEnvelopeKey)
	}
	return nil
}

func wrapPayloadReference(value string) (string, error) {
	if !api.IsLargePayloadReference(value) {
		return value, nil
	}
	raw, err := json.Marshal(map[string]functionsPayloadReference{payloadEnvelopeKey: {Version: 1, Reference: value}})
	if err != nil {
		return "", fmt.Errorf("durabletask: encode Functions payload reference: %w", err)
	}
	return string(raw), nil
}

func unwrapPayloadReference(value string) (string, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(value), &object); err != nil || object == nil {
		return value, nil
	}
	raw, present := object[payloadEnvelopeKey]
	if !present {
		return value, nil
	}
	var reference functionsPayloadReference
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if len(object) != 1 || decoder.Decode(&reference) != nil || reference.Version != 1 ||
		!api.IsLargePayloadReference(reference.Reference) {
		return "", fmt.Errorf("%w: malformed Functions payload envelope", api.ErrLargePayloadReference)
	}
	return reference.Reference, nil
}

type functionsPayloadConverter struct{ api.DataConverter }

func (c functionsPayloadConverter) Serialize(value any) (string, error) {
	raw, err := c.DataConverter.Serialize(value)
	if err != nil {
		return "", err
	}
	if err := rejectPayloadEnvelope(raw); err != nil {
		return "", err
	}
	return raw, nil
}

// payloadConnection adapts only protobuf application-payload fields. Upstream
// management methods retain ownership of serialization, offloading, hydration
// and their errors. The borrowed underlying connection is never closed here.
type functionsPayloadConnection struct{ grpc.ClientConnInterface }

func (c functionsPayloadConnection) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	request, ok := args.(proto.Message)
	if !ok {
		return fmt.Errorf("durabletask: management request %T is not protobuf", args)
	}
	request = proto.Clone(request)
	if err := transformPayloadWireMessage(request.ProtoReflect(), true); err != nil {
		return err
	}
	if err := c.ClientConnInterface.Invoke(ctx, method, request, reply, opts...); err != nil {
		return err
	}
	response, ok := reply.(proto.Message)
	if !ok {
		return fmt.Errorf("durabletask: management response %T is not protobuf", reply)
	}
	return transformPayloadWireMessage(response.ProtoReflect(), false)
}

func (c functionsPayloadConnection) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	stream, err := c.ClientConnInterface.NewStream(ctx, desc, method, opts...)
	if err != nil {
		return nil, err
	}
	return functionsPayloadStream{ClientStream: stream}, nil
}

type functionsPayloadStream struct{ grpc.ClientStream }

func (s functionsPayloadStream) SendMsg(message any) error {
	request, ok := message.(proto.Message)
	if !ok {
		return fmt.Errorf("durabletask: streamed management request %T is not protobuf", message)
	}
	request = proto.Clone(request)
	if err := transformPayloadWireMessage(request.ProtoReflect(), true); err != nil {
		return err
	}
	return s.ClientStream.SendMsg(request)
}

func (s functionsPayloadStream) RecvMsg(message any) error {
	if err := s.ClientStream.RecvMsg(message); err != nil {
		return err
	}
	response, ok := message.(proto.Message)
	if !ok {
		return fmt.Errorf("durabletask: streamed management response %T is not protobuf", message)
	}
	return transformPayloadWireMessage(response.ProtoReflect(), false)
}

func transformPayloadWireMessage(message protoreflect.Message, outgoing bool) error {
	transform := unwrapPayloadReference
	if outgoing {
		transform = wrapPayloadReference
	}
	return visitPayloadFields(message, transform)
}
