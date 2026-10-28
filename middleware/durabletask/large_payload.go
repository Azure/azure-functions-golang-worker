package durabletask

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/microsoft/durabletask-go/api"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// WithLargePayloads enables transparent payload offloading for Functions
// execution and all management clients supplied by this middleware. Use the
// same options with WithClientLargePayloads for independently created clients.
// Options are validated and copied during construction. Nil disables offloading.
// No storage settings are inferred from AzureWebJobsStorage.
func WithLargePayloads(options *api.LargePayloadOptions) Option {
	return func(d *Durable) {
		d.largePayloads = mustNormalizeLargePayloads(options)
		d.payloadsConfigured = true
	}
}

func mustNormalizeLargePayloads(options *api.LargePayloadOptions) *api.LargePayloadOptions {
	normalized, err := api.NormalizeLargePayloadOptions(options)
	if err != nil {
		panic(fmt.Sprintf("durabletask: configure large payloads: %v", err))
	}
	return normalized
}

type payloadReference struct {
	Location string `json:"location"`
	Size     int    `json:"size"`
	SHA256   string `json:"sha256"`
}

// Functions dispatch bypasses upstream's polling worker. Its scalar codec and
// protobuf transformations are internal, so this adapter uses the public
// store/token contracts at the Functions transport boundary. Blob validation,
// credentials, compression and integrity checks remain owned by the store.
func transformPayload(ctx context.Context, options *api.LargePayloadOptions, value string, externalize bool) (string, error) {
	var store api.LargePayloadTokenStore
	if options != nil {
		store = options.TokenStore
	}
	if !externalize {
		var err error
		value, err = unwrapPayloadReference(value, store)
		if err != nil {
			return "", err
		}
	}
	if options == nil {
		if api.IsLargePayloadReference(value) {
			return "", fmt.Errorf("%w: payload reference requires configured large payloads", api.ErrFeatureNotSupported)
		}
		return value, nil
	}
	if externalize {
		if err := rejectPayloadEnvelope(value); err != nil {
			return "", err
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if store != nil && store.IsLargePayloadToken(value) {
		if validator, ok := store.(api.LargePayloadTokenValidator); ok {
			if err := validator.ValidateLargePayloadToken(value); err != nil {
				return "", err
			}
		}
		if externalize {
			return value, nil
		}
		raw, err := store.ResolveToken(ctx, value)
		if err != nil {
			return "", fmt.Errorf("resolve large payload token: %w", err)
		}
		if len(raw) > options.MaxPayloadBytes {
			return "", api.ErrLargePayloadTooLarge
		}
		return string(raw), nil
	}
	if strings.HasPrefix(value, api.AzureBlobPayloadReferencePrefixV1) ||
		strings.HasPrefix(value, api.AzureBlobPayloadReferencePrefixV2) {
		return "", fmt.Errorf("%w: Azure Blob token requires a compatible token store", api.ErrFeatureNotSupported)
	}
	if strings.HasPrefix(value, api.DurableTaskPayloadReferencePrefix) {
		descriptor, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, api.DurableTaskPayloadReferencePrefix))
		if err != nil {
			return "", fmt.Errorf("%w: invalid base64 descriptor", api.ErrLargePayloadReference)
		}
		var ref payloadReference
		if err := json.Unmarshal(descriptor, &ref); err != nil {
			return "", fmt.Errorf("%w: invalid JSON descriptor", api.ErrLargePayloadReference)
		}
		expected, err := hex.DecodeString(ref.SHA256)
		if strings.TrimSpace(ref.Location) == "" || ref.Size < 0 || ref.Size > options.MaxPayloadBytes ||
			err != nil || len(expected) != sha256.Size {
			return "", fmt.Errorf("%w: invalid location, size or digest", api.ErrLargePayloadReference)
		}
		if externalize {
			return value, nil
		}
		raw, err := options.Resolver.Resolve(ctx, ref.Location)
		if err != nil {
			return "", fmt.Errorf("resolve large payload: %w", err)
		}
		actual := sha256.Sum256(raw)
		if len(raw) != ref.Size || subtle.ConstantTimeCompare(actual[:], expected) != 1 {
			return "", api.ErrLargePayloadIntegrity
		}
		return string(raw), nil
	}
	if !externalize {
		return value, nil
	}
	if len(value) > options.MaxPayloadBytes {
		return "", api.ErrLargePayloadTooLarge
	}
	inclusive, ok := options.Store.(api.InclusiveLargePayloadThreshold)
	if len(value) < options.ThresholdBytes || (len(value) == options.ThresholdBytes &&
		(!ok || !inclusive.UsesInclusiveLargePayloadThreshold())) {
		return value, nil
	}
	if store != nil {
		token, err := store.StoreToken(ctx, []byte(value))
		if err != nil {
			return "", fmt.Errorf("store large payload token: %w", err)
		}
		if strings.TrimSpace(token) == "" {
			return "", fmt.Errorf("large payload store returned an empty token")
		}
		return token, nil
	}
	location, err := options.Store.Store(ctx, []byte(value))
	if err != nil {
		return "", fmt.Errorf("store large payload: %w", err)
	}
	if strings.TrimSpace(location) == "" {
		return "", fmt.Errorf("large payload store returned an empty location")
	}
	digest := sha256.Sum256([]byte(value))
	descriptor, err := json.Marshal(payloadReference{location, len(value), hex.EncodeToString(digest[:])})
	if err != nil {
		return "", fmt.Errorf("encode large payload reference: %w", err)
	}
	return api.DurableTaskPayloadReferencePrefix + base64.RawURLEncoding.EncodeToString(descriptor), nil
}

// Only application payload wrappers are transformed. Identity, names, failure
// messages, entity properties and arbitrary application JSON are not traversed.
// Keys are upstream message names; fields mirror its transport codec.
var payloadFields = map[protoreflect.Name][]protoreflect.Name{
	"ExecutionStartedEvent":                  {"input"},
	"ExecutionCompletedEvent":                {"result"},
	"ExecutionTerminatedEvent":               {"input"},
	"TaskScheduledEvent":                     {"input"},
	"TaskCompletedEvent":                     {"result"},
	"SubOrchestrationInstanceCreatedEvent":   {"input"},
	"SubOrchestrationInstanceCompletedEvent": {"result"},
	"EventSentEvent":                         {"input"},
	"EventRaisedEvent":                       {"input"},
	"GenericEvent":                           {"data"},
	"ContinueAsNewEvent":                     {"input"},
	"ExecutionSuspendedEvent":                {"input"},
	"ExecutionResumedEvent":                  {"input"},
	"ExecutionRewoundEvent":                  {"input"},
	"EntityOperationSignaledEvent":           {"input"},
	"EntityOperationCalledEvent":             {"input"},
	"EntityOperationCompletedEvent":          {"output"},
	"OrchestrationState":                     {"input", "output", "customStatus"},
	"OrchestratorResponse":                   {"customStatus"},
	"ScheduleTaskAction":                     {"input"},
	"CreateSubOrchestrationAction":           {"input"},
	"SendEventAction":                        {"data"},
	"CompleteOrchestrationAction":            {"result"},
	"TerminateOrchestrationAction":           {"reason"},
	"EntityBatchRequest":                     {"entityState"},
	"OperationRequest":                       {"input"},
	"EntityBatchResult":                      {"entityState"},
	"OperationResultSuccess":                 {"result"},
	"SendSignalAction":                       {"input"},
	"StartNewOrchestrationAction":            {"input"},
	"CreateInstanceRequest":                  {"input"},
	"RaiseEventRequest":                      {"input"},
	"TerminateRequest":                       {"output"},
	"SignalEntityRequest":                    {"input"},
	"EntityMetadata":                         {"serializedState"},
}

func transformPayloadMessage(ctx context.Context, options *api.LargePayloadOptions, message protoreflect.Message, externalize bool) error {
	return visitPayloadFields(message, func(value string) (string, error) {
		transformed, err := transformPayload(ctx, options, value, externalize)
		if err != nil || !externalize {
			return transformed, err
		}
		var store api.LargePayloadTokenStore
		if options != nil {
			store = options.TokenStore
		}
		return wrapPayloadReference(transformed, store)
	})
}

func visitPayloadFields(message protoreflect.Message, transform func(string) (string, error)) error {
	for _, name := range payloadFields[message.Descriptor().Name()] {
		field := message.Descriptor().Fields().ByName(name)
		if field == nil || field.Message() == nil || field.Message().FullName() != "google.protobuf.StringValue" {
			return fmt.Errorf("durabletask: upstream payload field %s.%s is not a string wrapper", message.Descriptor().FullName(), name)
		}
		if !message.Has(field) {
			continue
		}
		wrapper := message.Get(field).Message()
		valueField := wrapper.Descriptor().Fields().ByName("value")
		value, err := transform(wrapper.Get(valueField).String())
		if err != nil {
			return fmt.Errorf("durabletask: transform %s.%s: %w", message.Descriptor().Name(), name, err)
		}
		wrapper.Set(valueField, protoreflect.ValueOfString(value))
	}
	var transformErr error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Kind() != protoreflect.MessageKind || field.IsMap() ||
			field.Message().FullName() == "google.protobuf.StringValue" {
			return true
		}
		if field.IsList() {
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				if transformErr = visitPayloadFields(list.Get(i).Message(), transform); transformErr != nil {
					return false
				}
			}
		} else {
			transformErr = visitPayloadFields(value.Message(), transform)
		}
		return transformErr == nil
	})
	return transformErr
}
