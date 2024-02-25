package kafka

import (
	"context"
	"errors"
	"os"

	streamdal "github.com/streamdal/streamdal/sdks/go"
)

const (
	StreamdalEnvAddress     = "STREAMDAL_ADDRESS"
	StreamdalEnvAuthToken   = "STREAMDAL_AUTH_TOKEN"
	StreamdalEnvServiceName = "STREAMDAL_SERVICE_NAME"

	StreamdalDefaultComponentName = "kafka"
	StreamdalDefaultOperationName = "unknown"
	StreamdalContextValueKey      = "streamdal-runtime-config"
)

// StreamdalRuntimeConfig is an optional configuration structure that can be
// passed to kafka.FetchMessage() and kafka.WriteMessage() methods to influence
// streamdal shim behavior.
//
// NOTE: This struct is intended to be passed as a value in a context.Context.
// This is done this way to avoid having to change FetchMessage() and WriteMessages()
// signatures.
type StreamdalRuntimeConfig struct {
	// StrictErrors will cause the shim to return a kafka.Error if Streamdal.Process()
	// runs into an unrecoverable error. Default: swallow error and return original value.
	StrictErrors bool

	// Audience is used to specify a custom audience when the shim calls on
	// streamdal.Process(); if nil, a default ComponentName and OperationName
	// will be used. Only non-blank values will be used to override audience defaults.
	Audience *streamdal.Audience
}

func streamdalSetup() (*streamdal.Streamdal, error) {
	address := os.Getenv(StreamdalEnvAddress)
	if address == "" {
		return nil, errors.New(StreamdalEnvAddress + " env var is not set")
	}

	authToken := os.Getenv(StreamdalEnvAuthToken)
	if authToken == "" {
		return nil, errors.New(StreamdalEnvAuthToken + " env var is not set")
	}

	serviceName := os.Getenv(StreamdalEnvServiceName)
	if serviceName == "" {
		return nil, errors.New(StreamdalEnvServiceName + " env var is not set")
	}

	sc, err := streamdal.New(&streamdal.Config{
		ServerURL:   address,
		ServerToken: authToken,
		ServiceName: serviceName,
		ClientType:  streamdal.ClientTypeShim,
	})

	if err != nil {
		return nil, errors.New("unable to create streamdal client: " + err.Error())
	}

	return sc, nil
}

func streamdalProcess(ctx context.Context, sc *streamdal.Streamdal, ot streamdal.OperationType, msg *Message, loggers ...Logger) (*Message, error) {
	// Nothing to do if streamdal client is nil
	if sc == nil {
		return msg, nil
	}

	// Maybe extract runtime config from context
	var src *StreamdalRuntimeConfig
	if ctx != nil {
		src, _ = ctx.Value(StreamdalContextValueKey).(*StreamdalRuntimeConfig)
	}

	// Generate an audience from the provided parameters
	aud := streamdalGenerateAudience(ot, msg.Topic, src)

	// Process msg payload via Streamdal
	resp := sc.Process(ctx, &streamdal.ProcessRequest{
		ComponentName: aud.ComponentName,
		OperationType: ot,
		OperationName: aud.OperationName,
		Data:          msg.Value,
	})

	switch resp.Status {
	case streamdal.ExecStatusTrue, streamdal.ExecStatusFalse:
		// Process() did not error - replace kafka.Value
		msg.Value = resp.Data
	case streamdal.ExecStatusError:
		// Process() errored - return message as-is; if strict errors are NOT
		// set, return error instead of message
		streamdalLogError(loggers, "streamdal.Process() error: "+ptrStr(resp.StatusMessage))

		if src != nil && src.StrictErrors {
			return nil, errors.New("streamdal.Process() error: " + ptrStr(resp.StatusMessage))
		}
	}

	return msg, nil
}

// Helper func for generating an "audience" that can be passed to streamdal's .Process() method.
//
// Topic is only used if the provided runtime config is nil or the underlying
// audience does not have an OperationName set.
func streamdalGenerateAudience(ot streamdal.OperationType, topic string, src *StreamdalRuntimeConfig) *streamdal.Audience {
	var (
		componentName = StreamdalDefaultComponentName
		operationName = StreamdalDefaultOperationName
	)

	if topic != "" {
		operationName = topic
	}

	if src != nil && src.Audience != nil {
		if src.Audience.OperationName != "" {
			operationName = src.Audience.OperationName
		}

		if src.Audience.ComponentName != "" {
			componentName = src.Audience.ComponentName
		}
	}

	return &streamdal.Audience{
		OperationType: ot,
		OperationName: operationName,
		ComponentName: componentName,
	}
}

// Helper func for logging errors encountered during streamdal.Process()
func streamdalLogError(loggers []Logger, msg string) {
	for _, l := range loggers {
		if l == nil {
			continue
		}

		l.Printf(msg)
	}
}

// Helper func to deref string ptrs
func ptrStr(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}
