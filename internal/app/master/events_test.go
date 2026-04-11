package master

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"

	"djs/internal/config"
	"djs/internal/domain"
)

func TestBuildLifecycleEventIncludesTraceAndHeaders(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	service := NewService(&config.Config{
		App: config.AppConfig{ID: "master-1"},
		Messaging: config.MessagingConfig{TopicLifecycle: "djs.lifecycle.v1"},
	}, nil, nil, nil, nil, nil, nil, nil, nil, nil)

	traceID := oteltrace.TraceID{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	spanID := oteltrace.SpanID{2, 2, 2, 2, 2, 2, 2, 2}
	ctx := oteltrace.ContextWithSpanContext(context.Background(), oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: oteltrace.FlagsSampled,
	}))

	jobID := uint64(11)
	instanceID := uint64(22)
	attemptNo := uint32(3)
	event, headers, err := service.buildLifecycleEvent(
		ctx,
		domain.EventTypeTaskDispatched,
		domain.AggregateTypeAttempt,
		"22/3",
		"instance:22",
		&jobID,
		&instanceID,
		&attemptNo,
		"worker-a",
		map[string]any{"status": domain.AttemptStatusDispatched},
	)
	if err != nil {
		t.Fatalf("buildLifecycleEvent failed: %v", err)
	}

	if event.TraceID != traceID.String() {
		t.Fatalf("expected trace id %s, got %s", traceID.String(), event.TraceID)
	}
	if event.Topic != "djs.lifecycle.v1" {
		t.Fatalf("unexpected topic: %s", event.Topic)
	}
	if headers["traceparent"] == "" {
		t.Fatalf("expected traceparent header to be injected")
	}
}
