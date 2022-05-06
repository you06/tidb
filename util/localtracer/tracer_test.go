package local_tracer

import (
	"context"
	"fmt"
	"testing"

	"github.com/opentracing/opentracing-go"
)

func f2(ctx context.Context) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("f2", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
}

func f1(ctx context.Context) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("f1", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	f2(ctx)
}

func TestTrace(t *testing.T) {
	tracer := NewLocalTracer()
	opentracing.SetGlobalTracer(tracer)
	span := opentracing.StartSpan("start")
	ctx := context.Background()
	ctx = opentracing.ContextWithSpan(ctx, span)
	f1(ctx)
	spans, _ := span.(*Span).traceHandle.Collect()
	fmt.Printf("%+v", spans)
}
