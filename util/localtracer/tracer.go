package local_tracer

import (
	"context"
	"sync/atomic"

	"github.com/opentracing/opentracing-go"
	"github.com/tikv/minitrace-go"
)

var (
	_ opentracing.Tracer = &LocalTracer{}
	//_ opentracing.TracerContextWithSpanExtension = &LocalTracer{}
)

type LocalTraceContextKey struct{}

var LocalTraceKey = LocalTraceContextKey{}

type LocalTracer struct {
	parent uint64
}

func NewLocalTracer() opentracing.Tracer {
	return &LocalTracer{parent: 0}
}

func (l *LocalTracer) StartSpan(operationName string, opts ...opentracing.StartSpanOption) opentracing.Span {
	if len(opts) == 0 {
		return l.startRootSpan()
	}
	sso := opentracing.StartSpanOptions{}
	for _, o := range opts {
		o.Apply(&sso)
	}
	if len(sso.References) == 0 {
		return l.startRootSpan()
	}
	return l.startSpanWithOptions(operationName, sso)
}

func (l *LocalTracer) startRootSpan() opentracing.Span {
	id := atomic.AddUint64(&l.parent, 1)
	ctx, root := minitrace.StartRootSpan(context.Background(), "root", id, 0, nil)
	span := newSpan(l, ctx)
	span.traceHandle = &root
	return span
}

func (l *LocalTracer) startSpanWithOptions(
	operationName string,
	options opentracing.StartSpanOptions,
) opentracing.Span {
	spanContext := options.References[len(options.References)-1].ReferencedContext.(*SpanContext)
	ctx, handle := minitrace.StartSpanWithContext(spanContext.span.ctx, operationName)
	span := newSpan(l, spanContext.span.ctx)
	span.ctx = ctx
	span.spanHandle = &handle
	return span
}

func (l *LocalTracer) Inject(sm opentracing.SpanContext, format interface{}, carrier interface{}) error {
	return nil
}

func (l *LocalTracer) Extract(format interface{}, carrier interface{}) (opentracing.SpanContext, error) {
	return nil, nil
}
