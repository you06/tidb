package local_tracer

import (
	"context"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/tikv/minitrace-go"
)

var (
	_    opentracing.Span        = &Span{}
	_    opentracing.SpanContext = &SpanContext{}
	pool sync.Pool
)

func init() {
	pool = sync.Pool{
		New: func() interface{} {
			return &Span{
				baggage: make(map[string]string),
			}
		},
	}
}

type SpanContext struct {
	span *Span
}

type Span struct {
	sync.Mutex
	operationName string
	baggage       map[string]string
	traceHandle   *minitrace.TraceHandle
	spanHandle    *minitrace.SpanHandle
	ctx           context.Context
	tracer        *LocalTracer
}

func newSpan(tracer *LocalTracer, ctx context.Context) *Span {
	span, _ := pool.Get().(*Span)
	span.ctx = ctx
	span.tracer = tracer
	return span
}

func (s *Span) Finish() {
	if s.spanHandle != nil {
		s.spanHandle.Finish()
		s.spanHandle = nil
	}
	if s.traceHandle != nil {
		s.traceHandle.Finish()
		s.traceHandle = nil
	}
	s.ctx = nil
	for k := range s.baggage {
		delete(s.baggage, k)
	}
	pool.Put(s)
}

func (s *Span) FinishWithOptions(_ opentracing.FinishOptions) {
	s.Finish()
}

func (s *Span) Context() opentracing.SpanContext {
	return &SpanContext{span: s}
}

func (s *Span) SetOperationName(operationName string) opentracing.Span {
	s.Lock()
	s.operationName = operationName
	s.Unlock()
	return s
}

func (s *Span) SetTag(key string, value interface{}) opentracing.Span {
	s.Lock()
	s.ctx = context.WithValue(s.ctx, key, value)
	s.Unlock()
	return s
}

func (s *Span) LogFields(fields ...log.Field) {

}

func (s *Span) LogKV(alternatingKeyValues ...interface{}) {

}

func (s *Span) SetBaggageItem(restrictedKey, value string) opentracing.Span {
	s.Lock()
	s.baggage[restrictedKey] = value
	s.Unlock()
	return s
}

// Gets the value for a baggage item given its key. Returns the empty string
// if the value isn't found in this Span.
func (s *Span) BaggageItem(restrictedKey string) string {
	s.Lock()
	defer s.Unlock()
	return s.baggage[restrictedKey]
}

// Provides access to the Tracer that created this Span.
func (s *Span) Tracer() opentracing.Tracer {
	return s.tracer
}

// Deprecated: use LogFields or LogKV
func (s *Span) LogEvent(event string) {}

// Deprecated: use LogFields or LogKV
func (s *Span) LogEventWithPayload(event string, payload interface{}) {}

// Deprecated: use LogFields or LogKV
func (s *Span) Log(data opentracing.LogData) {}

func (s *SpanContext) ForeachBaggageItem(handler func(k, v string) bool) {

}
