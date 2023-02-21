package workers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type DummyIntWorker struct {
	receive chan int
}

func (d *DummyIntWorker) HandleTask(_ context.Context, task int) error {
	d.receive <- task
	return nil
}

func (d *DummyIntWorker) Close() {}

func NewDummyIntWorker(receive chan int) *DummyIntWorker {
	return &DummyIntWorker{
		receive: receive,
	}
}

func TestPoolOneshot(t *testing.T) {
	receive := make(chan int)
	pool := New(context.Background(), 0, 1, func() *DummyIntWorker {
		return NewDummyIntWorker(receive)
	}, (*int)(nil))
	pool.Send(1)
	require.Equal(t, 1, <-receive)
	pool.Close(true)
}

func TestPoolMultiTask(t *testing.T) {
	receive := make(chan int, 200)
	pool := New(context.Background(), 0, 10, func() *DummyIntWorker {
		return NewDummyIntWorker(receive)
	}, (*int)(nil))
	results := make(map[int]struct{}, 100)
	for i := 0; i < 100; i++ {
		results[i] = struct{}{}
		pool.Send(i)
	}
	pool.Close(true)
	close(receive)
	for i := 0; i < 100; i++ {
		r := <-receive
		_, ok := results[r]
		require.True(t, ok)
		delete(results, r)
	}
}

func TestBenchProjection(t *testing.T) {
	resultCh := make(chan int, CONC)
	newDummyProjWorker := func() *DummyProjWorker {
		return &DummyProjWorker{
			factor:   CONC,
			resultCh: resultCh,
		}
	}
	pool := New(context.Background(), 0, CONC, newDummyProjWorker, (*int)(nil))
	go func() {
		for i := 0; i < COUNT; i++ {
			pool.Send(i)
		}
	}()
	for i := 0; i < COUNT; i++ {
		<-resultCh
	}
	pool.Close(false)
}
