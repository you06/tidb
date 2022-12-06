package workers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type DummyIntWorker struct {
	receives []int
}

func (d *DummyIntWorker) HandleTask(_ context.Context, task int) error {
	d.receives = append(d.receives, task)
	return nil
}

func (d *DummyIntWorker) Close() {}

func NewDummyIntWorker() *DummyIntWorker {
	return &DummyIntWorker{
		receives: make([]int, 0),
	}
}

func TestPool(t *testing.T) {
	pool := New(context.Background(), 0, 1, NewDummyIntWorker, (*int)(nil))
	pool.Send(1)
	pool.Close(true)
	require.Equal(t, []int{1}, pool.workers[0].receives)

	pool = New(context.Background(), 0, 1, NewDummyIntWorker, (*int)(nil))
	pool.Send(1)
	pool.Send(2)
	pool.Send(3)
	pool.Close(true)
	require.Equal(t, []int{1, 2, 3}, pool.workers[0].receives)
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
