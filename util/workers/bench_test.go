package workers

import (
	"context"
	"testing"
)

const (
	CONC  = 8
	COUNT = 2
)

type DummyProjWorker struct {
	factor   int
	resultCh chan int
}

func (d *DummyProjWorker) HandleTask(_ context.Context, task int) error {
	task += 1
	for i := 0; i < 1000; i++ {
		task = (task * 10) % 7
	}
	d.resultCh <- task
	return nil
}

func (d *DummyProjWorker) Close() {}

func BenchmarkBaseline(b *testing.B) {
	for n := 0; n < b.N; n++ {
		ctx := context.Background()
		taskCh := make(chan int, CONC)
		resultCh := make(chan int, CONC)
		closeCh := make(chan struct{})
		newDummyProjWorker := func() *DummyProjWorker {
			return &DummyProjWorker{
				factor:   CONC,
				resultCh: resultCh,
			}
		}
		for i := 0; i < CONC; i++ {
			go func() {
				worker := newDummyProjWorker()
				for {
					select {
					case t := <-taskCh:
						worker.HandleTask(ctx, t)
					case <-closeCh:
						return
					}
				}
			}()
		}
		go func() {
			for i := 0; i < COUNT; i++ {
				taskCh <- i
			}
		}()
		for i := 0; i < COUNT; i++ {
			<-resultCh
		}
		close(closeCh)
		close(taskCh)
	}
}

func BenchmarkProjection(b *testing.B) {
	for n := 0; n < b.N; n++ {
		ctx := context.Background()
		resultCh := make(chan int, CONC)
		newDummyProjWorker := func() *DummyProjWorker {
			return &DummyProjWorker{
				factor:   CONC,
				resultCh: resultCh,
			}
		}
		pool := New(ctx, 1, CONC, newDummyProjWorker, (*int)(nil))
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
}
