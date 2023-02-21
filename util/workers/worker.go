package workers

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
)

const (
	MAX_WORKER = math.MaxUint16
)

type Worker[task any] interface {
	HandleTask(context.Context, task) error
	Close()
}

type NewWorker[task any, w Worker[task]] func() w

type WorkerManager[task any, w Worker[task]] struct {
	max       int32
	used      atomic.Int32
	count     atomic.Int32
	closed    atomic.Bool
	flying    atomic.Int64
	ctx       context.Context
	cancel    context.CancelFunc
	newWorker NewWorker[task, w]
	workers   []w
	ch        chan task
	closeCh   chan struct{}
	//waitCloseCh chan struct{}
	wg sync.WaitGroup
}

func New[task any, w Worker[task]](ctx context.Context, least, max int, newWorker NewWorker[task, w], _phantom *task) *WorkerManager[task, w] {
	if max > MAX_WORKER {
		max = MAX_WORKER
	}
	if least > max {
		least = max
	}
	ctx1, cancel := context.WithCancel(ctx)
	worker := &WorkerManager[task, w]{
		max:       int32(max),
		ctx:       ctx1,
		cancel:    cancel,
		newWorker: newWorker,
		workers:   make([]w, max),
		ch:        make(chan task, max),
		closeCh:   make(chan struct{}),
		wg:        sync.WaitGroup{},
	}
	if least > 0 {
		worker.count.Add(int32(least))
		for i := 0; i < least; i++ {
			worker.createWorker(i)
		}
	}
	return worker
}

// Size returns the count of worker.
func (p *WorkerManager[task, w]) Size() int {
	return int(p.count.Load())
}

// Send sends task to worker and return whether the job is closed.
func (p *WorkerManager[task, w]) Send(t task) bool {
	p.flying.Add(1)
	if p.closed.Load() {
		p.flying.Add(-1)
		return true
	}
	count := p.count.Load()
	if count < p.max && p.used.Load()+int32(len(p.ch)) >= count {
		// may create new worker when the free worker is not enough
		for {
			if p.count.CompareAndSwap(count, count+1) {
				break
			}
			count = p.count.Load()
			if count >= p.max {
				break
			}
		}
		if p.closed.Load() {
			p.flying.Add(-1)
			return true
		}
		if count < p.max {
			p.createWorker(int(count))
		}
	} else {
		if p.closed.Load() {
			p.flying.Add(-1)
			return true
		}
	}
	var res bool
	select {
	case <-p.closeCh:
		res = true
	case p.ch <- t:
	}
	p.flying.Add(-1)
	return res
}

func (p *WorkerManager[task, w]) createWorker(idx int) {
	p.wg.Add(1)
	p.workers[idx] = p.newWorker()
	go func() {
	LOOP:
		for {
			select {
			case <-p.closeCh:
				// nowait break out
				break LOOP
			case t, ok := <-p.ch:
				// wait break out
				if !ok {
					break LOOP
				}
				p.used.Add(1)
				p.workers[idx].HandleTask(p.ctx, t)
				p.used.Add(-1)
			}
		}
		p.wg.Done()
		p.workers[idx].Close()
	}()
}

// Close the manager, this will stop handling flying and quit all workers.
func (p *WorkerManager[task, w]) Close(wait bool) {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	if wait {
		// wait for all tasks enqueue.
		for p.flying.Load() > 0 {
		}
		// close p.ch after it's not accessible.
		close(p.ch)
		p.wg.Wait()
		close(p.closeCh)
		p.cancel()
	} else {
		// quick stop tasks
		close(p.closeCh)
		p.cancel()
		for p.flying.Load() > 0 {
		}
		// close p.ch after it's not accessible.
		close(p.ch)
	}
}
