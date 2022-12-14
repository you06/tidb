package workers

import (
	"context"
	"sync"
	"sync/atomic"
)

type Worker[task any] interface {
	HandleTask(context.Context, task) error
	Close()
}

type NewWorker[task any, w Worker[task]] func() w

type WorkerManager[task any, w Worker[task]] struct {
	max       int32
	used      int32
	count     int32
	exit      int32
	wait      int32
	closed    int32
	cleaned   int32
	ctx       context.Context
	cancel    context.CancelFunc
	newWorker NewWorker[task, w]
	workers   []w
	ch        chan task
	closeCh   chan struct{}
	//waitCloseCh chan struct{}
	wg sync.WaitGroup
}

func New[task any, w Worker[task]](ctx context.Context, least int, max int32, newWorker NewWorker[task, w], _phantom *task) *WorkerManager[task, w] {
	ctx1, cancel := context.WithCancel(ctx)
	worker := &WorkerManager[task, w]{
		max:       max,
		used:      0,
		count:     0,
		wait:      0,
		exit:      0,
		closed:    0,
		cleaned:   0,
		ctx:       ctx1,
		cancel:    cancel,
		newWorker: newWorker,
		workers:   make([]w, max),
		ch:        make(chan task, max),
		closeCh:   make(chan struct{}),
		//waitCloseCh: make(chan struct{}, max),
		wg: sync.WaitGroup{},
	}
	for i := 0; i < least; i++ {
		worker.count++
		worker.createWorker(i)
	}
	return worker
}

// Size returns the count of worker.
func (p *WorkerManager[task, w]) Size() int {
	return int(atomic.LoadInt32(&p.count))
}

// Send sends task to worker and return whether the job is closed.
func (p *WorkerManager[task, w]) Send(t task) bool {
	if atomic.LoadInt32(&p.closed) == 1 {
		return true
	}
	if atomic.LoadInt32(&p.used)+int32(len(p.ch)) >= atomic.LoadInt32(&p.count) {
		// may create new worker when the free worker is not enough
		var idx int32
		for {
			idx = atomic.LoadInt32(&p.count)
			if idx >= p.max {
				break
			}
			if atomic.CompareAndSwapInt32(&p.count, idx, idx+1) {
				break
			}
		}
		if idx < p.max {
			p.createWorker(int(idx))
		}
	}
	select {
	case <-p.closeCh:
		return true
	case p.ch <- t:
		return false
	}
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
				atomic.AddInt32(&p.used, 1)
				p.workers[idx].HandleTask(p.ctx, t)
				atomic.AddInt32(&p.used, -1)
			}
		}
		p.wg.Done()
		// reduce the count
		exit := atomic.AddInt32(&p.exit, 1)
		//if atomic.LoadInt32(&p.wait) == 1 {
		//	if atomic.CompareAndSwapInt32(&p.cleaned, 0, 1) {
		//		close(p.waitCloseCh)
		//		for range p.waitCloseCh {
		//		}
		//	}
		//} else {
		// cnt == 1 means the last work thread
		if exit == atomic.LoadInt32(&p.count) {
			if atomic.CompareAndSwapInt32(&p.cleaned, 0, 1) {
				//close(p.waitCloseCh)
				// drain the channel, release memory.
				for range p.ch {
				}
			}
		}
		//}
		p.workers[idx].Close()
	}()
}

// Close the manager, this will stop handling tasks and quit all workers.
func (p *WorkerManager[task, w]) Close(wait bool) {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return
	}
	if wait {
		atomic.StoreInt32(&p.wait, 1)
	}
	p.cancel()
	close(p.closeCh)
	close(p.ch)
	if wait {
		p.wg.Wait()
		//for {
		//	exit := atomic.LoadInt32(&p.exit)
		//	count := atomic.LoadInt32(&p.count)
		//	if exit >= count {
		//		break
		//	}
		//	//<-p.waitCloseCh
		//}
	}
	// if wait is set to false, p.ch will be closed when the last worker exits.
}
