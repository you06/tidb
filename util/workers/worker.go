package workers

import (
	"context"
	"sync/atomic"
)

type Worker[task any] interface {
	HandleTask(context.Context, task) error
	Close()
}

type NewWorker[task any, w Worker[task]] func() w

type WorkerManager[task any, w Worker[task]] struct {
	max         int32
	used        int32
	count       int32
	wait        int32
	closed      int32
	ctx         context.Context
	cancel      context.CancelFunc
	newWorker   NewWorker[task, w]
	workers     []w
	ch          chan task
	closeCh     chan struct{}
	waitCloseCh chan struct{}
}

func New[task any, w Worker[task]](ctx context.Context, least int, max int32, newWorker NewWorker[task, w], _phantom *task) *WorkerManager[task, w] {
	ctx1, cancel := context.WithCancel(ctx)
	worker := &WorkerManager[task, w]{
		max:         max,
		used:        0,
		count:       0,
		wait:        0,
		closed:      0,
		ctx:         ctx1,
		cancel:      cancel,
		newWorker:   newWorker,
		workers:     make([]w, max),
		ch:          make(chan task, max),
		closeCh:     make(chan struct{}),
		waitCloseCh: make(chan struct{}),
	}
	for i := 0; i < least; i++ {
		worker.createWorker(i)
	}
	return worker
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
	p.workers[idx] = p.newWorker()
	go func() {
	LOOP:
		for {
			select {
			case <-p.closeCh:
				break LOOP
			case t := <-p.ch:
				atomic.AddInt32(&p.used, 1)
				p.workers[idx].HandleTask(p.ctx, t)
				atomic.AddInt32(&p.used, -1)
			}
		}
		if atomic.LoadInt32(&p.wait) == 1 {
			p.waitCloseCh <- struct{}{}
		} else {
			// if not wait, we can reuse count
			var cnt int32
			for {
				cnt = atomic.LoadInt32(&p.count)
				if atomic.CompareAndSwapInt32(&p.count, cnt, cnt-1) {
					break
				}
			}
			// cnt == 1 means the last work thread
			if cnt == 1 {
				close(p.ch)
			}
		}
		p.workers[idx].Close()
	}()
}

func (p *WorkerManager[task, w]) Close(wait bool) {
	if wait {
		atomic.StoreInt32(&p.wait, 1)
	}
	atomic.StoreInt32(&p.closed, 0)
	p.cancel()
	close(p.closeCh)
	if wait {
		cnt := atomic.LoadInt32(&p.count)
		i := int32(0)
		for ; i < cnt; i++ {
			<-p.waitCloseCh
		}
		close(p.ch)
	}
	// if wait is set to false, p.ch will be closed when the last worker exits.
}
