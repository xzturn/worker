package worker

import (
	"fmt"
	"runtime"
	"strings"
)

// concurrent work parameters
type workParams struct {
	cidx   chan int
	done   chan struct{}
	opaque []interface{}
	status []error
}

func newParams(opaque []interface{}) *workParams {
	return &workParams{
		cidx:   make(chan int),
		done:   make(chan struct{}),
		opaque: opaque,
		status: make([]error, len(opaque)),
	}
}

func (p *workParams) clear() {
	close(p.cidx)
	close(p.done)
}

func (p workParams) Status() error {
	msg := make([]string, 0)
	for i, e := range p.status {
		if e != nil {
			msg = append(msg, fmt.Sprintf("[%d] '%v': %v", i, p.opaque[i], e))
		}
	}
	if len(msg) != 0 {
		return fmt.Errorf("%v", strings.Join(msg, "\n"))
	}
	return nil
}

// worker abstraction
type Worker interface {
	Run(opaque interface{}) error
}

func runWorker(w Worker, p *workParams) {
	for {
		select {
		case idx := <-p.cidx:
			p.status[idx] = w.Run(p.opaque[idx])
		case <-p.done:
			return
		}
	}
}

func ConcurrentRun(w Worker, k int, opaques []interface{}) error {
	n := len(opaques)
	if k <= 0 {
		k = runtime.NumCPU()
	}
	p := newParams(opaques)
	defer p.clear()

	for i := 0; i < k; i++ {
		go runWorker(w, p)
	}
	for i := 0; i < n; i++ {
		p.cidx <- i
	}
	for i := 0; i < k; i++ {
		p.done <- struct{}{}
	}

	return p.Status()
}
