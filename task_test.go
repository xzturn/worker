package worker

import (
	"fmt"
	"sync"
	"testing"
)

type iworker struct {
	param  int   // parameter for the worker
	status error // status of the worker
}

func newIWorker(p int) *iworker {
	return &iworker{
		param: p,
	}
}

func (w *iworker) Work() {
	DPrintf("%s - working: %d\n", trace(), w.param)
	w.status = fmt.Errorf("%d", w.param)
}

type xworker struct {
	param int // parameter for the worker
}

func newXWorker(p int) *xworker {
	return &xworker{
		param: p,
	}
}

func (w *xworker) Work() error {
	DPrintf("%s - working: %d\n", trace(), w.param)
	return fmt.Errorf("%d", w.param)
}

func TestTask1(t *testing.T) {
	m, n := 10, 13
	task := NewTask(m)
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		w := newIWorker(i)
		go func() {
			task.Do(w)
			wg.Done()
		}()
	}

	wg.Wait()
	task.Shutdown()
}

func TestTask2(t *testing.T) {
	m, n := 10, 13
	tp := NewTaskPool(m)

	for i := 0; i < n; i++ {
		tp.Do(newIWorker(i))
	}

	tp.Shutdown()
}

func TestTask3(t *testing.T) {
	m, n := 10, 13
	status := make(chan error, n)
	tp := NewTaskPoolEx(m, status)

	for i := 0; i < n; i++ {
		tp.Do(newXWorker(i))
	}

	tp.Shutdown()
	err := tp.Status(status, n)
	DPrintf("%s - %v\n", trace(), err)
	close(status)
}
