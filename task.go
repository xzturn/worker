package worker

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
)

// IWorker interface defines the worker behavior
type IWorker interface {
	Work()
}

type XWorker interface {
	Work() error
}

////////////////////////////////////////////////////////////////////////////////

// Task  provides a pool of goroutines that can execute
// any IWorker tasks that are submitted
type Task struct {
	work chan IWorker
	wg   sync.WaitGroup
}

// create task with a work pool
func NewTask(pool int) *Task {
	t := Task{
		work: make(chan IWorker),
	}

	t.wg.Add(pool)
	for i := 0; i < pool; i++ {
		go func() {
			for w := range t.work {
				w.Work()
			}
			t.wg.Done()
		}()
	}
	return &t
}

// wait for all workers to shutdown
func (t *Task) Shutdown() {
	close(t.work)
	t.wg.Wait()
}

// submit work to the pool
func (t *Task) Do(w IWorker) {
	t.work <- w
}

////////////////////////////////////////////////////////////////////////////////

// TaskPool  provides a pool of goroutines that can execute
// any IWorker tasks that are submitted
type TaskPool struct {
	size int
	work chan IWorker
	done chan struct{}
}

func NewTaskPool(n int) *TaskPool {
	if n <= 0 {
		n = runtime.NumCPU()
	}

	t := TaskPool{
		size: n,
		work: make(chan IWorker),
		done: make(chan struct{}),
	}

	for i := 0; i < t.size; i++ {
		go func() {
			for {
				select {
				case w := <-t.work:
					w.Work()
				case <-t.done:
					return
				}
			}
		}()
	}

	return &t
}

// wait for all workers to shutdown
func (t *TaskPool) Shutdown() {
	for i := 0; i < t.size; i++ {
		t.done <- struct{}{}
	}
	close(t.work)
	close(t.done)
}

// submit work to the pool
func (t *TaskPool) Do(w IWorker) {
	t.work <- w
}

////////////////////////////////////////////////////////////////////////////////

// TaskPoolEx  provides a pool of goroutines that can execute
// any XWorker tasks that are submitted
type TaskPoolEx struct {
	size int
	work chan XWorker
	done chan struct{}
}

func NewTaskPoolEx(n int, status chan error) *TaskPoolEx {
	if n <= 0 {
		n = runtime.NumCPU()
	}

	t := TaskPoolEx{
		size: n,
		work: make(chan XWorker),
		done: make(chan struct{}),
	}

	for i := 0; i < t.size; i++ {
		go func() {
			for {
				select {
				case w := <-t.work:
					status <- w.Work()
				case <-t.done:
					return
				}
			}
		}()
	}

	return &t
}

// wait for all workers to shutdown
func (t *TaskPoolEx) Shutdown() {
	for i := 0; i < t.size; i++ {
		t.done <- struct{}{}
	}
	close(t.work)
	close(t.done)
}

// submit work to the pool
func (t *TaskPoolEx) Do(w XWorker) {
	t.work <- w
}

// get the status of all n tasks
func (t TaskPoolEx) Status(status chan error, n int) error {
	msg := make([]string, 0)
	for n > 0 {
		err := <-status
		n--
		if err != nil {
			msg = append(msg, fmt.Sprintf("%v", err))
		}
	}
	if len(msg) == 0 {
		return nil
	}
	return fmt.Errorf("%v", strings.Join(msg, "\n"))
}
