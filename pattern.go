package worker

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"
)

type WorkParam struct {
	Param  interface{}
	Taskid int
}

// concurrency patterns
type Pattern struct {
	workFunc func(param WorkParam) error
}

func NewPattern(w func(param WorkParam) error) *Pattern {
	return &Pattern{workFunc: w}
}

// waitForResult: You are a manager and you hire a new employee. Your new
// employee knows immediately what they are expected to do and starts their
// work. You sit waiting for the result of the employee's work. The amount
// of time you wait on the employee is unknown because you need a
// guarantee that the result sent by the employee is received by you.
func (p Pattern) WaitForResult(param interface{}) error {
	// status channel of task
	ch := make(chan error)
	defer close(ch)

	// worker do task immediately
	go func() {
		ch <- p.workFunc(WorkParam{param, 0})
		DPrintf("%s - employee: sent signal: %v\n", trace(), param)
	}()

	// wait for task done
	err := <-ch
	DPrintf("%s - manager : recv'd signal : %v\n", trace(), err)
	return err
}

// fanOut: You are a manager and you hire one new employee for the exact amount
// of work you have to get done. Each new employee knows immediately what they
// are expected to do and starts their work. You sit waiting for all the results
// of the employees work. The amount of time you wait on the employees is
// unknown because you need a guarantee that all the results sent by employees
// are received by you. No given employee needs an immediate guarantee that you
// received their result.
func (p Pattern) FanOut0(n int, param interface{}) error {
	// status channel of n tasks
	ch := make(chan error, n)
	defer close(ch)

	// n workers to do n tasks immediately
	for i := 0; i < n; i++ {
		go func(idx int) {
			ch <- p.workFunc(WorkParam{param, idx})
			DPrintf("%s - employee : sent signal : %d\n", trace(), idx)
		}(i)
	}

	// wait for all task done
	status := make([]string, 0)
	for n > 0 {
		err := <-ch
		n--
		DPrintf("%s - manager : recv'd signal [%d] %v\n", trace(), n, err)
		if err != nil {
			status = append(status, fmt.Sprintf("%v", err))
		}
	}
	if len(status) == 0 {
		return nil
	}
	return errors.New(strings.Join(status, "\n"))
}

func (p Pattern) FanOut(params []interface{}) error {
	// status channel of n tasks
	n := len(params)
	ch := make(chan error, n)
	defer close(ch)

	// n workers to do n tasks immediately
	for i := 0; i < n; i++ {
		go func(idx int) {
			ch <- p.workFunc(WorkParam{params[idx], idx})
			DPrintf("%s - employee : sent signal : %d\n", trace(), idx)
		}(i)
	}

	// wait for all task done
	status := make([]string, 0)
	for n > 0 {
		err := <-ch
		n--
		DPrintf("%s - manager : recv'd signal [%d] %v\n", trace(), n, err)
		if err != nil {
			status = append(status, fmt.Sprintf("%v", err))
		}
	}
	if len(status) == 0 {
		return nil
	}
	return errors.New(strings.Join(status, "\n"))
}

// waitForTask: You are a manager and you hire a new employee. Your new
// employee doesn't know immediately what they are expected to do and waits for
// you to tell them what to do. You prepare the work and send it to them. The
// amount of time they wait is unknown because you need a guarantee that the
// work your sending is received by the employee.
func (p Pattern) WaitForTask(param interface{}) {
	// param channel for task
	ch := make(chan WorkParam)
	defer close(ch)

	// worker wait task
	go func() {
		task := <-ch
		DPrintf("%s - employee : recv'd signal : %v\n", trace(), task)
		p.workFunc(task)
	}()

	// send task to worker
	ch <- WorkParam{param, 0}
	DPrintf("%s - manager : sent signal %v\n", trace(), param)
}

// pooling: You are a manager and you hire a team of employees. None of the new
// employees know what they are expected to do and wait for you to provide work.
// When work is provided to the group, any given employee can take it and you
// don't care who it is. The amount of time you wait for any given employee to
// take your work is unknown because you need a guarantee that the work your
// sending is received by an employee.
func (p Pattern) Pooling0(k, n int, param interface{}) {
	// param channel for task
	ch := make(chan WorkParam)

	if k <= 0 {
		k = runtime.NumCPU()
	}
	// k workers waiting for task
	for i := 0; i < k; i++ {
		go func(idx int) {
			for task := range ch {
				DPrintf("%s - employee %d : recv'd signal : %v\n", trace(), idx, task)
				p.workFunc(task)
			}
			DPrintf("employee %d : recv'd shutdown signal\n", idx)
		}(i)
	}

	// sent n tasks
	for w := 0; w < n; w++ {
		DPrintf("%s - manager : sent signal %d\n", trace(), w)
		ch <- WorkParam{param, w}
	}

	DPrintf("%s - manager : sent shutdown signal\n", trace())
	close(ch)
}

func (p Pattern) Pooling(k int, params []interface{}) {
	// param channel for task
	ch := make(chan WorkParam)

	n := len(params)
	if k <= 0 {
		k = runtime.NumCPU()
	}
	if k > n {
		k = n
	}
	// k workers waiting for task
	for i := 0; i < k; i++ {
		go func(idx int) {
			for param := range ch {
				DPrintf("%s - employee %d : recv'd signal : %v\n", trace(), idx, param)
				p.workFunc(param)
			}
			DPrintf("employee %d : recv'd shutdown signal\n", idx)
		}(i)
	}

	// sent n tasks
	for i, param := range params {
		DPrintf("%s - manager : sent signal %d\n", trace(), i)
		ch <- WorkParam{param, i}
	}

	DPrintf("%s - manager : sent shutdown signal\n", trace())
	close(ch)
}

// fanOutSem: You are a manager and you hire one new employee for the exact amount
// of work you have to get done. Each new employee knows immediately what they
// are expected to do and starts their work. However, you don't want all the
// employees working at once. You want to limit how many of them are working at
// any given time. You sit waiting for all the results of the employees work.
// The amount of time you wait on the employees is unknown because you need a
// guarantee that all the results sent by employees are received by you. No
// given employee needs an immediate guarantee that you received their result.
func (p Pattern) FanOutSem0(k, n int, param interface{}) error {
	// status channel for n tasks
	ch := make(chan error, n)
	defer close(ch)

	// n workers, n tasks, k concurrently working workers
	if k <= 0 {
		k = runtime.NumCPU()
	}
	if k > n {
		k = n
	}
	sem := make(chan bool, k)

	// n workers start immediately for the n tasks
	// but only k workers are working concurrently, not all n workers
	for i := 0; i < n; i++ {
		go func(idx int) {
			sem <- true
			{
				ch <- p.workFunc(WorkParam{param, idx})
				DPrintf("%s - employee : sent signal %v\n", trace(), idx)
			}
			<-sem
		}(i)
	}

	// wait all n tasks done
	status := make([]string, 0)
	for n > 0 {
		err := <-ch
		n--
		DPrintf("%s - manager : recv'd signal [%d] %v\n", trace(), n, err)
		if err != nil {
			status = append(status, fmt.Sprintf("%v", err))
		}
	}
	if len(status) == 0 {
		return nil
	}
	return errors.New(strings.Join(status, "\n"))
}

func (p Pattern) FanOutSem(k int, params []interface{}) error {
	// status channel for n tasks
	n := len(params)
	ch := make(chan error, n)
	defer close(ch)

	// n workers, n tasks, k concurrently working workers
	if k <= 0 {
		k = runtime.NumCPU()
	}
	if k > n {
		k = n
	}
	sem := make(chan bool, k)

	// n workers start immediately for the n tasks
	// but only k workers are working concurrently, not all n workers
	for i := 0; i < n; i++ {
		go func(idx int) {
			sem <- true
			{
				ch <- p.workFunc(WorkParam{params[idx], idx})
				DPrintf("%s - employee : sent signal %v\n", trace(), idx)
			}
			<-sem
		}(i)
	}

	// wait all n tasks done
	status := make([]string, 0)
	for n > 0 {
		err := <-ch
		n--
		DPrintf("%s - manager : recv'd signal [%d] %v\n", trace(), n, err)
		if err != nil {
			status = append(status, fmt.Sprintf("%v", err))
		}
	}
	if len(status) == 0 {
		return nil
	}
	return errors.New(strings.Join(status, "\n"))
}

// boundedWorkPooling: You are a manager and you hire a team of employees. None of
// the new employees know what they are expected to do and wait for you to
// provide work. The amount of work that needs to get done is fixed and staged
// ahead of time. Any given employee can take work and you don't care who it is
// or what they take. The amount of time you wait on the employees to finish
// all the work is unknown because you need a guarantee that all the work is
// finished.
func (p Pattern) BoundedWorkPooling(k int, params []interface{}) error {
	if k <= 0 {
		// k workers will do the n tasks
		k = runtime.NumCPU()
	}

	// k workers to sync
	var wg sync.WaitGroup
	wg.Add(k)

	// param channel for k workers
	ch := make(chan WorkParam, k)
	status := make([]string, 0)

	// k workers waiting for n tasks
	for i := 0; i < k; i++ {
		go func(idx int) {
			defer wg.Done()
			for w := range ch {
				DPrintf("%s - employee %d : recv'd signal %v\n", trace(), idx, w)
				if err := p.workFunc(w); err != nil {
					status = append(status, fmt.Sprintf("%v", err))
				}
			}
			DPrintf("%s - employee %d : recv'd shutdown signal\n", trace(), idx)
		}(i)
	}

	// n tasks sent to param channel
	for i, param := range params {
		ch <- WorkParam{param, i}
	}
	close(ch)

	// wait all the k workers done
	wg.Wait()

	if len(status) == 0 {
		return nil
	}
	return errors.New(strings.Join(status, "\n"))
}

// drop: You are a manager and you hire a new employee. Your new employee
// doesn't know immediately what they are expected to do and waits for
// you to tell them what to do. You prepare the work and send it to them. The
// amount of time they wait is unknown because you need a guarantee that the
// work your sending is received by the employee. You won't wait for the
// employee to take the work if they are not ready to receive it. In that case
// you drop the work on the floor and try again with the next piece of work.
func (p Pattern) Drop(capacity, n int, param interface{}) {
	// param channel with a large capacity
	ch := make(chan WorkParam, capacity)

	// worker waiting for tasks
	go func() {
		for w := range ch {
			DPrintf("%s - employee : recv'd signal %v\n", trace(), w)
			p.workFunc(w)
		}
	}()

	// send n tasks, drop if channel full
	for w := 0; w < n; w++ {
		select {
		case ch <- WorkParam{param, w}:
			DPrintf("%s - manager : sent signal %d\n", trace(), w)
		default:
			DPrintf("%s - manager : dropped data %d\n", trace(), w)
		}
	}

	close(ch)
	DPrintf("%s - manager : sent shutdown signal\n", trace())
}

// cancellation: You are a manager and you hire a new employee. Your new
// employee knows immediately what they are expected to do and starts their
// work. You sit waiting for the result of the employee's work. The amount
// of time you wait on the employee is unknown because you need a
// guarantee that the result sent by the employee is received by you. Except
// you are not willing to wait forever for the employee to finish their work.
// They have a specified amount of time and if they are not done, you don't
// wait and walk away.
func (p Pattern) Cancellation(delayms int, param interface{}) error {
	// set cancel timeout
	duration := time.Duration(delayms) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	// status channel for task
	ch := make(chan error, 1)
	defer close(ch)

	// worker do task immediately
	go func() {
		ch <- p.workFunc(WorkParam{param, 0})
		DPrintf("%s - employee: sent signal: %v\n", trace(), param)
	}()

	// wait for task done or timeout
	select {
	case e := <-ch:
		DPrintf("%s - work complete: %v\n", trace(), e)
		return e
	case <-ctx.Done():
		DPrintf("%s - work cancelled\n", trace())
		return errors.New("timeout")
	}
	return nil
}
