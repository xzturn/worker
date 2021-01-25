package worker

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func fooWork(x WorkParam) error {
	time.Sleep(time.Duration(rand.Intn(x.Param.(int))) * time.Millisecond)
	return fmt.Errorf("%v", x.Taskid)
}

func TestWaitForResult(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	p := NewPattern(fooWork)
	err := p.WaitForResult(500)
	t.Logf("%s - %v\n", trace(), err)
}

func TestSimpleFanOut(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	p := NewPattern(fooWork)
	err := p.FanOut0(2000, 200)
	t.Logf("%s - %v\n", trace(), err)
}

func TestFanOut2(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	p := NewPattern(fooWork)
	params := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}
	err := p.FanOut(params)
	t.Logf("%s - %v\n", trace(), err)
}

func TestWaitForTask(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	p := NewPattern(fooWork)
	p.WaitForTask(300)
}

func TestSimplePooling(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	p := NewPattern(fooWork)
	p.Pooling0(0, 100, 100)
}

func TestPooling2(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	p := NewPattern(fooWork)
	params := []interface{}{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}
	p.Pooling(0, params)
}

func TestFanOutSem0(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	p := NewPattern(fooWork)
	err := p.FanOutSem0(0, 2000, 200)
	t.Logf("%s - %v\n", trace(), err)
}

func TestFanOutSem2(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	p, n := NewPattern(fooWork), 100
	params := make([]interface{}, n)
	for i := 0; i < n; i++ {
		params[i] = i + 10
	}
	err := p.FanOutSem(0, params)
	t.Logf("%s - %v\n", trace(), err)
}

func TestBoundedWorkPooling(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	tasks := []interface{}{100, 200, 200, 300, 500, 400, 450, 120, 370, 600}
	p := NewPattern(fooWork)
	err := p.BoundedWorkPooling(0, tasks)
	t.Logf("%s - %v\n", trace(), err)
}

func TestDrop(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	p := NewPattern(fooWork)
	p.Drop(100, 2000, 10)
}

func TestCancellation(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	p := NewPattern(fooWork)
	err := p.Cancellation(150, 200)
	t.Logf("%s - %v\n", trace(), err)
}
