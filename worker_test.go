package worker

import (
	"fmt"
	"testing"
)

type sworker struct{}

func (w sworker) Run(param interface{}) error {
	fmt.Printf("%s - sworker.Run(%v) success!\n", trace(), param.(string))
	return nil
}

type fworker struct{}

func (w fworker) Run(param interface{}) error {
	return fmt.Errorf("fworker.Run(%v) fail!", param.(string))
}

func TestFooSuccess(t *testing.T) {
	var w sworker
	params := []interface{}{"sfoo1", "sfoo2"}
	if err := ConcurrentRun(w, 0, params); err != nil {
		t.Logf("%v\n", err)
	}
}

func TestFooFailure(t *testing.T) {
	var w fworker
	params := []interface{}{"ffoo1", "ffoo2", "ffoo3"}
	if err := ConcurrentRun(w, 0, params); err != nil {
		t.Logf("%v\n", err)
	}
}
