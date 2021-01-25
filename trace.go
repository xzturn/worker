package worker

import (
	"fmt"
	"runtime"
)

const debugEnabled = false
const defaultString = ""

func trace() string {
	if !debugEnabled {
		return defaultString
	}
	pc := make([]uintptr, 15)
	n := runtime.Callers(2, pc)
	frames := runtime.CallersFrames(pc[:n])
	frame, _ := frames.Next()
	return fmt.Sprintf("%s:%d %s", frame.File, frame.Line, frame.Function)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}
