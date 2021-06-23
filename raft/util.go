package raft

import (
	"fmt"
	"time"
)

// Debugging
const Debug = true

// //
// const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {

	if Debug {
		now := time.Now().Format("15:04:05.000")
		fmt.Printf(now+format+"\n", a...)
	}
	return
}
