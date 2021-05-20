package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = true

// const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(0)
	log.SetPrefix(time.Now().Format("15:04:05.000") + " ")

	if Debug {
		log.Printf(format, a...)
	}
	return
}
