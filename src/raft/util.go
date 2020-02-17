package raft

import (
	"log"
)

// Debugging
const Debug = 0

func DPrintf(funcName string, format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(funcName + " " + format, a...)
	}
	return
}
