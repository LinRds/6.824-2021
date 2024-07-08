package raft

import (
	"log"
	"math/rand"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func versionIncLog(reason string) {
	//log.Printf("version inc for %s", reason)
}
func randName() string {
	char := "abcdefghijkrmnopqrstuvwxyz"
	name := make([]byte, 3)
	for i := 0; i < 3; i++ {
		name[i] = char[rand.Int()%len(char)]
	}
	return string(name)
}

func isLogEqual(l1, l2 *LogEntry) bool {
	return l1.Term == l2.Term && l1.Index == l2.Index
}
