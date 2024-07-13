package raft

import (
	"bytes"
	"github.com/LinRds/raft/labgob"
	"github.com/sirupsen/logrus"
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

func cmdEncode(cmd any) []byte {
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	if err := enc.Encode(cmd); err != nil {
		logrus.Error(err)
		return nil
	}
	return w.Bytes()
}

func cmdDecode(snapshot []byte) any {
	var cmd int
	r := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(r)
	if err := decoder.Decode(&cmd); err != nil {
		log.Fatalf("cmdDecode error: %v", err)
		return 0
	} else {
		return cmd
	}
}
