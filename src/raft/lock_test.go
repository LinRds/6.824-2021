package raft

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"
)

func TestLockInDifferentGoRoutine(t *testing.T) {
	lock := NewContextLock(&sync.RWMutex{})
	ctx := context.Background()
	lock.Lock(ctx)
	go func() {
		log.Println("start goroutine")
		lock.Lock(ctx)
		log.Println("not blocked")
	}()
	time.Sleep(time.Second)
}

func TestContextTransfer(t *testing.T) {
	ctx := context.Background()
	func(ctx context.Context) {
		log.Println("run here")
		context.WithValue(ctx, "Hello", "World")
	}(ctx)
	v := ctx.Value("Hello")
	vv, ok := v.(string)
	if ok && vv == "World" {
		log.Println("transfer ok")
	} else {
		log.Println("transfer not ok")
	}
}
