package raft

import (
	"context"
	"log"
	"sync"
)

const (
	Lock   = "Lock"
	RWLock = "RWLock"
)

type ContextLock struct {
	RWLock *sync.RWMutex
}

func NewContextLock(RWLock *sync.RWMutex) *ContextLock {
	return &ContextLock{RWLock: RWLock}
}

func (cl *ContextLock) Lock(ctx context.Context) context.Context {
	locked := ctx.Value(Lock)
	if locked != nil && locked.(bool) == true {
		return ctx
	}
	cl.RWLock.Lock()
	return context.WithValue(ctx, Lock, true)
}

func (cl *ContextLock) Unlock(ctx context.Context) {
	locked := ctx.Value(Lock)
	if locked != nil && locked.(bool) == false {
		log.Fatal("Lock is not locked")
	}
	cl.RWLock.Unlock()
	ctx = context.WithValue(ctx, Lock, false)
}

func (cl *ContextLock) RLock(ctx context.Context) {
	for _, lock := range []string{Lock, RWLock} {
		locked := ctx.Value(lock)
		if locked != nil && locked.(bool) == true {
			return
		}
	}
	cl.RWLock.RLock()
	ctx = context.WithValue(ctx, RWLock, true)
}

func (cl *ContextLock) RUnlock(ctx context.Context) {
	for _, lock := range []string{Lock, RWLock} {
		locked := ctx.Value(lock)
		if locked != nil && locked.(bool) == true {
			switch lock {
			case Lock:
				cl.RWLock.Unlock()
			case RWLock:
				cl.RWLock.RUnlock()
				ctx = context.WithValue(ctx, RWLock, false)
			}
			return
		}
	}
	log.Fatalf("Lock is not locked")
}
