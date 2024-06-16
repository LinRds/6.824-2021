package raft

import "sync"

// PersistentState Updated on stable storage before responding to RPC
type PersistentState struct {
	mu          sync.RWMutex
	Value       int
	CurrentTerm int
	Vote        *Vote
	logMu       sync.RWMutex
	Logs        []*LogEntry
}

func (ps *PersistentState) inc() (int, bool) {
	if ps.Value == leader {
		return 0, false
	}
	ps.CurrentTerm++
	ps.Value = candidate
	return ps.CurrentTerm, true
}

func (ps *PersistentState) set(old, val int) bool {
	if ps.CurrentTerm != old {
		return false
	}
	ps.CurrentTerm = val
	ps.Value = follower
	return true
}

func (ps *PersistentState) elegant(term int) bool {
	if term != ps.CurrentTerm || ps.Value != candidate {
		return false
	}
	ps.Value = leader
	return true
}

func (ps *PersistentState) get() (int, int) {
	return ps.CurrentTerm, ps.Value
}

func (ps *PersistentState) lastLog() (prevLogIndex int, prevLogTerm int) {
	n := len(ps.Logs)
	if n < 2 {
		return
	}
	prevLogTerm = ps.Logs[n-1].Term
	// index start from 1
	prevLogIndex = ps.Logs[n-1].Index
	return
}
