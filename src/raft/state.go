package raft

import (
	"github.com/sirupsen/logrus"
	"math/bits"
)

type bitMap uint64

func (b bitMap) add(i int) bitMap {
	return b | (1 << i)
}

func (b bitMap) len() int {
	return bits.OnesCount64(uint64(b))
}

type LogEntry struct {
	Term  int
	Count bitMap
	Index int
	Cmd   any
}

// PersistentState Updated on stable storage before responding to RPC
type PersistentState struct {
	CurrentTerm int
	Vote        *Vote
	Logs        []*LogEntry
}

func (ps *PersistentState) copy() *PersistentState {
	return &PersistentState{
		CurrentTerm: ps.CurrentTerm,
	}
}

type volatileState struct {
	commitIndex       int   // index of highest log entry known to be committed(initialized to 0, increases monotonically)
	lastApplied       int   // index of highest log entry applied to state machine(initialized to 0, increases monotonically)
	nextIndex         []int // only for leader. for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex        []int // only for leader. for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)
	lastIndexEachTerm map[int]int
}

func (vs *volatileState) copy() *volatileState {
	return &volatileState{
		commitIndex: vs.commitIndex,
		lastApplied: vs.lastApplied,
		nextIndex:   vs.nextIndex,
		matchIndex:  vs.matchIndex,
	}
}

func (vs *volatileState) fastIndex(term int) (int, int) {
	lastCurTerm := vs.lastIndexInTerm(term)
	if lastCurTerm != -1 {
		return term, lastCurTerm
	}
	prevTerm := -1
	for k := range vs.lastIndexEachTerm {
		if k < term && k > prevTerm {
			prevTerm = k
		}
	}
	return prevTerm, vs.lastIndexEachTerm[prevTerm]
}

func (vs *volatileState) lastIndexInTerm(term int) int {
	if v, ok := vs.lastIndexEachTerm[term]; ok {
		return v
	}
	return -1
}

func (vs *volatileState) init(n int, lastIndex int) {
	vs.nextIndex = make([]int, n)
	for i := range vs.nextIndex {
		vs.nextIndex[i] = lastIndex
	}
	vs.matchIndex = make([]int, n)
}

func (vs *volatileState) setNextIndex(server, index int, inc bool) {
	if inc && vs.nextIndex[server] >= index {
		return
	}
	vs.nextIndex[server] = index
}

func (vs *volatileState) setMatchIndex(server, index int) {
	vs.matchIndex[server] = index
}

type State struct {
	version int
	pState  *PersistentState
	vState  *volatileState
}

// half open interval [from,to]
func (s *State) getLogRange(from, to int) []*LogEntry {
	if from < 1 || to < 1 || from > to {
		return nil
	}
	if to == -1 {
		return s.pState.Logs[from-1:]
	}
	return s.pState.Logs[from-1 : to]
}

// The caller is responsible for ensuring the index is within bounds.
func (s *State) getLogEntry(index int) *LogEntry {
	return s.pState.Logs[index-1]
}

// if two entry equal, any entry before are equal too
func (s *State) lastLogEntry() (int, int) {
	n := s.logLen()
	if n == 0 {
		return -1, -1
	}
	lastEntry := s.getLogEntry(n)
	return lastEntry.Term, lastEntry.Index
}

func (s *State) setCommitIndex(index int) bool {
	set := false
	if s.vState.commitIndex < index {
		s.vState.commitIndex = index
		set = true
	}
	return set
}

func (s *State) isVoted() bool {
	if s.pState.Vote == nil {
		return false
	}
	return s.pState.Vote.Voted
}

func (s *State) voteForSelf(me int) {
	versionIncLog("vote for self")
	s.version++
	s.pState.Vote = &Vote{
		VotedFor: me,
		Voted:    true,
	}
}
func (s *State) init() {
	s.pState = &PersistentState{
		CurrentTerm: 1,
		Vote:        new(Vote),
		Logs:        make([]*LogEntry, 0, 10),
	}
	s.version = 1
	s.vState = new(volatileState)
	s.vState.lastIndexEachTerm = make(map[int]int)
}

func (s *State) match(version int) bool {
	return s.version == version
}

func (s *State) getTerm() int {
	return s.pState.CurrentTerm
}

func (s *State) incTerm() {
	versionIncLog("inc term")
	s.version++
	s.pState.Vote = nil
	s.pState.CurrentTerm++
}
func (s *State) setTerm(term int) {
	if s.pState.CurrentTerm > term {
		logrus.Println("new term is less than current in setTerm")
		return
	}
	versionIncLog("set term")
	s.version++
	s.pState.CurrentTerm = term
	s.pState.Vote = nil
}

func (s *State) setNextIndex(server, index int, inc bool) {
	s.vState.setNextIndex(server, index, inc)
}

func (s *State) getNextIndex(server int) int {
	return s.vState.nextIndex[server]
}
func (s *State) setMatchIndex(server, index int) {
	s.vState.setMatchIndex(server, index)
}

func (s *State) getMatchIndex(server int) int {
	return s.vState.matchIndex[server]
}

func (s *State) logAppend(entries ...*LogEntry) {
	if len(entries) > 0 {
		versionIncLog("log append")
		s.version++
	}
	s.pState.Logs = append(s.pState.Logs, entries...)
}

func (s *State) logLen() int {
	return len(s.pState.Logs)
}

func (s *State) fastIndex(term int) (int, int) {
	return s.vState.fastIndex(term)
}

func (s *State) lastIndexInTerm(term int) int {
	return s.vState.lastIndexInTerm(term)
}

func (s *State) updateLastIndex(entries ...*LogEntry) {
	for _, entry := range entries {
		if entry.Index > s.vState.lastIndexEachTerm[entry.Term] {
			s.vState.lastIndexEachTerm[entry.Term] = entry.Index
		}
	}
}

func (s *State) deleteLastIndex(entries []*LogEntry) {
	for _, entry := range entries {
		delete(s.vState.lastIndexEachTerm, entry.Term)
	}
}
