package raft

import "log"

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
	commitIndex int   // index of highest log entry known to be committed(initialized to 0, increases monotonically)
	lastApplied int   // index of highest log entry applied to state machine(initialized to 0, increases monotonically)
	nextIndex   []int // only for leader. for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex  []int // only for leader. for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)
}

func (vs *volatileState) copy() *volatileState {
	return &volatileState{
		commitIndex: vs.commitIndex,
		lastApplied: vs.lastApplied,
		nextIndex:   vs.nextIndex,
		matchIndex:  vs.matchIndex,
	}
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

// The caller is responsible for ensuring the index is within bounds.
func (s *State) getLogEntry(index int) *LogEntry {
	return s.pState.Logs[index]
}

func (s *State) lastLogEntry() (int, int) {
	n := s.logLen()
	if n == 0 {
		return -1, -1
	}
	return s.pState.Logs[n-1].Term, s.pState.Logs[n-1].Index
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
	s.vState = new(volatileState)
}

func (s *State) match(version int) bool {
	return s.version == version
}

func (s *State) getTerm() int {
	return s.pState.CurrentTerm
}

func (s *State) incTerm() {
	s.pState.CurrentTerm++
}
func (s *State) setTerm(term int) {
	if s.pState.CurrentTerm > term {
		log.Println("new term is less than current in setTerm")
		return
	}
	s.version++
	s.pState.CurrentTerm = term
	s.pState.Vote = nil
}

func (s *State) copy() *State {
	return &State{
		version: s.version,
		pState:  s.pState.copy(),
		vState:  s.vState.copy(),
	}
}

func (s *State) setNextIndex(server, index int, inc bool) {
	s.vState.setNextIndex(server, index, inc)
}

func (s *State) setMatchIndex(server, index int) {
	s.vState.setMatchIndex(server, index)
}

func (s *State) getMatchIndex(server int) int {
	return s.vState.matchIndex[server]
}

func (s *State) logAppend(entries ...*LogEntry) {
	if len(entries) > 0 {
		s.version++
	}
	s.pState.Logs = append(s.pState.Logs, entries...)
}

func (s *State) logLen() int {
	return len(s.pState.Logs)
}
