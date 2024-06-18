package raft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type LogEntry struct {
	Term  int
	Index int
	Cmd   any
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int

	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	TermAndSuccess int64
}

func (a *AppendEntriesReply) Set(term int64, success bool) {
	var s uint32
	if success {
		s = 1
	}
	packed := pack(uint32(term), s)
	atomic.StoreInt64(&a.TermAndSuccess, int64(packed))
}

func (a *AppendEntriesReply) Get() (int64, bool) {
	term, g := unPack(uint64(atomic.LoadInt64(&a.TermAndSuccess)))
	return int64(term), g == 1
}

type volatileState struct {
	mu          sync.RWMutex
	commitIndex int // index of highest log entry known to be committed(initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine(initialized to 0, increases monotonically)
	//lastApplied atomic.Int64
	nextIndex  []int // only for leader. for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex []int // only for leader. for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)
	stopChan   chan struct{}
}

func (vs *volatileState) init(n int, lastIndex int) {
	vs.nextIndex = make([]int, n)
	for i := range vs.nextIndex {
		vs.nextIndex[i] = lastIndex
	}
	vs.matchIndex = make([]int, n)
	vs.stopChan = make(chan struct{})
}

func (vs *volatileState) setNextIndex(server, index int, inc bool) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if inc && vs.nextIndex[server] >= index {
		return
	}
	vs.nextIndex[server] = index
}

func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args == nil {
		return
	}
	rf.pState.mu.Lock()
	defer rf.pState.mu.Unlock()
	term, id := rf.pState.get()
	if term > args.Term {
		reply.Set(int64(term), false)
		return
	}
	rf.lastHeartbeatFromLeader.Store(time.Now().UnixMilli())
	if args.Entries == nil {
		return
	}
	switch id {
	case leader:
		rf.appendEntriesAsLeader(args, reply, term)
	case follower:
		rf.appendEntriesAsFollower(args, reply, term)
	case candidate:
		rf.appendEntriesAsCandidate(args, reply, term)
	}
}

func (rf *Raft) appendEntriesAsLeader(args *AppendEntriesArgs, reply *AppendEntriesReply, term int) {
	_, set := rf.become(args.Term, follower)
	if !set {
		return
	}
	rf.appendEntriesAsFollower(args, reply, args.Term)
}

func (rf *Raft) appendEntriesAsFollower(args *AppendEntriesArgs, reply *AppendEntriesReply, term int) {
	if term < args.Term {
		_, set := rf.become(args.Term, follower)
		if !set {
			return
		}
	}
	if args.PrevLogIndex != 0 {
		//log.Println("prevIndex is not zero")
		func() {
			rf.pState.logMu.Lock()
			defer rf.pState.logMu.Unlock()
			// reply false if log doesn't contain an entry at prevLogIndex
			// whose term matches prevLogTerm
			if args.PrevLogIndex > len(rf.pState.Logs) {
				reply.Set(int64(term), false)
				return
			}
			prevItem := rf.pState.Logs[args.PrevLogIndex-1]
			if prevItem == nil || prevItem.Term != args.PrevLogTerm {
				reply.Set(int64(term), false)
				return
			}
		}()
	}
	rf.pState.logMu.Lock()
	defer rf.pState.logMu.Unlock()
	rf.state.mu.Lock()
	defer rf.state.mu.Unlock()
	// append any new entries not already in the log
	rf.pState.Logs = append(rf.pState.Logs[:args.PrevLogIndex], args.Entries...)
	if args.LeaderCommit > rf.state.commitIndex {
		rf.state.commitIndex = min(args.LeaderCommit, len(rf.pState.Logs))
	}
	// if commitIndex > lastApplied: increment lastApplied, apply
	// log[lastApplied] to state machine
	if rf.state.commitIndex > rf.state.lastApplied {
		rf.updateLogState()
	}
	reply.Set(int64(term), true)
}

func (rf *Raft) appendEntriesAsCandidate(args *AppendEntriesArgs, reply *AppendEntriesReply, term int) {
	_, set := rf.become(args.Term, follower)
	if !set {
		return
	}
	rf.appendEntriesAsFollower(args, reply, args.Term)
}

// no lock, lock outside
func (rf *Raft) buildAppendArgs(server int) *AppendEntriesArgs {
	prevIndex := rf.state.nextIndex[server] - 1
	if prevIndex < 0 {
		log.Fatalf("invalid nextIndex: %v", rf.state.nextIndex)
	}
	// nextIndex start from 1
	cpLen := max(0, len(rf.pState.Logs)-prevIndex)
	var entries []*LogEntry
	if cpLen > 0 {
		entries = make([]*LogEntry, cpLen)
		copy(entries, rf.pState.Logs[prevIndex:])
	} else {
		return nil
	}
	var prevTerm int
	if prevIndex == 0 {
		prevTerm = 0
	} else {
		prevTerm = rf.pState.Logs[prevIndex-1].Term
	}
	return &AppendEntriesArgs{
		Term:         rf.pState.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.state.commitIndex,
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, result chan *rpcResult, mark string) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply, mark)
	// TODO not ok also should send to result
	if result != nil {
		result <- &rpcResult{ok, server}
	}
	return ok
}

func (rf *Raft) handleAppendEntriesReply(server int, req *AppendEntriesArgs, reply *AppendEntriesReply) {
	myTerm, id := rf.GetState()
	if !id {
		return
	}
	term, success := reply.Get()
	if term == 0 {
		log.Println("term is 0")
		return
	}
	if success {
		log.Printf("term: %d, leader is %d and follower is %d,  success append entry", term, rf.me, server)
		rf.state.setNextIndex(server, req.PrevLogIndex+1+len(req.Entries), true)
		return
		// TODO how to update matchIndex
	}
	if int(term) > myTerm {
		rf.become(int(term), follower)
	} else {
		log.Printf("set nextindex to %d with term %d", req.PrevLogIndex, term)
		rf.state.setNextIndex(server, req.PrevLogIndex, false)
	}
}
