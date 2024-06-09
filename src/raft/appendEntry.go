package raft

import "time"

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
	Term    int
	Success bool
}

func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args == nil {
		return
	}
	rf.identity.mu.Lock()
	defer rf.identity.mu.Unlock()
	term, id := rf.identity.get()
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
	if term <= args.Term {
		_, set := rf.become(args.Term, follower)
		if !set {
			return
		}
		rf.lastHeartbeatFromLeader.Store(time.Now().UnixMilli())
		rf.state.stepDown()
	}
}

func (rf *Raft) appendEntriesAsFollower(args *AppendEntriesArgs, reply *AppendEntriesReply, term int) {
	if term <= args.Term {
		//rf.lastHeartbeatFromLeader.Store(time.Now().UnixMilli())
		rf.state.stepDown()
		_, set := rf.become(args.Term, follower)
		if !set {
			return
		}
	}
}

func (rf *Raft) appendEntriesAsCandidate(args *AppendEntriesArgs, reply *AppendEntriesReply, term int) {
	if term <= args.Term {
		rf.lastHeartbeatFromLeader.Store(time.Now().UnixMilli())
		rf.state.stepDown()
		_, set := rf.become(args.Term, follower)
		if !set {
			return
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}
