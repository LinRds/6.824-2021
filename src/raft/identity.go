package raft

import (
	"log"
	"time"
)

const (
	follower = iota
	candidate
	leader
)

type identity interface {
	replyVote(rf *Raft, args *RequestVoteArgs) int64
	replyAppendEntries(rf *Raft, args *AppendEntriesArgs) *AppendEntriesReply
	setState(rf *Raft, id int)
	getState() int
}

type Leader struct {
}

func (l *Leader) takingOffice(rf *Raft) {
	log.Printf("server %d become leader", rf.me)

	rf.state.vState.init(len(rf.peers), rf.state.logLen()+1)
}

func (l *Leader) leavingOffice() {
}

func (l *Leader) replyVote(rf *Raft, args *RequestVoteArgs) int64 {
	term := rf.state.getTerm()
	if term >= args.Term {
		return RpcRefuse(term)
	}
	l.setState(rf, follower)
	return rf.id.replyVote(rf, args)
}

func (l *Leader) replyAppendEntries(rf *Raft, args *AppendEntriesArgs) *AppendEntriesReply {
	l.setState(rf, follower)
	return rf.id.replyAppendEntries(rf, args)
}

func (l *Leader) setState(rf *Raft, id int) {
	if id != follower {
		log.Fatal("leader only can trans to follower")
	}
	l.leavingOffice()
	rf.id = rf.getId(follower)
}

func (l *Leader) getState() int {
	return leader
}

type Follower struct{}

func RpcRefuse(term int) int64 {
	return int64(pack(uint32(term), 0))
}

func RpcAccept(term int) int64 {
	return int64(pack(uint32(term), 1))
}

func (f *Follower) replyVote(rf *Raft, args *RequestVoteArgs) int64 {
	term := rf.state.getTerm()
	version := rf.state.version
	defer rf.persistIfVersionMismatch(version)
	if term >= args.Term || rf.state.isVoted() {
		return RpcRefuse(term)
	}
	rf.setVote(&Vote{Voted: true, VotedFor: args.CandidateId})
	rf.state.setTerm(args.Term)
	rf.lastHeartbeatFromLeader = time.Now()
	// reply new term for receiver to validate whether it is a reply for old term
	return RpcAccept(rf.state.getTerm())
	lt, li := rf.state.lastLogEntry()
	// safety
	if lt > args.LastLogTerm || (lt == args.LastLogTerm && li > args.LastLogIndex) {
		rf.state.setTerm(args.Term)
		return RpcRefuse(rf.state.getTerm())
	}
	rf.setVote(&Vote{term: args.Term, voted: true, votedFor: args.CandidateId})
	rf.state.setTerm(args.Term)
	rf.lastHeartbeatFromLeader = time.Now()
	return RpcAccept(rf.state.getTerm())
}

func (f *Follower) replyAppendEntries(rf *Raft, args *AppendEntriesArgs) *AppendEntriesReply {
	term := rf.state.getTerm()
	version := rf.state.version
	defer rf.persistIfVersionMismatch(version)
	if args.Term < term {
		return RpcRefuse(term)
	}
	if term < args.Term {
		rf.state.setTerm(args.Term)
		term = args.Term
	}

	if args.PrevLogIndex != 0 {
		if args.PrevLogIndex > len(rf.state.pState.Logs) {
			log.Println("server", rf.me, "append fail for not have prev log index")
			return rf.refuseAppendEntries(term)
		}
		// reply false if log doesn't contain an entry at prevLogIndex
		// whose term matches prevLogTerm
		prevItem := rf.state.pState.Logs[args.PrevLogIndex-1]
		if prevItem == nil || prevItem.Term != args.PrevLogTerm {
			log.Printf("append fail for server %d: prev log not match\n", rf.me)
			return rf.refuseAppendEntries(term)
		}
	}
	for _, entry := range rf.state.pState.Logs[args.PrevLogIndex:] {
		delete(rf.state.vState.lastIndexEachTerm, entry.Term)
	}
	//log.Printf(`---->server %d,start append entries<----`, rf.me)
	// append any new entries not already in the log
	rf.state.pState.Logs = append(rf.state.pState.Logs[:args.PrevLogIndex], args.Entries...)
	for _, entry := range args.Entries {
		if entry.Index > rf.state.vState.lastIndexEachTerm[entry.Term] {
			rf.state.vState.lastIndexEachTerm[entry.Term] = entry.Index
		}
	}
	// if commitIndex > lastApplied: increment lastApplied, apply
	// log[lastApplied] to state machine
	if rf.state.setCommitIndex(min(args.LeaderCommit, len(rf.state.pState.Logs))) {
		rf.updateLogState()
	}
	return rf.acceptAppendEntries(term)
}

func (f *Follower) setState(rf *Raft, id int) {
	if id == leader {
		log.Fatal("follower can not trans to leader directly")
	}
	if id == candidate {
		rf.state.incTerm()
		rf.state.voteForSelf(rf.me)
	}
	rf.id = rf.getId(id)
}

func (f *Follower) getState() int {
	return follower
}

// Candidate reset to 0 every time
type Candidate struct {
	voteCount int
}

func (c *Candidate) incVoteCount() int {
	c.voteCount++
	return c.voteCount
}

func (c *Candidate) replyVote(rf *Raft, args *RequestVoteArgs) int64 {
	term := rf.state.getTerm()
	if term >= args.Term {
		return RpcRefuse(term)
	}
	c.setState(rf, follower)
	return rf.id.replyVote(rf, args)
}

func (c *Candidate) replyAppendEntries(rf *Raft, args *AppendEntriesArgs) *AppendEntriesReply {
	c.setState(rf, follower)
	return rf.id.replyAppendEntries(rf, args)
}

func (c *Candidate) setState(rf *Raft, id int) {
	rf.id = rf.getId(id)
	if id == leader {
		rf.id.(*Leader).takingOffice(rf)
	}
	if id == candidate {
		rf.state.incTerm()
		rf.state.voteForSelf(rf.me)
	}
}

func (c *Candidate) getState() int {
	return candidate
}
