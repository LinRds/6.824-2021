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
	replyAppendEntries(rf *Raft, args *AppendEntriesArgs) int64
	setState(rf *Raft, id int)
	getState() int
}

type Leader struct {
	stopCh chan struct{}
}

func (l *Leader) takingOffice(rf *Raft) {
	log.Printf("server %d become leader", rf.me)
	l.stopCh = make(chan struct{})

	rf.state.vState.nextIndex = make([]int, len(rf.peers))
	rf.state.vState.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.state.vState.matchIndex[i] = 0
		rf.state.vState.nextIndex[i] = rf.state.logLen() + 1
	}
}

func (l *Leader) leavingOffice() {
	close(l.stopCh)
}

func (l *Leader) replyVote(rf *Raft, args *RequestVoteArgs) int64 {
	term := rf.state.getTerm()
	if term >= args.Term {
		return RpcRefuse(term)
	}
	l.setState(rf, follower)
	return rf.id.replyVote(rf, args)
}

func (l *Leader) replyAppendEntries(rf *Raft, args *AppendEntriesArgs) int64 {
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
	// voteGranted is false
	// although Vote also has problem of data race, lock of currentTerm is enough
	term := rf.state.getTerm()
	if term >= args.Term || (rf.state.pState.Vote.term == args.Term && rf.state.pState.Vote.voted) {
		return RpcRefuse(term)
	}
	if rf.setVote(&Vote{term: args.Term, voted: true, votedFor: args.CandidateId}) {
		rf.state.setTerm(args.Term)
		rf.lastHeartbeatFromLeader = time.Now()
		return RpcAccept(rf.state.getTerm())
	}
	return RpcAccept(term)
}

func (f *Follower) replyAppendEntries(rf *Raft, args *AppendEntriesArgs) int64 {
	term := rf.state.getTerm()
	if term < args.Term {
		rf.state.setTerm(args.Term)
	}

	if args.PrevLogIndex != 0 {
		if args.PrevLogIndex > len(rf.state.pState.Logs) {
			log.Println("append fail for not have prevlogindex")
			return RpcRefuse(term)
		}
		// reply false if log doesn't contain an entry at prevLogIndex
		// whose term matches prevLogTerm
		prevItem := rf.state.pState.Logs[args.PrevLogIndex-1]
		if prevItem == nil || prevItem.Term != args.PrevLogTerm {
			log.Println("append fail for prev log not match")
			return RpcRefuse(term)
		}
	}
	//log.Printf(`---->server %d,start append entries<----`, rf.me)
	// append any new entries not already in the log
	rf.state.pState.Logs = append(rf.state.pState.Logs[:args.PrevLogIndex], args.Entries...)
	if args.LeaderCommit > rf.state.vState.commitIndex {
		rf.state.vState.commitIndex = min(args.LeaderCommit, len(rf.state.pState.Logs))
	}
	// if commitIndex > lastApplied: increment lastApplied, apply
	// log[lastApplied] to state machine
	if rf.state.vState.commitIndex > rf.state.vState.lastApplied {
		rf.updateLogState()
	} else {
		//log.Printf("server %d, commit index is %d, last applied is %d", rf.me, rf.state.vState.commitIndex, rf.state.vState.lastApplied)
	}
	return RpcAccept(term)
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

func (c *Candidate) replyAppendEntries(rf *Raft, args *AppendEntriesArgs) int64 {
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
