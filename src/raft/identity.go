package raft

import (
	"fmt"
	"github.com/sirupsen/logrus"
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
	logrus.WithFields(logrus.Fields{
		"server": rf.me,
	}).Info("become leader")

	rf.state.vState.init(len(rf.peers), rf.state.logLen()+1)
}

func (l *Leader) leavingOffice(rf *Raft) {
	rf.saveRaceLeader(false)
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
		logrus.Fatal("leader only can trans to follower")
	}
	l.leavingOffice(rf)
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
	if term > args.Term {
		logrus.WithFields(logrus.Fields{
			"me":        rf.me,
			"candidate": args.CandidateId,
			"myTerm":    rf.state.getTerm(),
			"otherTerm": args.Term,
		}).Warn("refuse vote")
		return RpcRefuse(rf.state.getTerm()) // reply new term for receiver to validate whether it is a reply for old term
	}
	if term == args.Term && rf.state.isVoted() {
		logrus.WithFields(logrus.Fields{
			"me":        rf.me,
			"candidate": args.CandidateId,
			"voted":     rf.state.pState.Vote.Voted,
			"votedFor":  rf.state.pState.Vote.VotedFor,
		}).Warn("refuse vote")
		return RpcRefuse(rf.state.getTerm()) // reply new term for receiver to validate whether it is a reply for old term
	}
	if term < args.Term {
		rf.state.setTerm(args.Term)
	}
	// safety
	lt, li := rf.state.lastLogEntry()
	if lt > args.LastLogTerm || (lt == args.LastLogTerm && li > args.LastLogIndex) {
		return RpcRefuse(rf.state.getTerm())
	}
	rf.setVote(&Vote{Voted: true, VotedFor: args.CandidateId})
	rf.lastHeartbeatFromLeader = time.Now()
	return RpcAccept(rf.state.getTerm())
}

func (f *Follower) replyAppendEntries(rf *Raft, args *AppendEntriesArgs) *AppendEntriesReply {
	oldTerm := rf.state.getTerm()
	version := rf.state.version
	log := logrus.WithFields(logrus.Fields{
		"server":       rf.me,
		"oldTerm":      oldTerm,
		"prevIndex":    args.PrevLogIndex,
		"prevTerm":     args.PrevLogTerm,
		"from":         args.From,
		"leaderCommit": args.LeaderCommit,
		"client":       args.LeaderId,
	})
	if args.Term < oldTerm {
		log = log.WithField("reason", "term too low")
		return rf.refuseAppendEntries(log, args.PrevLogTerm)
	}
	if oldTerm < args.Term {
		rf.state.setTerm(args.Term)
	}

	if args.PrevLogIndex != 0 {
		if args.PrevLogIndex > rf.state.logLen() {
			log = log.WithField("reason", "log too short")
			return rf.refuseAppendEntries(log, args.PrevLogTerm)
		}
		// reply false if log doesn't contain an entry at prevLogIndex
		// whose term matches prevLogTerm
		prevItem := rf.state.pState.Logs[args.PrevLogIndex-1]
		if prevItem == nil || prevItem.Term != args.PrevLogTerm {
			log = log.WithField("reason", fmt.Sprintf("log not match, expected %d got %d", prevItem.Term, args.PrevLogTerm))
			return rf.refuseAppendEntries(log, args.PrevLogTerm)
		}
	}
	// To prevent errors from requests with outdated parameters,
	// validate them to avoid inadvertent deletion of already append logs.
	if oldTerm == args.Term && args.PrevLogIndex < rf.state.logLen() {
		return rf.acceptAppendEntries(log)
	}
	// TODO if from == heartbeat && len(log) == 0 return ?
	for _, entry := range rf.state.pState.Logs[args.PrevLogIndex:] {
		delete(rf.state.vState.lastIndexEachTerm, entry.Term)
	}
	//log.Printf(`---->server %d,start append entries<----`, rf.me)
	// append any new entries not already in the log
	rf.state.pState.Logs = rf.state.pState.Logs[:args.PrevLogIndex]
	rf.state.logAppend(args.Entries...)
	from := args.PrevLogIndex + 1
	to := rf.state.logLen()
	if from > to {
		log.Fatal("from bigger than to")
	}
	log = log.WithFields(logrus.Fields{
		"appendFrom": from,
		"appendTo":   to,
	})
	for _, entry := range args.Entries {
		if entry.Index > rf.state.vState.lastIndexEachTerm[entry.Term] {
			rf.state.vState.lastIndexEachTerm[entry.Term] = entry.Index
		}
	}
	rf.persistIfVersionMismatch(version)
	// if commitIndex > lastApplied: increment lastApplied, apply
	// log[lastApplied] to state machine
	if rf.state.setCommitIndex(min(args.LeaderCommit, rf.state.logLen())) {
		rf.updateLogState()
	}
	return rf.acceptAppendEntries(log)
}

func (f *Follower) setState(rf *Raft, id int) {
	if id == leader {
		logrus.Fatal("follower can not trans to leader directly")
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
		rf.saveRaceLeader(true)
	}
	if id == candidate {
		rf.state.incTerm()
		rf.state.voteForSelf(rf.me)
	}
}

func (c *Candidate) getState() int {
	return candidate
}
