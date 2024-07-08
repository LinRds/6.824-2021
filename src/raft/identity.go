package raft

import (
	"errors"
	"github.com/sirupsen/logrus"
	"time"
)

const (
	follower = iota
	candidate
	leader
)

var (
	errLogTooShort = errors.New("log too short")
	errLogNotMatch = errors.New("log not match")
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
	log := logrus.WithFields(logrus.Fields{
		"server": rf.me,
		"client": args.CandidateId,
	})
	if term > args.Term {
		log.WithFields(logrus.Fields{
			"myTerm":    rf.state.getTerm(),
			"otherTerm": args.Term,
		}).Warn("refuse vote")
		return RpcRefuse(rf.state.getTerm()) // reply new term for receiver to validate whether it is a reply for old term
	}
	if term == args.Term && rf.state.isVoted() {
		log.WithFields(logrus.Fields{
			"voted":    rf.state.pState.Vote.Voted,
			"votedFor": rf.state.pState.Vote.VotedFor,
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

func prevLogValidation(rf *Raft, prevIndex, prevTerm int) error {
	if prevIndex > rf.state.logLen() {
		return errLogTooShort
	}
	// reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	prevItem := rf.state.getLogEntry(prevIndex)
	if prevItem == nil || prevItem.Term != prevTerm {
		return errLogNotMatch
	}
	return nil
}

func (f *Follower) replyAppendEntries(rf *Raft, args *AppendEntriesArgs) *AppendEntriesReply {
	oldTerm := rf.state.getTerm()
	version := rf.state.version
	log := logrus.WithFields(logrus.Fields{
		"server":       rf.me,
		"oldTerm":      oldTerm,
		"newTerm":      args.Term,
		"prevIndex":    args.PrevLogIndex,
		"prevTerm":     args.PrevLogTerm,
		"from":         args.From,
		"leaderCommit": args.LeaderCommit,
		"client":       args.LeaderId,
		"entryLen":     len(args.Entries),
	})
	if oldTerm < args.Term {
		rf.state.setTerm(args.Term)
	}
	var err error
	if args.PrevLogIndex > 0 {
		err = prevLogValidation(rf, args.PrevLogIndex, args.PrevLogTerm)
	}
	if err != nil {
		log = log.WithField("reason", err)
		return rf.refuseAppendEntries(log, args.PrevLogTerm)
	}

	defer func() {
		rf.persistIfVersionMismatch(version)
		// if commitIndex > lastApplied: increment lastApplied, apply
		// log[lastApplied] to state machine
		if rf.state.setCommitIndex(min(args.LeaderCommit, rf.state.logLen())) {
			rf.updateLogState()
		}
	}()
	entryLen := len(args.Entries)
	if entryLen == 0 {
		return rf.acceptAppendEntries(log)
	}
	// To prevent errors from requests with outdated parameters,
	// validate them to avoid inadvertent deletion of already append logs.
	lastEntry := args.Entries[entryLen-1]
	if lastEntry.Index <= rf.state.logLen() && isLogEqual(lastEntry, rf.state.getLogEntry(lastEntry.Index)) {
		return rf.acceptAppendEntries(log)
	}
	rf.state.deleteLastIndex(rf.state.getLogRange(args.PrevLogIndex+1, -1))
	// append any entries in args
	rf.state.pState.Logs = rf.state.getLogRange(1, args.PrevLogIndex)
	rf.state.logAppend(args.Entries...)
	// from could be bigger than to in fast sync request which entry is nil
	from := args.PrevLogIndex + 1
	to := rf.state.logLen()
	log = log.WithFields(logrus.Fields{
		"appendFrom": from,
		"appendTo":   to,
	})
	rf.state.updateLastIndex(args.Entries...)
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
