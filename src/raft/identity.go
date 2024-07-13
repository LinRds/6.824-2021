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
	replyInstallSnapshot(rf *Raft, args *InstallSnapshotReq) *InstallSnapshotResp
	setState(rf *Raft, id int) identity
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
	return l.setState(rf, follower).replyVote(rf, args)
}

func (l *Leader) setState(rf *Raft, id int) identity {
	if id != follower {
		logrus.Fatal("leader only can trans to follower")
	}
	l.leavingOffice(rf)
	rf.id = rf.getId(follower)
	return rf.id
}

func (l *Leader) getState() int {
	return leader
}

type logIndex struct {
	term  int
	index int
}
type Follower struct {
	tmpSnapshot map[logIndex][]byte
}

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
		logrus.WithFields(logrus.Fields{
			"server":    rf.me,
			"prevIndex": prevIndex,
			"pureLen":   rf.state.pureLogLen(),
			"len":       rf.state.logLen(),
		}).Warn("log too short")
		return errLogTooShort
	}
	// reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	prevItem := rf.state.getPrevLogEntry(prevIndex)
	if prevItem == nil || prevItem.Term != prevTerm {
		return errLogNotMatch
	}
	return nil
}

func (f *Follower) setState(rf *Raft, id int) identity {
	if id == leader {
		logrus.Fatal("follower can not trans to leader directly")
	}
	if id == candidate {
		rf.state.incTerm()
		rf.state.voteForSelf(rf.me)
	}
	rf.id = rf.getId(id)
	return rf.id
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
	return c.setState(rf, follower).replyVote(rf, args)
}

func (c *Candidate) setState(rf *Raft, id int) identity {
	rf.id = rf.getId(id)
	if id == leader {
		rf.id.(*Leader).takingOffice(rf)
		rf.saveRaceLeader(true)
	}
	if id == candidate {
		rf.state.incTerm()
		rf.state.voteForSelf(rf.me)
	}
	return rf.id
}

func (c *Candidate) getState() int {
	return candidate
}
