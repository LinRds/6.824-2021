package raft

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"sync/atomic"
)

const (
	minimumIndex = 1
)

type termLog struct {
	Term  int
	Index int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int

	Entries      []*LogEntry
	LeaderCommit int
	From         string // track
}

type AppendEntriesReply struct {
	TermAndSuccess int64
	FastIndex      int
	FastTerm       int
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

func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args == nil {
		return
	}
	rf.appendEntriesReqCh <- args
	tp := <-rf.appendEntriesRepCh
	reply.TermAndSuccess = tp.TermAndSuccess
	reply.FastTerm = tp.FastTerm
	reply.FastIndex = tp.FastIndex
}

func (rf *Raft) refuseAppendEntries(log *logrus.Entry, term int) *AppendEntriesReply {
	reply := new(AppendEntriesReply)
	reply.TermAndSuccess = RpcRefuse(rf.state.getTerm())
	reply.FastTerm, reply.FastIndex = rf.state.fastIndex(term)
	log.WithFields(logrus.Fields{
		"fastTerm":  reply.FastTerm,
		"fastIndex": reply.FastIndex,
		"newTerm":   rf.state.getTerm(),
	}).Warn("append entries refuse")
	return reply
}

func (rf *Raft) acceptAppendEntries(log *logrus.Entry) *AppendEntriesReply {
	reply := new(AppendEntriesReply)
	myTerm := rf.state.getTerm()
	reply.TermAndSuccess = RpcAccept(myTerm)
	log.Info("append entries success")
	return reply
}

func (rf *Raft) sendAppendEntries(server int, args *appendEntriesArg, reply *AppendEntriesReply, result chan *rpcResult, mark string) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args.arg, reply, mark)
	// TODO not ok also should send to result
	if result != nil {
		result <- &rpcResult{ok, server}
	}
	if ok {
		rf.appendEntryResCh <- &appendEntryResult{
			server:       server,
			stateVersion: args.version,
			reply:        reply,
			prevLogIndex: args.arg.PrevLogIndex,
			elemLength:   len(args.arg.Entries),
		}
	}
	return ok
}

type appendEntryResult struct {
	server       int
	stateVersion int
	reply        *AppendEntriesReply
	prevLogIndex int
	elemLength   int
}

func versionNotMatch(v1, v2 int) string {
	return fmt.Sprintf("version not match, expected %d, got %d", v1, v2)
}

func handleSuccess(rf *Raft, reply *appendEntryResult, log *logrus.Entry) {
	server := reply.server
	log = log.WithFields(logrus.Fields{
		"server":    rf.me,
		"client":    server,
		"fastIndex": reply.reply.FastIndex,
		"fastTerm":  reply.reply.FastTerm,
		"old":       rf.state.getNextIndex(server),
	})
	update := false
	for i := reply.prevLogIndex + 1; i <= reply.prevLogIndex+reply.elemLength; i++ {
		entry := rf.state.getLogEntry(i)
		entry.Count = entry.Count.add(reply.server)
		if rf.isMajority(entry.Count.len()) && entry.Term == rf.state.getTerm() && rf.state.setCommitIndex(i) {
			update = true
			rf.updateLogState()
		}
	}
	if update {
		//rf.updateLogState()
	}
	log.WithField("new", reply.prevLogIndex+reply.elemLength+1).Info("set next index when success")
	rf.state.setNextIndex(server, reply.prevLogIndex+reply.elemLength+1, true)
	rf.state.setMatchIndex(server, max(rf.state.getMatchIndex(server), reply.prevLogIndex+reply.elemLength))
}

func setNextIndexWhenFailure(rf *Raft, re *appendEntryResult, log *logrus.Entry) {
	fastTerm, fastIndex := re.reply.FastTerm, re.reply.FastIndex
	if fastIndex < 0 {
		log.Fatalf("fastIndex %d of server %d is negative", fastIndex, re.server)
	}

	server := re.server
	log = log.WithFields(logrus.Fields{
		"server":    rf.me,
		"client":    server,
		"fastIndex": re.reply.FastIndex,
		"fastTerm":  re.reply.FastTerm,
		"old":       rf.state.getNextIndex(server),
	})
	// fastIndex = 0 when fastTerm = -1
	//if fastTerm == rf.state.getTerm() || fastTerm == -1 {
	//	log.WithField("new", fastIndex+1).Warn("set next index when failure")
	//	rf.state.setNextIndex(server, fastIndex+1, false)
	//	return
	//}

	// fastTerm < myTerm
	// clip to avoid fail in TestRejoin2B, as disconnected leader may try to agree on some entries
	// it's un committed log in some term is bigger than leader
	fastIndex = min(fastIndex, rf.state.lastIndexInTerm(fastTerm))
	// leader may not have log in fastTerm
	if fastIndex < 0 {
		fastTerm, fastIndex = rf.state.fastIndex(fastTerm)
		// leader not have any log in term less than fastTerm
		if fastTerm == -1 {
			log.WithFields(logrus.Fields{
				"follower": server,
				"fastTerm": re.reply.FastTerm,
			}).Warn("follower has log which leader not have")
			fastIndex = minimumIndex - 1 // set index 1 less than minimum value
		}
	}
	if fastIndex > 0 {
		// safety validation
		entry := rf.state.getLogEntry(fastIndex)
		if entry.Term != fastTerm {
			log.Fatalf("expected fast term to be %d, got %d", fastTerm, entry.Term)
		}
	}
	log.WithField("new", fastIndex+1).Warn("set next index when failure")
	rf.state.setNextIndex(server, fastIndex+1, false)
}

func handleFailure(rf *Raft, reply *appendEntryResult, log *logrus.Entry) {
	setNextIndexWhenFailure(rf, reply, log)
}
