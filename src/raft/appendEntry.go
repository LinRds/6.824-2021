package raft

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"math/bits"
	"sync/atomic"
)

const (
	minimumIndex = 1
)

type termLog struct {
	Term  int
	Index int
}

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
	reply.TermAndSuccess = RpcRefuse(term)
	reply.FastTerm, reply.FastIndex = rf.state.vState.fastIndex(term)
	log.WithFields(logrus.Fields{
		"fastTerm":  reply.FastTerm,
		"fastIndex": reply.FastIndex,
		"newTerm":   rf.state.getTerm(),
	}).Warn("append entries refuse")
	return reply
}

func (rf *Raft) acceptAppendEntries(log *logrus.Entry, term int) *AppendEntriesReply {
	reply := new(AppendEntriesReply)
	reply.TermAndSuccess = RpcAccept(term)
	log.WithField("term", rf.state.getTerm()).Info("append entries success")
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

func handleSuccess(rf *Raft, reply *appendEntryResult) {
	//log.Printf("term: %d, leader is %d and follower is %d,  success append entry", myTerm, rf.me, server)
	server := reply.server
	for i := reply.prevLogIndex + 1; i <= reply.prevLogIndex+reply.elemLength; i++ {
		entry := rf.state.getLogEntry(i - 1)
		entry.Count = entry.Count.add(reply.server)
		if rf.isMajority(entry.Count.len()) && entry.Term == rf.state.getTerm() && rf.state.setCommitIndex(i) {
			rf.updateLogState()
		}
	}
	rf.state.setNextIndex(server, reply.prevLogIndex+reply.elemLength+1, true)
	rf.state.setMatchIndex(server, max(rf.state.getMatchIndex(server), reply.prevLogIndex+reply.elemLength))
}

func setNextIndexWhenFailure(rf *Raft, re *appendEntryResult, log *logrus.Entry) {
	fastTerm, fastIndex := re.reply.FastTerm, re.reply.FastIndex
	if fastIndex < 0 {
		log.Fatalf("fastIndex %d of server %d is negative", fastIndex, re.server)
	}

	server := re.server
	// fastIndex = 0 when fastTerm = -1
	if fastTerm == rf.state.getTerm() || fastTerm == -1 {
		log.Infof("set server %d next index to %d", server, fastIndex+1)
		rf.state.setNextIndex(server, fastIndex+1, false)
		return
	}

	// fastTerm < myTerm
	// clip to avoid fail in TestRejoin2B, as disconnected leader may try to agree on some entries
	// it's un committed log in some term is bigger than leader
	fastIndex = min(fastIndex, rf.state.vState.lastIndexInTerm(fastTerm))
	// leader may not have log in fastTerm
	if fastIndex < 0 {
		fastTerm, fastIndex = rf.state.vState.fastIndex(fastTerm)
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
		entry := rf.state.getLogEntry(fastIndex - 1)
		if entry.Term != fastTerm {
			log.Fatalf("expected fast term to be %d, got %d", fastTerm, entry.Term)
		}
	}
	log.Infof("set server %d next index to %d", server, fastIndex+1)
	rf.state.setNextIndex(server, fastIndex+1, false)
}

func handleFailure(rf *Raft, reply *appendEntryResult, log *logrus.Entry) {
	setNextIndexWhenFailure(rf, reply, log)
}

func (rf *Raft) handleAppendEntriesReply(re *appendEntryResult) {
	// There might be a case where returning is not necessary,
	// which is when the version change is caused by logAppend rather than term or vote.
	// However, in this case, subsequent heartbeats or new appendEntriesReq can still achieve log synchronization,
	// so returning here is not wrong.
	//if !rf.state.match(re.stateVersion) {
	//	log.Printf(versionNotMatch(rf.state.version, re.stateVersion))
	//	return
	//}
	log := logrus.WithField("server", rf.me)
	myTerm := rf.state.getTerm()
	term, success := re.reply.Get()
	if term == 0 {
		log.Warn("term is 0")
		return
	}
	if int(term) > myTerm {
		rf.state.setTerm(int(term))
		rf.id.setState(rf, follower)
		return
	}
	if success && int(term) != myTerm {
		log.Warn("receive reply from old term")
		return
	}
	if success {
		handleSuccess(rf, re)
	} else {
		handleFailure(rf, re, log)
	}

	// fast sync
	// TODO avoid requests not necessary
	arg := rf.buildAppendArgs(re.server)
	go rf.sendAppendEntries(re.server, arg, &AppendEntriesReply{}, nil, "handleAppendEntry")
}
