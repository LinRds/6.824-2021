package raft

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"sync/atomic"
	"time"
)

const (
	minimumIndex = 1
)

func (l *Leader) replyAppendEntries(rf *Raft, args *AppendEntriesArgs) *AppendEntriesReply {
	return l.setState(rf, follower).replyAppendEntries(rf, args)
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
			rf.makeSnapshot()
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
	rf.state.pState.Logs = rf.state.getLogRange(rf.state.getSnapshot().LastIndex+1, args.PrevLogIndex)
	rf.state.logAppend(args.Entries...)
	// from could be bigger than to in fast sync request which entry is nil
	from := args.PrevLogIndex + 1
	to := rf.state.logLen()
	log = log.WithFields(logrus.Fields{
		"appendFrom": from,
		"appendTo":   to,
	})
	return rf.acceptAppendEntries(log)
}

func (c *Candidate) replyAppendEntries(rf *Raft, args *AppendEntriesArgs) *AppendEntriesReply {
	return c.setState(rf, follower).replyAppendEntries(rf, args)
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

func handleSuccess(rf *Raft, reply *appendEntryResult, log *logrus.Entry) bool {
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
		if entry == nil {
			continue
		}
		entry.Count = entry.Count.add(reply.server)
		if rf.isMajority(entry.Count.len()) && entry.Term == rf.state.getTerm() && rf.state.setCommitIndex(i) {
			update = true
		}
	}
	if update {
		rf.updateLogState()
		rf.makeSnapshot()
	}
	log.WithField("new", reply.prevLogIndex+reply.elemLength+1).Info("set next index when success")
	rf.state.setMatchIndex(server, max(rf.state.getMatchIndex(server), reply.prevLogIndex+reply.elemLength))
	return rf.state.setNextIndex(server, reply.prevLogIndex+reply.elemLength+1, true)
}

func setNextIndexWhenFailure(rf *Raft, re *appendEntryResult, log *logrus.Entry) bool {
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
		entry := rf.state.getPrevLogEntry(fastIndex)
		if entry != nil && entry.Term != fastTerm {
			log.Fatalf("expected fast term to be %d, got %d", fastTerm, entry.Term)
		}
	}
	log.WithField("new", fastIndex+1).Warn("set next index when failure")
	return rf.state.setNextIndex(server, fastIndex+1, false)
}

func handleFailure(rf *Raft, reply *appendEntryResult, log *logrus.Entry) bool {
	return setNextIndexWhenFailure(rf, reply, log)
}

type appendEntriesArg struct {
	arg     *AppendEntriesArgs
	version int
}

func (a *appendEntriesArg) send(client *Raft, server int, from string) {
	if a.arg.Entries == nil {
		logrus.WithFields(logrus.Fields{
			"server": server,
			"client": client.me,
			"from":   from,
		}).Warn("nil arg")
	}
	client.sendAppendEntries(server, a, &AppendEntriesReply{}, nil, from)
}

func buildAppendArgs(rf *Raft, server int, from string) logSyncEntry {
	defer recordElapse(time.Now(), "build append args", rf.me)
	if server == rf.me {
		return nil
	}
	nextIndex := rf.state.getNextIndex(server)
	prevIndex := nextIndex - 1
	if prevIndex < 0 {
		logrus.Fatalf("invalid nextIndex: %v", rf.state.vState.nextIndex)
	}
	firstIndex := rf.state.firstLogIndex()
	if prevIndex < firstIndex-1 {
		logrus.WithFields(logrus.Fields{
			"server": rf.me,
			"client": server,
			"from":   from,
		}).Infof("prevIndex %d < firstIndex-1 %d", prevIndex, firstIndex-1)
		snapshot := rf.state.getSnapshot()
		if snapshot.LastIndex != firstIndex-1 {
			logrus.Fatalf("log missing")
			return nil
		}
		return &InstallSnapshotReq{
			Term:              rf.state.getTerm(),
			LeaderId:          rf.me,
			LastIncludedIndex: snapshot.LastIndex,
			LastIncludedTerm:  snapshot.LastTerm,
			Offset:            0,
			Data:              snapshot.byte(),
			Done:              true,
		}
	}
	// nextIndex start from 1
	cpLen := max(0, rf.state.logLen()-prevIndex)
	entries := make([]*LogEntry, cpLen)
	if cpLen > 0 {
		logs := rf.state.getLogRange(nextIndex, -1)
		if len(logs) < cpLen {
			logrus.WithFields(logrus.Fields{
				"firstIndex": firstIndex,

				"purLen":    rf.state.pureLogLen(),
				"len":       rf.state.logLen(),
				"nextIndex": nextIndex,
			}).Warn("log len not enough")
		}
		for i, item := range logs {
			entries[i] = &LogEntry{
				Term:  item.Term,
				Index: item.Index,
				Cmd:   item.Cmd,
			}
		}
	}
	var prevTerm int
	if prevIndex == 0 {
		prevTerm = 0
	} else {
		prevTerm = rf.state.getPrevLogEntry(prevIndex).Term
	}
	arg := &AppendEntriesArgs{
		Term:         rf.state.pState.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.state.vState.commitIndex,
		From:         from,
	}
	return &appendEntriesArg{arg: arg, version: rf.state.version}
}
