package raft

import (
	"fmt"
	"log"
	"math/bits"
	"sync/atomic"
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
	reply.TermAndSuccess = <-rf.appendEntriesRepCh
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

func (rf *Raft) handleAppendEntriesReply(re *appendEntryResult) {
	server := re.server
	// 这里可能存在一种不应该返回的情况，就是version的改变是由logAppend引起的，而非term或者vote
	// 但是这种情况下后续的心跳或者新的appendEntriesReq仍然可以达到日志同步的目的，所以这里返回
	// 也不为错.
	if !rf.state.match(re.stateVersion) {
		log.Printf(versionNotMatch(rf.state.version, re.stateVersion))
		return
	}
	myTerm := rf.state.getTerm()
	term, success := re.reply.Get()
	if term == 0 {
		log.Println("term is 0")
		return
	}
	if success && int(term) != myTerm {
		log.Println("receive reply from old term")
		return
	}
	if success {
		//log.Printf("term: %d, leader is %d and follower is %d,  success append entry", myTerm, rf.me, server)
		for i := re.prevLogIndex + 1; i <= re.prevLogIndex+re.elemLength; i++ {
			entry := rf.state.getLogEntry(i - 1)
			entry.Count = entry.Count.add(re.server)
			if rf.isMajority(entry.Count.len()) {
				if rf.state.setCommitIndex(i) {
					rf.updateLogState()
				}
			}
		}
		rf.state.setNextIndex(server, re.prevLogIndex+re.elemLength+1, true)
		rf.state.setMatchIndex(server, max(rf.state.getMatchIndex(server), re.prevLogIndex+re.elemLength))
	} else {
		if int(term) > myTerm {
			rf.state.setTerm(int(term))
			rf.id.setState(rf, follower)
		} else {
			log.Printf("set nextindex to %d of %d with term %d\n", re.prevLogIndex, re.server, term)
			rf.state.setNextIndex(server, re.prevLogIndex, false)
			arg := rf.buildAppendArgs(re.server)

			go rf.sendAppendEntries(re.server, arg, &AppendEntriesReply{}, nil, "handleAppendEntry")
		}
	}
}
