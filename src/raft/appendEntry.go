package raft

import (
	"fmt"
	"log"
	"sync/atomic"
)

type termLog struct {
	Term  int
	Index int
}

type LogEntry struct {
	Term  int
	Count int
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, result chan *rpcResult, mark string) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply, mark)
	// TODO not ok also should send to result
	if result != nil {
		result <- &rpcResult{ok, server}
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
	// TODO 这里可能有问题，还没有思考什么时候更新version，可能会有version更新但是不该return的情况
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
	if success {
		//log.Printf("term: %d, leader is %d and follower is %d,  success append entry", myTerm, rf.me, server)
		for i := re.prevLogIndex + 1; i <= re.prevLogIndex+re.elemLength; i++ {
			rf.state.pState.Logs[i-1].Count++
			if rf.isMajority(rf.state.pState.Logs[i-1].Count) {
				key := termLog{
					Term:  int(term),
					Index: i,
				}
				if rf.startReplyCh[key] != nil {
					rf.startReplyCh[key] <- &startRes{
						index:    i,
						term:     int(term),
						isLeader: true,
					}
					if rf.state.setCommitIndex(i + 1) {
						syncApply(rf.applyCh, rf.state.pState.Logs[i-1].Cmd, rf.state.pState.Logs[i-1].Index)
					}
					delete(rf.startReplyCh, key)
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
			log.Printf("set nextindex to %d with term %d", re.prevLogIndex, term)
			rf.state.setNextIndex(server, re.prevLogIndex, false)
		}
	}
}
