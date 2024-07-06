package raft

import (
	"github.com/sirupsen/logrus"
	"sync/atomic"
)

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func pack(a, b uint32) uint64 {
	return uint64(a)<<32 | uint64(b)
}

func unPack(num uint64) (uint32, uint32) {
	return uint32(num >> 32), uint32(num & (1<<32 - 1))
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	TermAndGranted int64
}

func (r *RequestVoteReply) Get() (int64, bool) {
	term, g := unPack(uint64(atomic.LoadInt64(&r.TermAndGranted)))
	var granted bool
	if g == 1 {
		granted = true
	}
	return int64(term), granted
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.voteReqCh <- args
	reply.TermAndGranted = <-rf.voteRepCh
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, result chan *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply, "")
	if !ok {
		logrus.WithFields(logrus.Fields{
			"from": rf.me,
			"to":   server,
		}).Info("rpc error")
	}
	if result != nil {
		result <- reply
	}
	return ok
}

type rpcResult struct {
	ok     bool
	server int
}

type electionResult struct {
	success bool
	maxTerm int
}

func (rf *Raft) electionOnce(term int) {
	/*
		1. 发送voteRPC
		2. 获得了多数投票则晋升为leader
		3. 获得了多数否定则退回follower
		4. 获得了任期大于自己的回复退回follower
		5. 超时没有完成选举任期加1，进入下一轮
	*/
	log := logrus.WithField("server", rf.me)
	log.WithField("term", term).Info("start election")
	lt, li := rf.state.lastLogEntry()
	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: li,
		LastLogTerm:  lt,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(i, args, reply, nil)
			rf.electionResCh <- reply
		}(i)
	}
}
