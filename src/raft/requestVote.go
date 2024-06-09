package raft

import (
	"log"
	"sync/atomic"
	"time"
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

func (r *RequestVoteReply) Set(term int64, granted bool) {
	var g uint32
	if granted {
		g = 1
	}
	packed := pack(uint32(term), g)
	atomic.StoreInt64(&r.TermAndGranted, int64(packed))
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
	if args == nil {
		return
	}
	var newTerm int
	rf.identity.mu.Lock()
	defer rf.identity.mu.Unlock()
	myTerm, id := rf.identity.get()
	switch id {
	case leader:
		newTerm = rf.requestVoteReplyAsLeader(args, reply, int64(myTerm))
	case follower:
		newTerm = rf.requestVoteAsFollower(args, reply, int64(myTerm))
	case candidate:
		newTerm = rf.requestVoteAsCandidate(args, reply, int64(myTerm))
	}
	if newTerm != 0 {
		//rf.identity.set(newTerm)
	}
}

// term of vote only can less or equal (not bigger) than term of raft
func (rf *Raft) requestVoteReplyAsLeader(args *RequestVoteArgs, reply *RequestVoteReply, myTerm int64) int {
	var (
		granted bool
		newTerm int
	)
	if myTerm < int64(args.Term) {
		rf.state.stepDown()
		return rf.requestVoteAsFollower(args, reply, myTerm)
	}
	reply.Set(myTerm, granted)
	return newTerm
}

func (rf *Raft) requestVoteAsFollower(args *RequestVoteArgs, reply *RequestVoteReply, myTerm int64) int {
	var (
		granted bool
		newTerm int
	)
	// voteGranted is false
	// although vote also has problem of data race, lock of currentTerm is enough
	if int(myTerm) >= args.Term || (rf.identity.vote.term == args.Term && rf.identity.vote.voted) {
		newTerm = 0
	} else {
		// setVote may fail after get the lock
		if rf.setVote(&vote{term: args.Term, voted: true, votedFor: args.CandidateId}) {
			// reset election timeout as the Fig.2 in paper
			rf.lastHeartbeatFromLeader.Store(time.Now().UnixMilli())
			granted = true
			newTerm = args.Term
		}
	}
	reply.Set(myTerm, granted)
	if newTerm != 0 {
		rf.become(newTerm, follower)
		//rf.identity.set(int(myTerm), newTerm)
	}
	return newTerm
}

func (rf *Raft) requestVoteAsCandidate(args *RequestVoteArgs, reply *RequestVoteReply, myTerm int64) int {
	var (
		granted bool
		newTerm int
	)
	if myTerm < int64(args.Term) {
		return rf.requestVoteAsFollower(args, reply, myTerm)
	}
	reply.Set(myTerm, granted)
	return newTerm
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, result chan int) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		log.Printf("%d->%d:rpc error", rf.me, server)
	} else {
		result <- server
	}
	return ok
}

type electionResult struct {
	success bool
	maxTerm int
}

func (rf *Raft) electionWithTimeout(term int, timeout time.Duration) {
	select {
	case <-time.After(timeout):
		log.Println(rf.me, "election timeout")
		return
	case res := <-rf.electionOnce(term):
		if res.success {
			_, chd := rf.become(res.maxTerm, leader)
			if !chd {
				log.Println(rf.me, "win the election, but term has changed")
			} else {
				log.Printf("node %d is leader now", rf.me)
				//for i := range rf.peers {
				//	if i != rf.me {
				//		go rf.heartbeat(i)
				//	}
				//}
			}
		} else {
			log.Printf("%d election faild, newTerm is %d", rf.me, res.maxTerm)
			if res.maxTerm > 0 {
				rf.become(res.maxTerm, follower)
			}
		}
	}
}

func (rf *Raft) electionOnce(term int) <-chan *electionResult {
	/*
		1. 发送voteRPC
		2. 获得了多数投票则晋升为leader
		3. 获得了多数否定则退回follower
		4. 获得了任期大于自己的回复退回follower
		5. 超时没有完成选举任期加1，进入下一轮
	*/
	res := make(chan *electionResult)
	ready := make(chan int, len(rf.peers))
	go func() {
		args := &RequestVoteArgs{
			Term:        term,
			CandidateId: rf.me,
		}
		rps := make([]RequestVoteReply, len(rf.peers))
		for i := range rps {
			if i != rf.me {
				rps[i] = RequestVoteReply{}
			}
		}
		success := 1
		failed := 0
		majority := len(rf.peers)/2 + 1
		for i := range rf.peers {
			if i != rf.me {
				go rf.sendRequestVote(i, args, &rps[i], ready)
			}
		}
		checked := make([]bool, len(rf.peers))
		maxTerm := term
		for i := range ready {
			// impossible theoretically
			if checked[i] {
				continue
			}
			checked[i] = true
			rt, rg := rps[i].Get()
			if rt > int64(maxTerm) {
				maxTerm = int(rt)
			}
			if rg {
				success++
			} else {
				failed++
			}
			if success >= majority || failed >= majority {
				break
			}
		}
		if maxTerm > term || failed >= majority {
			res <- &electionResult{
				success: false,
				maxTerm: maxTerm,
			}
		} else {
			res <- &electionResult{
				success: true,
				maxTerm: maxTerm,
			}
		}
	}()
	return res
}
