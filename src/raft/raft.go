package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	ElectionTimeout  = 2000
	HeartbeatTimeout = 50
)

const (
	follower = iota
	candidate
	leader
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type vote struct {
	term     int
	votedFor int
	voted    bool
}

type term struct {
	mu    sync.RWMutex
	value int
}

// if identity changed, term must changed exclude candidate timeout
func (t *term) inc(id *identity) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.value++
	id.mu.Lock()
	defer id.mu.Unlock()
	id.value = candidate
}

func (t *term) set(val int, id *identity) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.value = val
	id.mu.Lock()
	defer id.mu.Unlock()
	id.value = follower
}

func (t *term) get() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.value
}

type identity struct {
	mu    sync.RWMutex
	value int
	term  *term
}

func (id *identity) inc() (int, bool) {
	id.term.mu.Lock()
	defer id.term.mu.Unlock()
	id.term.value++
	id.mu.Lock()
	defer id.mu.Unlock()
	if id.value == leader {
		return 0, false
	}
	id.value = candidate
	return id.term.value, true
}

func (id *identity) set(val int) bool {
	id.term.mu.Lock()
	defer id.term.mu.Unlock()
	if id.term.value > val {
		return false
	}
	id.term.value = val
	id.mu.Lock()
	defer id.mu.Unlock()
	id.value = follower
	return true
}

func (id *identity) elegant(term int) bool {
	id.term.mu.RLock()
	defer id.term.mu.RUnlock()
	id.mu.Lock()
	defer id.mu.Unlock()
	if term != id.term.value || id.value != candidate {
		return false
	}
	id.value = leader
	return true
}

func (id *identity) next() {
	id.mu.Lock()
	defer id.mu.Unlock()
	switch id.value {
	case follower:
		id.value = candidate
	case candidate:
		id.value = leader
	case leader:
		id.value = follower
	}
}

func (id *identity) get() int {
	id.term.mu.RLock()
	defer id.term.mu.RUnlock()
	id.mu.RLock()
	defer id.mu.RUnlock()
	return id.value
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect  access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//currentTerm             *term
	identity                *identity
	lastHeartbeatFromLeader atomic.Int64
	vote                    *vote
	voteHandler             map[atomic.Uint64]func(args *RequestVoteArgs, reply *RequestVoteReply)
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.identity.term.get()
	isleader = rf.identity.get() == leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64
	VoteGranted int64
}

func (r *RequestVoteReply) Set(term int64, granted bool) {
	atomic.StoreInt64(&r.Term, term)
	var g int64
	if granted {
		g = 1
	}
	atomic.StoreInt64(&r.VoteGranted, g)
}

func (r *RequestVoteReply) Get() (int64, bool) {
	term := atomic.LoadInt64(&r.Term)
	g := atomic.LoadInt64(&r.VoteGranted)
	var granted bool
	if g == 1 {
		granted = true
	}
	return term, granted
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args == nil {
		return
	}
	var newTerm int
	switch rf.identity.get() {
	case leader:
		newTerm = rf.requestVoteReplyAsLeader(args, reply)
	case follower:
		newTerm = rf.requestVoteAsFollower(args, reply)
	case candidate:
		newTerm = rf.requestVoteAsCandidate(args, reply)
	}
	if newTerm != 0 {
		rf.identity.set(newTerm)
	}
}

func (rf *Raft) requestVoteReplyAsLeader(args *RequestVoteArgs, reply *RequestVoteReply) int {
	myTerm := int64(rf.identity.term.get())
	var (
		granted bool
		newTerm int
	)
	if myTerm < int64(args.Term) {
		granted = true
		newTerm = args.Term
	}
	reply.Set(myTerm, granted)
	return newTerm
}

func (rf *Raft) requestVoteAsFollower(args *RequestVoteArgs, reply *RequestVoteReply) int {
	myTerm := rf.identity.term.get()
	var (
		granted bool
		newTerm int
	)
	// voteGranted is false
	// although vote also has problem of data race, lock of currentTerm is enough
	if myTerm >= args.Term || (rf.vote.term == args.Term && rf.vote.voted) {
		newTerm = 0
	} else {
		rf.vote.voted = true
		rf.vote.votedFor = args.CandidateId
		rf.vote.term = args.Term
		granted = true
		newTerm = args.Term
	}
	reply.Set(int64(myTerm), granted)
	return newTerm
}

func (rf *Raft) requestVoteAsCandidate(args *RequestVoteArgs, reply *RequestVoteReply) int {
	myTerm := int64(rf.identity.term.get())
	var (
		granted bool
		newTerm int
	)
	if myTerm < int64(args.Term) {
		granted = true
		newTerm = args.Term
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		log.Println("rpc error")
	}
	return ok
}

type AppendEntriesArgs struct {
}

type AppendEntriesReply struct {
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	sleep := func(duration, randLimit int) {
		randN := rand.Intn(randLimit)
		time.Sleep(time.Duration(ElectionTimeout+randN) * time.Millisecond)
	}
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		switch rf.identity.get() {
		case leader:
			sleep(HeartbeatTimeout, 200)
		case follower:
			oldHeart := rf.lastHeartbeatFromLeader.Load()
			sleep(ElectionTimeout, 500)
			if oldHeart != rf.lastHeartbeatFromLeader.Load() {
				continue
			}
		ElectionLoop:
			for {
				time.Sleep(50 * time.Millisecond)
				term, ok := rf.identity.inc()
				if !ok {
					log.Println("failed to inc term")
					break ElectionLoop
				}
				select {
				case <-time.After(ElectionTimeout * time.Millisecond):
					log.Println(rf.me, "election timeout")
				case res := <-rf.electionOnce(term):
					if res.success {
						chd := rf.identity.elegant(res.maxTerm)
						if !chd {
							log.Println(rf.me, "win the election, but term has changed")
						} else {
							log.Printf("node %d is leader now", rf.me)
						}
					} else {
						log.Printf("%d election faild, newTerm is %d", rf.me, res.maxTerm)
						rf.identity.set(res.maxTerm)
					}
					break ElectionLoop
				}
			}
		case candidate:
			log.Printf("invalid identity in ticker")
		}
	}
}

type electionResult struct {
	success bool
	maxTerm int
}

func (rf *Raft) electionOnce(term int) <-chan *electionResult {
	/*
		1. 发送voteRPC
		2. 获得了多数投票则晋升为leader
		3. 获得了多数否定则退回follower
		4. 获得了任期大于自己的回复退回follower
		5. 超时没有完成选举任期加1，进入下一轮
	*/
	log.Println(rf.me, "start election")
	res := make(chan *electionResult)
	go func() {
		rf.vote = &vote{
			term:     term,
			votedFor: rf.me,
			voted:    true,
		}
		args := &RequestVoteArgs{
			Term:        term,
			CandidateId: rf.me,
		}
		rps := make([]RequestVoteReply, len(rf.peers))
		for i := range rps {
			if i == rf.me {
				rps[rf.me].Set(int64(term), true)
			} else {
				rps[i] = RequestVoteReply{}
			}
		}
		success := 0
		failed := 0
		majority := len(rf.peers)/2 + 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.sendRequestVote(i, args, &rps[i])
		}
		checked := make([]bool, len(rf.peers))
		maxTerm := term
		for {
			for i := range rps {
				rt, rg := rps[i].Get()
				if rt == 0 || checked[i] {
					continue
				}
				checked[i] = true
				if rt > int64(maxTerm) {
					maxTerm = int(rt)
				}
				if rg {
					success++
				} else {
					failed++
				}
			}
			// this sleep is very important
			// other go routine will have no chance to run without it (maybe)
			time.Sleep(5 * time.Millisecond)
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

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.identity = &identity{
		mu:    sync.RWMutex{},
		value: follower,
		term: &term{
			mu:    sync.RWMutex{},
			value: 1,
		},
	}
	rf.vote = &vote{}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
