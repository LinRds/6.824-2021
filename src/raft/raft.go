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
	ElectionTimeout  = 250
	HeartbeatTimeout = 150
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

func syncApply(applyCh chan ApplyMsg, cmd any, cmdIndex int) {
	applyCh <- ApplyMsg{CommandValid: true, Command: cmd, CommandIndex: cmdIndex}
}

type Vote struct {
	term     int
	votedFor int
	voted    bool
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
	stopLead                chan struct{}
	pState                  *PersistentState
	lastHeartbeatFromLeader atomic.Int64
	lastHeartbeat           atomic.Int64
	voteHandler             map[atomic.Uint64]func(args *RequestVoteArgs, reply *RequestVoteReply)
	state                   *volatileState
	applyCh                 chan ApplyMsg
}

// update commitIndex, lastApplied and sync to applyCh
func (rf *Raft) updateLogState() {
	for i := rf.state.lastApplied; i < rf.state.commitIndex; i++ {
		if i > rf.state.lastApplied {
			// TODO apply log
		}
		if rf.pState.Logs[i] == nil {
			log.Fatalf("raft [%d] log state is nil", rf.me)
		}
		syncApply(rf.applyCh, rf.pState.Logs[i].Cmd, rf.pState.Logs[i].Index)
	}
	rf.state.lastApplied = rf.state.commitIndex
}

func (rf *Raft) become(term, role int) (int, bool) {
	oldTerm, id := rf.pState.get()
	if oldTerm > term {
		return 0, false
	}
	switch id {
	case leader:
		if role != follower {
			return 0, false
		}
		close(rf.stopLead)
	case follower:
		if role == leader {
			return 0, false
		}
	case candidate:
	}

	switch role {
	case leader:
		return term, rf.becomeLeader(term)
	case follower:
		return term, rf.becomeFollower(oldTerm, term)
	case candidate:
		return rf.becomeCandidate()
	}
	return 0, false
}

// change of PersistentState should be atomic
func (rf *Raft) becomeFollower(oldTerm, term int) bool {
	set := rf.pState.set(oldTerm, term)
	return set
}

func (rf *Raft) becomeCandidate() (int, bool) {
	return rf.pState.inc()
}

func (rf *Raft) becomeLeader(term int) bool {
	rf.stopLead = make(chan struct{})
	rf.state.mu.Lock()
	rf.state.init(len(rf.peers), len(rf.pState.Logs)+1)
	rf.state.mu.Unlock()
	go rf.heartbeat(rf.stopLead)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.LogControllerLoop(i, rf.stopLead)
	}
	return rf.pState.elegant(term)
}

func (rf *Raft) LogControllerLoop(i int, stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		default:
			rf.pState.logMu.RLock()
			rf.state.mu.Lock()
			args := rf.buildAppendArgs(i)
			rf.pState.logMu.RUnlock()
			rf.state.mu.Unlock()
			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(i, args, reply, nil) {
				rf.handleAppendEntriesReply(i, args, reply)
			}
			//if len(rf.pState.Logs) != rf.state.nextIndex[i] {
			//} else {
			//	rf.pState.logMu.RUnlock()
			//	rf.state.mu.Unlock()
			//}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) setVote(vote *Vote) bool {
	// old term cannot overwrite new term
	if vote.term <= rf.pState.Vote.term {
		return false
	}
	rf.pState.Vote = vote
	return true
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	var id int
	rf.pState.mu.RLock()
	term, id = rf.pState.get()
	rf.pState.mu.RUnlock()
	isleader = id == leader
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

func (rf *Raft) isMajority(num int) bool {
	return num >= len(rf.peers)>>1+1
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
	// if not leader, return
	term, isLeader = rf.GetState()
	if !isLeader {
		return -1, -1, false
	}
	rf.pState.logMu.Lock()
	defer rf.pState.logMu.Unlock()
	rf.pState.Logs = append(rf.pState.Logs, &LogEntry{
		Term:  term,
		Index: len(rf.pState.Logs) + 1, // index start from 1
		Cmd:   command,
	})
	index = len(rf.pState.Logs)
	ready := make(chan *rpcResult, len(rf.peers))
	prevLogIndex, prevLogTerm := rf.pState.lastLog()
	nl := len(rf.pState.Logs)
	replys := make([]*AppendEntriesReply, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		replys[i] = &AppendEntriesReply{}
		func() {
			rf.state.mu.Lock()
			defer rf.state.mu.Unlock()
			if nl-rf.state.nextIndex[i]+1 < 0 {
				log.Fatalf("nl is %d, nextIndex is %d", nl, rf.state.nextIndex[i])
			} else {
				log.Printf("nl is %d, nextIndex is %d", nl, rf.state.nextIndex[i])
			}
			entries := make([]*LogEntry, nl-rf.state.nextIndex[i]+1)
			copy(entries, rf.pState.Logs[rf.state.nextIndex[i]-1:])
			go rf.sendAppendEntries(i, &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.state.commitIndex,
			}, replys[i], ready)
		}()
	}
	accept := 1
WaitRPC:
	for {
		select {
		case res := <-ready:
			if res.ok {
				_, success := replys[res.server].Get()
				if success {
					accept++
					if rf.isMajority(accept) {
						rf.state.mu.Lock()
						rf.state.commitIndex = index
						rf.updateLogState()
						rf.state.mu.Unlock()
						return index, term, true
					}
				}
			}
		case <-time.After(100 * time.Millisecond):
			break WaitRPC
		}
	}
	// TODO 这里要通过判断服务器已经复制的日志来确定是否达到，同样也要加个超时时间
	begin := time.Now()
	accept = 0
	counted := make(map[int]bool, len(rf.pState.Logs))
	for time.Since(begin) <= 200*time.Millisecond {
		for i := range rf.peers {
			func() {
				rf.state.mu.RLock()
				defer rf.state.mu.RUnlock()
				if !counted[i] && rf.state.matchIndex[i] >= index {
					counted[i] = true
					accept++
				}
			}()
			if rf.isMajority(accept) {
				rf.state.mu.Lock()
				rf.state.commitIndex = index
				rf.updateLogState()
				rf.state.mu.Unlock()
				return index, term, true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
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

func (rf *Raft) heartbeat(stopCh chan struct{}) {
	rf.pState.mu.RLock()
	term, id := rf.pState.get()
	rf.pState.mu.RUnlock()
	if id != leader {
		return
	}
	for {
		select {
		case <-stopCh:
			return
		default:
			since := time.Since(time.UnixMilli(rf.lastHeartbeat.Load()))
			if since >= HeartbeatTimeout {
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					rf.sendAppendEntries(i, &AppendEntriesArgs{
						Term:         term,
						LeaderId:     rf.me,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Entries:      nil,
						LeaderCommit: 0,
					}, &AppendEntriesReply{}, nil)
				}
				rf.lastHeartbeat.Store(time.Now().UnixMilli())
			}
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	sleep := func(duration, randLimit int) int {
		var randN int
		if randLimit > 0 {
			randN = rand.Intn(randLimit)
		}
		time.Sleep(time.Duration(duration+randN) * time.Millisecond)
		return duration + randN
	}
	election := func(term int) {
		rf.pState.mu.Lock()
		defer rf.pState.mu.Unlock()
		newTerm, ok := rf.become(term, candidate)
		// impossible
		if !ok {
			log.Printf("%d fail to become candidate", rf.me)
		}
		if !rf.setVote(&Vote{term: newTerm, voted: true, votedFor: rf.me}) {
			return
		}
		log.Printf("%d start election for term %d", rf.me, newTerm)
		rf.electionWithTimeout(newTerm, ElectionTimeout*time.Millisecond)
	}
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.pState.mu.RLock()
		term, id := rf.pState.get()
		rf.pState.mu.RUnlock()
		switch id {
		case leader:
		case follower:
			oldHeart := rf.lastHeartbeatFromLeader.Load()
			sleep(ElectionTimeout, 500)
			if oldHeart == rf.lastHeartbeatFromLeader.Load() {
				log.Printf("----------> %d check election timeout <------------\n", rf.me)
				election(term)
			}
		case candidate:
			election(term)
		}
		time.Sleep(50 * time.Millisecond)
	}
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.pState = &PersistentState{
		mu:          sync.RWMutex{},
		Value:       follower,
		CurrentTerm: 1,
		Vote:        new(Vote),
		logMu:       sync.RWMutex{},
		Logs:        make([]*LogEntry, 0, 10),
	}
	rf.state = new(volatileState)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
