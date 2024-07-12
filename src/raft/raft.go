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
	"bytes"
	"github.com/LinRds/raft/labgob"
	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/sirupsen/logrus"
	"log"
	"math/rand"
	_ "net/http/pprof"
	"os"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"github.com/LinRds/raft/labgob"
	"github.com/LinRds/raft/labrpc"
)

func init() {
	//runtime.SetBlockProfileRate(1)
	//go http.ListenAndServe("localhost:6060", nil)
	logrus.SetFormatter(&nested.Formatter{
		FieldsOrder: []string{"server", "me", "client", "candidate", "term", "prevTerm", "prevIndex", "fastTerm", "fastIndex", "appendFrom", "appendTo", "updateFrom", "updateTo", "old", "new"},
		NoColors:    true,
	})
	file, err := os.OpenFile("testlog/test_"+randName()+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	logrus.SetOutput(file)
}

const (
	ElectionTimeout  = 700
	HeartbeatTimeout = 300
	RandRange        = 500
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
	VotedFor int
	Voted    bool
}

type stateRes struct {
	isLeader bool
	term     int
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
	id                      identity
	fol                     *Follower
	led                     *Leader
	cdi                     *Candidate
	currentLeader           atomic.Bool
	lastHeartbeatFromLeader time.Time
	voteHandler             map[atomic.Uint64]func(args *RequestVoteArgs, reply *RequestVoteReply)
	state                   *State
	applyCh                 chan ApplyMsg
	stateCh                 chan struct{}
	getStateCh              chan *stateRes
	electionResCh           chan *RequestVoteReply
	voteReqCh               chan *RequestVoteArgs
	voteRepCh               chan int64
	appendEntriesReqCh      chan *AppendEntriesArgs
	appendEntriesRepCh      chan *AppendEntriesReply
	appendEntryResCh        chan *appendEntryResult
	startReqCh              chan *startReq
	snapshotReqCh           chan *InstallSnapshotReq
	snapshotRepCh           chan *InstallSnapshotResp
}

func (rf *Raft) saveRaceLeader(isLeader bool) {
	rf.currentLeader.Store(isLeader)
}

func (rf *Raft) getRaceLeader() bool {
	return rf.currentLeader.Load()
}

func (rf *Raft) initChan() {
	rf.stateCh = make(chan struct{}, 10)
	rf.getStateCh = make(chan *stateRes, 10)
	rf.electionResCh = make(chan *RequestVoteReply, 10)
	rf.voteReqCh = make(chan *RequestVoteArgs)
	rf.voteRepCh = make(chan int64)
	rf.appendEntriesReqCh = make(chan *AppendEntriesArgs)
	rf.appendEntriesRepCh = make(chan *AppendEntriesReply)
	rf.appendEntryResCh = make(chan *appendEntryResult, 10)
	rf.startReqCh = make(chan *startReq, 10)
	rf.snapshotReqCh = make(chan *InstallSnapshotReq)
	rf.snapshotRepCh = make(chan *InstallSnapshotResp)
}

type startReq struct {
	reply chan *startRes
	cmd   any
}

func (rf *Raft) getId(id int) identity {
	switch id {
	case follower:
		return rf.fol
	case leader:
		return rf.led
	case candidate:
		rf.cdi.voteCount = 1
		return rf.cdi
	}
	return nil
}

func (rf *Raft) isLeader() bool {
	return rf.id.getState() == leader
}

func (rf *Raft) isFollower() bool {
	return rf.id.getState() == follower
}

func (rf *Raft) isCandidate() bool {
	return rf.id.getState() == candidate
}

// update commitIndex, lastApplied and sync to applyCh
func (rf *Raft) updateLogState() {
	logrus.WithFields(logrus.Fields{
		"server": rf.me,
		"from":   rf.state.vState.lastApplied + 1,
		"to":     rf.state.vState.commitIndex,
	}).Info("sync apply")
	for i := rf.state.vState.lastApplied; i < rf.state.vState.commitIndex; i++ {
		if i > rf.state.vState.lastApplied {
			// TODO apply log
		}
		if rf.state.pState.Logs[i] == nil {
			log.Fatalf("raft [%d] log state is nil", rf.me)
		}
		//log.Printf("server %d sync log, cmd is %v, term is %d and index is %d", rf.me, rf.state.pState.Logs[i].Cmd, rf.state.pState.Logs[i].Term, rf.state.pState.Logs[i].Index)
		syncApply(rf.applyCh, rf.state.pState.Logs[i].Cmd, rf.state.pState.Logs[i].Index)
	}
	rf.state.vState.lastApplied = rf.state.vState.commitIndex
}

// LogControllerLoop deprecated
//func (rf *Raft) LogControllerLoop(i int, stopCh <-chan struct{}) {
//	for {
//		select {
//		case <-stopCh:
//			return
//		default:
//			arg := buildAppendArgs(rf, i, "log control loop")
//			// TODO 等到能够确定matchIndex的更新机制后，尝试减少不必要的
//			reply := &AppendEntriesReply{}
//			rf.sendAppendEntries(i, arg, reply, nil, "logController loop")
//			time.Sleep(100 * time.Millisecond)
//		}
//	}
//}

func (rf *Raft) setVote(vote *Vote) {
	versionIncLog("set vote")
	rf.state.version++
	rf.state.pState.Vote = vote
}

func (rf *Raft) getState() *stateRes {
	rf.stateCh <- struct{}{}
	//log.Printf("server %d have state req", rf.me)
	return <-rf.getStateCh
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	state := rf.getState()
	term = state.term
	isleader = state.isLeader
	return term, isleader
}

func (rf *Raft) persistIfVersionMismatch(version int) {
	if rf.state.match(version) {
		return
	}
	rf.persist()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// it should be called before responding to RPCs as the paper say.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	defer recordElapse(time.Now(), "persist", rf.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.state.pState)
	if err != nil {
		log.Fatal(err)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	//log.Printf("server %d success persist, data len is %d", rf.me, len(data))
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var pState PersistentState
	if err := d.Decode(&pState); err != nil {
		log.Fatal(err)
	}
	rf.state.pState = &pState
}

// CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	var snap Snapshot
	dec := labgob.NewDecoder(bytes.NewReader(snapshot))
	if err := dec.Decode(&snap); err != nil {
		return false
	}
	lastTerm, lastIndex := rf.state.lastLogEntry()
	if lastTerm > lastIncludedTerm || lastIndex > lastIncludedIndex {
		return false
	}
	//rf.state.installSnapshot(&snap)
	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	decoder := labgob.NewDecoder(bytes.NewReader(snapshot))
	if err := decoder.Decode(rf.state.getSnapshot()); err != nil {
		logrus.Fatal(err)
		return
	}
	rf.state.pState.Logs = rf.state.getLogRange(index+1, -1)
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
	// Your code here (2B).
	// if not leader, return
	begin := time.Now()
	log := logrus.WithField("cmd", command)
	if !rf.getRaceLeader() {
		log.WithField("wait", time.Now().Sub(begin)).
			WithField("process", time.Now().Sub(begin)).WithField("isLeader", false).Info("start end")
		return -1, -1, false
	}
	log.Info("start begin")
	reply := make(chan *startRes, 1)
	rf.startReqCh <- &startReq{reply: reply, cmd: command}
	rep := <-reply
	log.WithField("wait", rep.begin.Sub(begin)).
		WithField("process", rep.end.Sub(rep.begin)).WithField("isLeader", rep.isLeader).Info("start end")
	return rep.index, rep.term, rep.isLeader
}

type startRes struct {
	index    int
	term     int
	isLeader bool
	begin    time.Time // start to process
	end      time.Time // end process
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

func (rf *Raft) makeSnapshot() {
	if len(rf.state.pState.Logs) < SnapShotInterval {
		return
	}
	firstEntry := rf.state.pState.Logs[0]
	commitIndex := rf.state.getCommitIndex()
	lastCommitEntry := rf.state.getLogEntry(commitIndex)
	if lastCommitEntry.Index-firstEntry.Index < SnapShotInterval {
		return
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(&Snapshot{
		LastIndex: lastCommitEntry.Index,
		LastTerm:  lastCommitEntry.Term,
		Value:     lastCommitEntry.Cmd,
	})
	if err != nil {
		logrus.Fatal("encode snap failed")
	}
	rf.Snapshot(commitIndex, w.Bytes())
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	var timeOut = rand.Int63n(RandRange) + ElectionTimeout
	electionTimeoutTicker := time.NewTicker(time.Duration(timeOut) * time.Millisecond)
	defer electionTimeoutTicker.Stop()
	heartbeatTicker := time.NewTicker(time.Duration(HeartbeatTimeout) * time.Millisecond)
	defer heartbeatTicker.Stop()
	for {
		select {
		case cmd := <-rf.startReqCh:
			start(rf, cmd)
		case <-rf.stateCh:
			getState(rf)
		case <-heartbeatTicker.C:
			heartbeat(rf)
		case <-electionTimeoutTicker.C:
			election(rf, timeOut)
		case voteReq := <-rf.voteReqCh:
			replyVote(rf, voteReq)
		case res := <-rf.electionResCh:
			handleVote(rf, res)
		case appendReq := <-rf.appendEntriesReqCh:
			replyAppendEntry(rf, appendReq)
		case appendRes := <-rf.appendEntryResCh:
			handleAppendEntry(rf, appendRes)
		case snapshotReq := <-rf.snapshotReqCh:
			replyInstallSnapshot(rf, snapshotReq)
		case snapshotResp := <-rf.snapshotRepCh:
			handleInstallSnapshot(rf, snapshotResp)
		default:
			if rf.killed() {
				return
			}
		}
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
	rf.fol = &Follower{make(map[logIndex][]byte)}
	rf.led = &Leader{}
	rf.cdi = &Candidate{}
	rf.id = rf.getId(follower) // initial to follower
	rf.state = new(State)
	rf.state.init()
	rf.initChan()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
