package raft

import (
	"github.com/sirupsen/logrus"
	"time"
)

const (
	elapse = true
)

func recordElapse(begin time.Time, from string, server int) {
	if elapse {
		logrus.WithField("server", server).WithField("elapse", time.Since(begin)).Info(from)
	}
}
func start(rf *Raft, cmd *startReq) {
	begin := time.Now()
	defer recordElapse(begin, "start", rf.me)
	repCh := cmd.reply
	// double check
	if !rf.isLeader() {
		repCh <- &startRes{index: -1, term: -1, isLeader: false, begin: begin, end: time.Now()}
		return
	}
	log := logrus.WithField("server", rf.me)
	term := rf.state.getTerm()
	version := rf.state.version
	index := rf.state.logLen() + 1
	entry := &LogEntry{
		Term:  rf.state.getTerm(),
		Index: index,
		Cmd:   cmd.cmd,
	}
	entry.Count = entry.Count.add(rf.me)
	log.WithFields(logrus.Fields{
		"term":  rf.state.getTerm(),
		"index": index,
		"cmd":   cmd.cmd,
	}).Info("leader append log in Start")
	rf.state.logAppend(entry)
	rf.persistIfVersionMismatch(version)
	repCh <- &startRes{index: index, term: term, isLeader: true, begin: begin, end: time.Now()}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		arg := buildAppendArgs(rf, i, "start")
		go func(server int, arg logSyncEntry) {
			arg.send(rf, server, "start")
		}(i, arg)
	}
}

func election(rf *Raft, timeOut int64) {
	if rf.isLeader() {
		return
	}
	if time.Since(rf.lastHeartbeatFromLeader).Milliseconds() > timeOut {
		rf.id.setState(rf, candidate)
		rf.electionOnce(rf.state.getTerm())
	}
}

func handleVote(rf *Raft, res *RequestVoteReply) {
	defer recordElapse(time.Now(), "handleVote", rf.me)
	if !rf.isCandidate() {
		return
	}
	term, success := res.Get()
	if int(term) > rf.state.getTerm() {
		rf.state.setTerm(int(term))
		rf.id.setState(rf, follower)
	} else if success && int(term) == rf.state.getTerm() {
		if rf.isMajority(rf.id.(*Candidate).incVoteCount()) {
			rf.id.setState(rf, leader)
		}
	}
}

func heartbeat(rf *Raft) {
	if !rf.isLeader() {
		return
	}
	//defer recordElapse(time.Now(), "heartbeat", rf.me)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		arg := buildAppendArgs(rf, i, "heartbeat")
		if arg == nil {
			logrus.WithFields(logrus.Fields{
				"server": rf.me,
				"client": i,
			}).Warn("nil arg")
		}
		from := "heartbeat"
		_, ok := arg.(*InstallSnapshotReq)
		if ok {
			from += "-snapshot"
		}
		_, ok = arg.(*appendEntriesArg)
		if ok {
			from += "-appendEntry"
		}
		logrus.WithFields(logrus.Fields{
			"server": rf.me,
			"client": i,
		}).Info(from)
		go func(arg logSyncEntry) {
			arg.send(rf, i, from)
		}(arg)
	}
}

func getState(rf *Raft) {
	defer recordElapse(time.Now(), "getState", rf.me)
	rf.getStateCh <- &stateRes{
		isLeader: rf.isLeader(),
		term:     rf.state.getTerm(),
	}
}

func replyVote(rf *Raft, req *RequestVoteArgs) {
	defer recordElapse(time.Now(), "replyVote", rf.me)
	rf.voteRepCh <- rf.id.replyVote(rf, req)
}

func replyAppendEntry(rf *Raft, req *AppendEntriesArgs) {
	defer recordElapse(time.Now(), "replyAppendEntry", rf.me)
	term := rf.state.getTerm()
	if term > req.Term {
		log := logrus.WithFields(logrus.Fields{
			"server": rf.me,
			"client": req.LeaderId,
			"from":   req.From,
		})
		rf.appendEntriesRepCh <- rf.refuseAppendEntries(log.WithField("reason", "term too low"), req.PrevLogTerm)
		return
	}
	rf.lastHeartbeatFromLeader = time.Now()
	rf.appendEntriesRepCh <- rf.id.replyAppendEntries(rf, req)
}

func handleAppendEntry(rf *Raft, re *appendEntryResult) {
	defer recordElapse(time.Now(), "handleAppendEntry", rf.me)
	if !rf.isLeader() {
		return
	}
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
	if re.prevLogIndex != rf.state.getNextIndex(re.server)-1 {
		log.Warn("prevLogIndex not match")
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
	var set bool
	if success {
		set = handleSuccess(rf, re, log)
	} else {
		set = handleFailure(rf, re, log)
	}

	// fast sync
	// TODO avoid requests not necessary
	if set {
		arg := buildAppendArgs(rf, re.server, "fast sync")
		go arg.send(rf, re.server, "fast sync")
	}
}

func replyInstallSnapshot(rf *Raft, req *InstallSnapshotReq) {
	myTerm := rf.state.getTerm()
	log := logrus.WithFields(logrus.Fields{
		"server":    rf.me,
		"client":    req.LeaderId,
		"myTerm":    myTerm,
		"otherTerm": req.Term,
	})
	if req.Term < myTerm {
		log.Warn("replyInstallSnapshot: term too low")
		rf.snapshotRepCh <- commonReply(rf.me, myTerm, -1)
		return
	}
	rf.lastHeartbeatFromLeader = time.Now()
	rf.snapshotRepCh <- rf.id.replyInstallSnapshot(rf, req)
}

func handleInstallSnapshot(rf *Raft, resp *InstallSnapshotResp) {
	if resp.Term > rf.state.getTerm() {
		rf.state.setTerm(resp.Term)
		rf.id.setState(rf, follower)
		return
	}
	if !rf.isLeader() {
		return
	}
	var set bool
	if resp.LastIndex != -1 {
		logrus.WithFields(logrus.Fields{
			"server": rf.me,
			"client": resp.Server,
			"new":    resp.LastIndex + 1,
		}).Infof("set next index when snapshot")
		set = rf.state.setNextIndex(resp.Server, resp.LastIndex+1, true)
	}
	if set {
		arg := buildAppendArgs(rf, resp.Server, "fast sync(snapshot)")
		go arg.send(rf, resp.Server, "fast sync(snapshot)")
	}
}

func condInstallSnapshot(rf *Raft, snapshot *Snapshot) bool {
	lastIncludedTerm := snapshot.LastTerm
	lastIncludedIndex := snapshot.LastIndex
	curSnapshot := rf.state.getSnapshot()
	return lastIncludedTerm == curSnapshot.LastTerm && lastIncludedIndex == curSnapshot.LastIndex
}
func installSnapshot(rf *Raft, snapshot *Snapshot) {
	logrus.WithFields(logrus.Fields{
		"server":            rf.me,
		"lastIncludedIndex": snapshot.LastIndex,
		"logLen":            rf.state.logLen(),
	}).Info("install snapshot")
	lastIncludedIndex := snapshot.LastIndex
	entry := rf.state.getLogEntry(lastIncludedIndex)
	if snapshot.LastTerm == 0 && entry != nil {
		snapshot.LastTerm = entry.Term
	}
	if snapshot.LastTerm == 0 {
		panic("snapshot's term can't be zero")
	}

	rf.state.installSnapshot(rf, snapshot)
}
