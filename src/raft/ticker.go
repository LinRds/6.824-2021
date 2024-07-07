package raft

import (
	"github.com/sirupsen/logrus"
	"time"
)

const (
	elapse = true
)

func recordElapse(begin time.Time, from string) {
	if elapse {
		logrus.WithField("elapse", time.Since(begin)).Info(from)
	}
}
func start(rf *Raft, cmd *startReq) {
	begin := time.Now()
	//defer recordElapse(begin, "start")
	repCh := cmd.reply
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
	rf.state.vState.lastIndexEachTerm[entry.Term] = entry.Index
	replys := make([]*AppendEntriesReply, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		replys[i] = &AppendEntriesReply{}
		arg := rf.buildAppendArgs(i, "start")
		go func(server int, arg *appendEntriesArg) {
			rf.sendAppendEntries(server, arg, replys[server], nil, "start")
		}(i, arg)
	}
}

func election(rf *Raft, timeOut int64) {
	defer recordElapse(time.Now(), "election")
	if rf.isLeader() {
		return
	}
	if time.Since(rf.lastHeartbeatFromLeader).Milliseconds() > timeOut {
		rf.id.setState(rf, candidate)
		rf.electionOnce(rf.state.getTerm())
	}
}

func handleElection(rf *Raft, res *RequestVoteReply) {
	defer recordElapse(time.Now(), "handleElection")
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
	defer recordElapse(time.Now(), "heartbeat")
	if !rf.isLeader() {
		return
	}
	// appendEntries RPC also can refresh this time
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		arg := rf.buildAppendArgs(i, "heartbeat")
		go func() {
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(i, arg, reply, nil, "heartbeat")
		}()
	}
}

func getState(rf *Raft) {
	defer recordElapse(time.Now(), "getState")
	rf.getStateCh <- &stateRes{
		isLeader: rf.isLeader(),
		term:     rf.state.getTerm(),
	}
}

func replyVote(rf *Raft, req *RequestVoteArgs) {
	defer recordElapse(time.Now(), "replyVote")
	rf.voteRepCh <- rf.id.replyVote(rf, req)
}

func appendEntry(rf *Raft, req *AppendEntriesArgs) {
	defer recordElapse(time.Now(), "appendEntry")
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

func handleAppendEntry(rf *Raft, rep *appendEntryResult) {
	defer recordElapse(time.Now(), "handleAppendEntry")
	if !rf.isLeader() {
		return
	}
	rf.handleAppendEntriesReply(rep)
}
