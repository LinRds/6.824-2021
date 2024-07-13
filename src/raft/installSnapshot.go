package raft

import (
	"bytes"
	"github.com/sirupsen/logrus"
)

type InstallSnapshotReq struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

func (i *InstallSnapshotReq) send(client *Raft, server int, from string) {
	client.sendInstallSnapshot(server, i, &InstallSnapshotResp{})
}

type InstallSnapshotResp struct {
	Server    int
	Term      int
	LastIndex int
}

func commonReply(server int, term int, lastIndex int) *InstallSnapshotResp {
	return &InstallSnapshotResp{server, term, lastIndex}
}

func (l *Leader) replyInstallSnapshot(rf *Raft, args *InstallSnapshotReq) *InstallSnapshotResp {
	myTerm := rf.state.getTerm()
	if args.Term >= myTerm {
		rf.state.setTerm(args.Term)
		return l.setState(rf, follower).replyInstallSnapshot(rf, args)
	}
	return commonReply(rf.me, myTerm, -1)
}

func (f *Follower) replyInstallSnapshot(rf *Raft, args *InstallSnapshotReq) *InstallSnapshotResp {
	log := logrus.WithFields(logrus.Fields{
		"server": rf.me,
		"client": args.LeaderId,
	})
	log.Info("reply install snapshot")
	index := logIndex{args.LastIncludedTerm, args.LastIncludedIndex}
	tmpSnap, ok := f.tmpSnapshot[index]
	if !ok && args.Offset != 0 {
		log.Fatal("prev chunk missing or offset is wrong")
	}
	chunkSize := len(tmpSnap)
	if chunkSize != args.Offset {
		log.Fatalf("chunk size mismatch, expected %d got %d", args.Offset, chunkSize)
	}
	if tmpSnap == nil {
		tmpSnap = make([]byte, 0)
	}
	buf := bytes.NewBuffer(tmpSnap)
	buf.Grow(len(args.Data))
	buf.Write(args.Data)
	tmpSnap = buf.Bytes()
	myTerm := rf.state.getTerm()
	lastIndex := -1
	if args.Done {
		if rf.CondInstallSnapshot(args.LastIncludedTerm, args.LastIncludedIndex, tmpSnap) {
			rf.Snapshot(args.LastIncludedIndex, tmpSnap)
			lastIndex = args.LastIncludedIndex
		}
		delete(f.tmpSnapshot, index)
	} else {
		f.tmpSnapshot[index] = tmpSnap
	}
	return commonReply(rf.me, myTerm, lastIndex)
}

func (c *Candidate) replyInstallSnapshot(rf *Raft, args *InstallSnapshotReq) *InstallSnapshotResp {
	myTerm := rf.state.getTerm()
	if args.Term > myTerm {
		rf.state.setTerm(args.Term)
		return c.setState(rf, follower).replyInstallSnapshot(rf, args)
	}
	return commonReply(rf.me, myTerm, -1)
}

func (rf *Raft) RequestInstallSnapshot(args *InstallSnapshotReq, reply *InstallSnapshotResp) {
	if args == nil {
		return
	}
	rf.snapshotReqCh <- args
	rep := <-rf.snapshotRepCh
	reply.Term = rep.Term
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotReq, reply *InstallSnapshotResp) bool {
	ok := rf.peers[server].Call("Raft.RequestInstallSnapshot", args, reply, "")
	if ok {
		rf.snapshotRepCh <- reply
	}
	return ok
}
