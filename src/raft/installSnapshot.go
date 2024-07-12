package raft

import (
	"bytes"
	"log"
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
	Term int
}

func commonReply(term int) *InstallSnapshotResp {
	return &InstallSnapshotResp{term}
}

func (l *Leader) replyInstallSnapshot(rf *Raft, args *InstallSnapshotReq) *InstallSnapshotResp {
	myTerm := rf.state.getTerm()
	if args.Term > myTerm {
		rf.state.setTerm(args.Term)
		return l.setState(rf, follower).replyInstallSnapshot(rf, args)
	}
	return commonReply(myTerm)
}

func (f *Follower) replyInstallSnapshot(rf *Raft, args *InstallSnapshotReq) *InstallSnapshotResp {
	index := logIndex{args.LastIncludedTerm, args.LastIncludedIndex}
	tmpSnap, ok := f.tmpSnapshot[index]
	if !ok && args.Offset != 0 {
		log.Fatal("prev chunk missing or offset is wrong")
	}
	chunkSize := len(tmpSnap)
	if chunkSize != args.Offset {
		log.Fatal("chunk size mismatch")
	}
	if tmpSnap == nil {
		tmpSnap = make([]byte, 0)
	}
	buf := bytes.NewBuffer(tmpSnap)
	buf.Grow(len(args.Data))
	buf.Write(args.Data)
	tmpSnap = buf.Bytes()
	myTerm := rf.state.getTerm()
	if args.Done && rf.CondInstallSnapshot(args.LastIncludedTerm, args.LastIncludedIndex, tmpSnap) {
		rf.Snapshot(args.LastIncludedIndex, tmpSnap)
		delete(f.tmpSnapshot, index)
	}
	return commonReply(myTerm)
}

func (c *Candidate) replyInstallSnapshot(rf *Raft, args *InstallSnapshotReq) *InstallSnapshotResp {
	myTerm := rf.state.getTerm()
	if args.Term > myTerm {
		rf.state.setTerm(args.Term)
		return c.setState(rf, follower).replyInstallSnapshot(rf, args)
	}
	return commonReply(myTerm)
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
	ok := rf.peers[server].Call("Raft.", args, reply, "")
	if ok {
		rf.snapshotRepCh <- reply
	}
	return ok
}
