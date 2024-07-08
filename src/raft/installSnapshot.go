package raft

type InstallSnapshotReq struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Done              bool
}

type InstallSnapshotResp struct {
	Term int
}

func (l *Leader) replyInstallSnapshot(rf *Raft, args *InstallSnapshotReq) *InstallSnapshotResp {
	myTerm := rf.state.getTerm()
	if args.Term > myTerm {
		rf.state.setTerm(args.Term)
		return l.setState(rf, follower).replyInstallSnapshot(rf, args)
	}
	return &InstallSnapshotResp{Term: myTerm}
}

func (f *Follower) replyInstallSnapshot(rf *Raft, args *InstallSnapshotReq) *InstallSnapshotResp {
	return nil
}

func (c *Candidate) replyInstallSnapshot(rf *Raft, args *InstallSnapshotReq) *InstallSnapshotResp {
	myTerm := rf.state.getTerm()
	if args.Term > myTerm {
		rf.state.setTerm(args.Term)
		return c.setState(rf, follower).replyInstallSnapshot(rf, args)
	}
	return &InstallSnapshotResp{Term: myTerm}
}
