package raft

const (
	follower = iota
	candidate
	leader
)

type Identity interface {
	ReplyVote(args *RequestVoteArgs, reply *RequestVoteReply)
	ReplyAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)
}

type Leader struct {
	term int64
}

type Follower struct {
}

type Candidate struct {
}
