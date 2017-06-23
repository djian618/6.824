package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

func (rf *Raft) NewRequestVoteReply() *RequestVoteReply{
	rf.mu.Lock()
	defer rf.mu.Unlock()	
	msg := &RequestVoteReply{
		Term: -1,
		VoteGranted: false,
	}
	return msg
}

func (rf *Raft) NewRequestVoteArgs() *RequestVoteArgs{
	rf.mu.Lock()
	defer rf.mu.Unlock()	
	msg := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.log.GetLastIndex(),
		LastLogTerm: rf.log.GetCurrentTerm(),
	}
	return msg
}

