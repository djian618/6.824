package raft
// the AppendEntries strcut
type AppendEntriesArgs struct {
	Term       int
	LeaderId   int
	
	PrevLogIndex int
	PrevLogTerm int 
	Entries[] LogEntry
	LeaderCommit int
}

type AppendEntriesRply struct {
	Term       int
	Success    bool
}

//create emty reply msg 
func (rf *Raft) NewAppendEntriesRly() *AppendEntriesRply{
	msg := &AppendEntriesRply{
		Term: -1,
		Success: false,
	}
	return msg
}


//create hearbeat msg for peerId
func (rf *Raft) NewHeartBeatMsg(index int) *AppendEntriesArgs{
	rf.mu.Lock()
	defer rf.mu.Unlock()	
	msg := &AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		
		PrevLogIndex: index,
		PrevLogTerm: rf.log.GetTermFromIndex(index),
		Entries : nil,
		LeaderCommit: rf.GetCommitIndex(),
	}
	return msg
}
//create appendmsg by cutting log index from index
func(rf *Raft) NewAppendEntriesMsg(index int) *AppendEntriesArgs {
	msg := &AppendEntriesArgs {
		Term: rf.currentTerm,
		LeaderId: rf.me,
		
		PrevLogIndex: index,
		PrevLogTerm: rf.log.GetTermFromIndex(index),
		Entries : rf.log.GettingIndexfrom(index),
		LeaderCommit: rf.GetCommitIndex(),
	}
	return msg
}