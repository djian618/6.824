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
