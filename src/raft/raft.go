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
	"sync"
	"labrpc"
	"time"
)


// import "bytes"
// import "encoding/gob"

//------------------------------------------------------------------------------
//
// Constants
//
//------------------------------------------------------------------------------

const (
	Stopped   = "stopped"
	Follower  = "follower"
	Candidate = "candidate"
	Leader    = "leader"
	Nobody 	  = -1
)

const (
	DefaultHeartbeatTimeout = 100 * time.Millisecond
	DefaultElectionTimeout  = 250 * time.Millisecond
)
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	log 	  *Log
	state 	  string
	currentTerm int
	voteFor		int
	electionTimer	*Timer
	heartbeatTimer  *Timer
}

//--------------------------------------
// Election timeout
//--------------------------------------

// this thread handle  election timeout.
// if there is timeout then promote from 
// follower to candidiate
func (rf *Raft) InitElectionTimoutFunc() {
    rf.electionTimer.Reset()
    DPrintf("%d set its eleciton timer to be %v", rf.me, rf.electionTimer.timePeriod)
    select {
    	case  <-rf.electionTimer.C():
    		DPrintf("%d Start the election", rf.me)
    		rf.ElectionProcess()
    }
    return
}	


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesRply) {
	// Your code here (2A, 2B).
	DPrintf("%d recives a heart beat", rf.me)
	var myreply AppendEntriesRply
	if(args.Term > rf.GetCurrentTerm()) {
		rf.SetCurrentTerm(args.Term)
		//convert to follower 
		myreply.Success = false
		myreply.Term = rf.GetCurrentTerm()
		rf.ConvertToFollower()
		return
	}
	myreply.Success = true
	myreply.Term = rf.GetCurrentTerm()
	rf.electionTimer.Reset()
	return
}



//convert to follower is called when the 
//dcandiate state grant a vote,
//or recive a append entries from new leader 
func (rf *Raft) ConvertToFollower() {
	DPrintf("%d(term %d) recived converted to follower", 
		rf.me, rf.GetCurrentTerm())

	// if(rf.GetCurrentState() == Follower)  {
	// 	return
	// }

	if(rf.GetCurrentState() == Candidate) {
		rf.electionTimer.Pause()
	}else if(rf.GetCurrentState() == Leader) {
		rf.heartbeatTimer.Pause()
	}
	rf.ChangeState(Follower)
	go 	rf.InitElectionTimoutFunc()
}



// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.GetCurrentTerm()
	isleader = (rf.state==Leader)
	return term, isleader

}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}
//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("%d sending request vote to %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	DPrintf("%d has been killed!!", rf.me)
}


func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesRply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//start heartbeat to all the peers
//since the sender do not care about the response 
//we should just use a thread to send it 
func (rf *Raft) BroadcastHeartBeat() {
	appendEntrymsg := rf.NewHeartBeatMsg()
	for i, _ := range rf.peers {
		if(i!=rf.me){
			index := i
			go func() { 
				rly := rf.NewAppendEntriesRly()
				rf.sendHeartBeat(index, appendEntrymsg, rly)
			}()
		}
	}
	rf.heartbeatTimer.Reset()
}

//create hearbeat msg
func (rf *Raft) NewHeartBeatMsg() *AppendEntriesArgs{
	rf.mu.Lock()
	defer rf.mu.Unlock()	
	msg := &AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		
		PrevLogIndex: rf.log.GetPrevIndex(),
		PrevLogTerm: rf.log.GetCurrentTerm(),
		Entries : []LogEntry{},
		LeaderCommit: rf.log.GetCommitIndex(),
	}
	return msg
}

func (rf *Raft) heartbeatGenerator() {
	for{
		<- rf.heartbeatTimer.C()
		DPrintf("%d start sending heartbeat", rf.me)
		rf.BroadcastHeartBeat()
	}
}

//--------------------------------------
// Promotion to leader
//--------------------------------------
func (rf *Raft) PromoteToLeader() {
	DPrintf("%d Promoted to Leader", rf.me)
	rf.ChangeState(Leader)
	rf.electionTimer.Pause()
	rf.heartbeatTimer.Reset()
	go rf.heartbeatGenerator()
}

//--------------------------------------
// promote the server to candidate
// and start the voting process
//--------------------------------------
func (rf *Raft) PromoteToCandidate() {
	rf.IncreseCurrentTerm()
	rf.SetVoteFor(rf.me)
	rf.ChangeState(Candidate)
	rf.electionTimer.Reset()
}	
// start election process
// this is the thread that start the election 
// and try to repeat the elction until timeout
func (rf *Raft) ElectionProcess() {
	rf.PromoteToCandidate()
	requestVoteArgs := rf.NewRequestVoteArgs()
	grant_buffer := make(chan bool, rf.GetPeersTotal())
	for i, _ := range rf.peers {
		if(i != rf.me){
			index := i
			go func() {
				rly := rf.NewRequestVoteReply()
				success := rf.sendRequestVote(index, requestVoteArgs, rly)
				if(success) {
					if(rly.Term > rf.GetCurrentTerm()) {
						DPrintf("%d recvied rly  and change its term to %d", 
							rf.me, rly.Term)
						rf.SetCurrentTerm(rly.Term)
						rf.ConvertToFollower();
						return
					}
					grant_buffer <- rly.VoteGranted
				}else {
					DPrintf("%d sending vote request is FAILED", rf.me)
				}
			}()
		}
	}

	successCount := 1
	majorityVote := rf.GetPeersTotal()/2
	for {
		select {
			case is_success := <- grant_buffer:
				if(is_success) {
					successCount++
				}
				if(successCount>majorityVote) {
					//pause timer update to learder
					rf.PromoteToLeader()
				}
			case  <-rf.electionTimer.C():
				DPrintf("%d restart the election process", rf.me)
				//when the retart begin, the current state may not
				//be candidate
				go rf.ElectionProcess()
				
				return
		}
	}
}


// example RequestVote RPC handler.
// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// If the term in the RPC is smaller than the candidate’s
// current term, then the candidate rejects the RPC and continues
// in candidate state.
// 2. If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm

	DPrintf("%d(term %d) recived a request vote from %d term(%d) its vote for %d", 
		rf.me, rf.GetCurrentTerm(), args.CandidateId, args.Term, rf.voteFor)
	
	if (rf.GetCurrentTerm()>args.Term) {
		reply.VoteGranted = false
		reply.Term = args.Term
		return
	}

	if(rf.GetCurrentTerm()<args.Term) {
		rf.SetCurrentTerm(args.Term)
		rf.ConvertToFollower()
	}


	if ( (rf.GetVoteFor() == Nobody)  )  {
		DPrintf("%d(term %d) granted the vote to %d", 
			rf.me, rf.GetCurrentTerm(), args.CandidateId)
		reply.VoteGranted = true
		rf.SetVoteFor(args.CandidateId)
		return		
	} 
	DPrintf("Not vote granted for %d", rf.me) 
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.ChangeState(Follower)
	rf.log = NewLog()
	rf.SetCurrentTerm(0)
	rf.SetVoteFor(Nobody)
	rf.electionTimer = NewTimer(DefaultElectionTimeout, 2*DefaultElectionTimeout)
	rf.heartbeatTimer = NewTimer(DefaultHeartbeatTimeout, DefaultHeartbeatTimeout)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.heartbeatTimer.Pause()
	DPrintf("Initalize Peer %d", me)
	go rf.InitElectionTimoutFunc()

	return rf
}

//accessor
//get the candidate the current term voting for
func (rf *Raft) GetVoteFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.voteFor
}

func (rf *Raft) GetPeersTotal() int {
	return len(rf.peers)
}

//state accessror
func (rf *Raft) ChangeState(state string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state
}

func (rf *Raft) GetCurrentState() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) IncreseCurrentTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm += 1
	return  
}

//If RPC request or response contains term T > currentTerm:
//set currentTerm = T, convert to follower (§5.1)
func (rf *Raft) SetCurrentTerm(term int){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if(rf.currentTerm < term){
		rf.voteFor = Nobody
		rf.currentTerm = term
	}
}


func (rf *Raft) GetCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}
func (rf *Raft) SetVoteFor(id int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.voteFor = id
	return
}