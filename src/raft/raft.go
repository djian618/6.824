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
	"log"
//	"sync/atomic"
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
	startmu   sync.Mutex
	cond      *sync.Cond 		  // conditional variable that is used for sending applymsg
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	applych   chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	log 	  *Log
	state 	  string
	currentTerm int
	voteFor		int
	commitIndex int
	lastApplied int 
	flushcount  int 
	nextIndex   []int
	matchIndex  []int
	//prevLogIndex int
	electionTimer	*Timer
	heartbeatTimer  *Timer
	//check if it has been killed
	isKilled bool
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


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, myreply *AppendEntriesRply) {
	// Your code here (2A, 2B).
	if(len(args.Entries)>0) {
		BPrintf("%d(term %d) recives a append entries from %d with PrevLogIndex%d PrevLogTerm %d Sender Term(%d) and Command is %v", 
		rf.me, rf.GetCurrentTerm(), args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Term, args.Entries)

	}

	//var myreply AppendEntriesRply
	if(args.Term < rf.GetCurrentTerm()) {
		rf.SetCurrentTerm(args.Term)
		//convert to follower 
		myreply.Success = false
		myreply.Term = rf.GetCurrentTerm()
		BPrintf("%d REJECT the append Entries because argsterm %d is smaller than its own %d", rf.me, args.Term, rf.GetCurrentTerm())
		//rf.ConvertToFollower()
		return
	}

	//BPrintf("rf #%d match Index and term: prevLogIndex is %d, term is %d",rf.me, args.PrevLogIndex, args.PrevLogTerm)
	if(!rf.log.TermMatchIndex(args.PrevLogIndex, args.PrevLogTerm)) {
		myreply.Success = false
		myreply.Term = rf.GetCurrentTerm()
		BPrintf("%d REJECT the append Entries because current Term %d does not match prevLogTerm %d", rf.me, myreply.Term, args.PrevLogTerm)
		
		return		
	}

	//conflictindex := rf.log.FindConflictIndex(args.PrevLogIndex, args.Entries)
	if(len(args.Entries)>0) {
		rf.log.TruncateLogAfter(args.PrevLogIndex)
		rf.log.AppendEntries(args.Entries)
		BPrintf("%d appendentries success! current log entries are %v", rf.me, rf.log.entries)
	}
	
	if(args.LeaderCommit > rf.commitIndex) {
		nextCommitIndex := min(args.LeaderCommit, rf.log.GetLastIndex())
		BPrintf("%d will increase its commitIndex to %d", rf.me, nextCommitIndex)
		rf.SendCommitLogInfo(rf.commitIndex, nextCommitIndex)
		rf.commitIndex = nextCommitIndex
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
	BPrintf("%d(term %d) recived converted to follower", 
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

func (rf *Raft) appendToReplica(pid int, c chan int, is_done *bool) {
	prevLogIndex := rf.GetNextIndexForPeer(pid)
	for {

		select {
		case _, ok := <- c:
			if(!ok) {
				BPrintf("the ok channel has been closed may be %d has become to follower", rf.me)
				return
			}
		default :	
			if(rf.GetCurrentState() != Leader) {
				return
			}
			BPrintf("%d send append entry to the replica for follower %d with prevLogIndex is %d",rf.me, pid, prevLogIndex)
			appendEntrymsg := rf.NewAppendEntriesMsg(prevLogIndex)
			rly := rf.NewAppendEntriesRly()
			success := rf.sendHeartBeat(pid, appendEntrymsg, rly)
			if(success) {
				if(rly.Success) {
					//If successful: update nextIndex and matchIndex for follower
					//should set the match index but leave for now
					rf.SetNextIndexForPeer(pid, rf.log.GetLastIndex())
					rf.SetMatchIndexForPeer(pid, rf.log.GetLastIndex())
					rf.mu.Lock()
					rf.flushcount++
					BPrintf("%d Increase the flushCount to %d", rf.me, rf.flushcount)
					if(rf.flushcount >= (rf.GetPeersTotal())/2) {
						//rf.cond.L.Unlock()
						if(*is_done == false) {
							*is_done = true
							BPrintf("command has replicated to majority of replicas send Signal")
							rf.SendCommitLogInfo(rf.GetCommitIndex(), rf.log.GetLastIndex())
							rf.SetCommitIndex(rf.log.GetLastIndex());
						}

					}
					rf.mu.Unlock()
					return
				}else {
					//BPrintf("The append entry falied for follower %d", pid)
					if(rly.Term > rf.GetCurrentTerm()) {
						BPrintf("%d closed its channel", rf.me)
						close(c)
						rf.SetCurrentTerm(rly.Term)
						rf.ConvertToFollower()
						return
					}
					rf.DecNextIndexForPeer(pid)
				}
			}
			BPrintf("%d Unlock the mutex", rf.me)
		}
		//rf.mu.Unlock()
	}
}



//send append entry to all the follower 
func (rf *Raft) FlushoutReplica(c chan int) {
	is_done := false
	rf.flushcount = 0
	for i, _ := range rf.peers {
		if(i!=rf.me){
			index := i
			BPrintf("initalize thread to appendReplica to %d", i)
			go rf.appendToReplica(index, c, &is_done)
		}
	}
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
	// index := -1
	// term := -1
	// isLeader := true
	rf.startmu.Lock()
	defer rf.startmu.Unlock()
	BPrintf("%d start to recieve the Command %d", rf.me, command)
	if(rf.GetCurrentState() == Leader) {
		newEntry := LogEntry{rf.GetCurrentTerm(), command}
		rf.log.AppendEntry(newEntry)
		BPrintf("leader %d start to flushout replicate its log is %v", rf.me, rf.log.entries)

		c := make(chan int)
		go rf.FlushoutReplica(c)
		return rf.log.GetLastIndex(), rf.GetCurrentTerm(), true
	}else {
		return rf.log.GetLastIndex(), rf.GetCurrentTerm(), false
	}
	// Your code here (2B).
//	return index, term, isLeader
}


func (rf *Raft) NewApplyMsg() ApplyMsg{
	if(rf.log.GetLastCommand()!=100) {
		panic("command is not found")
	}
	msg := ApplyMsg {
		Index : rf.log.GetLastIndex(),
		Command : rf.log.GetLastCommand(),
	}
	return msg
}

//all the servers need to independently give 
//each newly committed entry to their local service replica (via their own applyCh)
//we shuold use conditional variable over here
func (rf *Raft) SendCommitLogInfo(begin int, end int) {
	for i:= begin; i<end; i++ {
		BPrintf("rf #%d send commited msg of index %d through applych", rf.me, i)
		msg := ApplyMsg {
			Index : i+1,
			Command : rf.log.GetCommandFromIndex(i),
		}
		rf.applych <- msg
	}

	//rf.cond.L.Unlock()
}


//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	log.Printf("%d has been KILLED!!", rf.me)
	rf.isKilled = true;
}





func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesRply) bool {
	DPrintf("Send heart beat to server %d", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//start heartbeat to all the peers
//since the sender do not care about the response 
//we should just use a thread to send it 
func (rf *Raft) BroadcastHeartBeat() {
	for i, _ := range rf.peers {
		if(rf.isKilled) {
			return;
		}
		if(i!=rf.me){
			pid := i
			//BPrintf("Planning to send i %d with %d of peers", i, len(rf.peers))

			go func() { 
				BPrintf("%d Planning to send pid %d with prevIndex %d", rf.me, pid, rf.GetMatchIndexForPeer(pid))
				appendEntrymsg := rf.NewAppendEntriesMsg(rf.GetMatchIndexForPeer(pid))
				rly := rf.NewAppendEntriesRly()
				success := rf.sendHeartBeat(pid, appendEntrymsg, rly)
				if(success) {
					if(rly.Success) {
						rf.SetMatchIndexForPeer(pid, rf.log.GetLastIndex())
					} else {
					if(rly.Term > rf.GetCurrentTerm()) {
						rf.SetCurrentTerm(rly.Term)
						rf.ConvertToFollower()
						return
					}					
				}
				} 
			}()
		}
	}
	rf.heartbeatTimer.Reset()
}


func (rf *Raft) heartbeatGenerator() {
	for{
		if(rf.isKilled) {
			return
		}
		if(rf.GetCurrentState() != Leader) {
			return;
		}
		<- rf.heartbeatTimer.C()
		DPrintf("%d start sending heartbeat", rf.me)
		rf.BroadcastHeartBeat()
	}
}

//--------------------------------------
// Promotion to leader
//--------------------------------------
func (rf *Raft) PromoteToLeader() {
	BPrintf("%d Promoted to Leader", rf.me)
	rf.ChangeState(Leader)
	//reinitalize a nexindex and matched index for this leader
	rf.InitmatchIndex()
	rf.InitNextIndex()
	rf.flushcount = 0
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
			case <-rf.electionTimer.C():
				if(rf.isKilled) {
					DPrintf("the timer has been close")
					return;
				}
				BPrintf("%d restart the election process", rf.me)
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

	BPrintf("%d(term %d) recived a request vote from %d term(%d) its vote for %d, RequestVoteArgs is %v", 
		rf.me, rf.GetCurrentTerm(), args.CandidateId, args.Term, rf.voteFor, *args)
	
	if (rf.GetCurrentTerm()>args.Term) {
		reply.VoteGranted = false
		reply.Term = rf.GetCurrentTerm()
		return
	}

	/*
	Raft determines which of two logs is more up-to-date
	by comparing the index and term of the last entries in the
	logs. If the logs have last entries with different terms, then
	the log with the later term is more up-to-date. If the logs
	end with the same term, then whichever log is longer is
	more up-to-date.
	*/
	//
	//if(rf.log.IsEmpty()==false) {
	BPrintf("test upto date")
	if ( (rf.log.GetTermForLastIndex() > args.LastLogTerm) ||
		((rf.log.GetTermForLastIndex() == args.LastLogTerm) && (rf.log.GetLastIndex() > args.LastLogIndex )) ) {
		BPrintf("%d reject vote from  %d", rf.me, args.CandidateId)
		reply.VoteGranted = false
		reply.Term = args.Term
		return
	}
	//}

	// if (rf.log.GetLastIndex() > args.LastLogIndex) {
	// 	reply.VoteGranted = false
	// 	reply.Term = args.Term
	// 	return
	// }

	if(rf.GetCurrentTerm()<args.Term) {
		rf.ConvertToFollower()
		rf.SetCurrentTerm(args.Term)
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
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.cond = sync.NewCond(&rf.mu)
	rf.applych = applyCh
	
	// Your initialization code here (2A, 2B, 2C).
	rf.ChangeState(Follower)
	rf.log = NewLog()
	rf.SetCurrentTerm(0)
	rf.SetVoteFor(Nobody)
	rf.electionTimer = NewTimer(DefaultElectionTimeout, 3*DefaultElectionTimeout)
	rf.heartbeatTimer = NewTimer(DefaultHeartbeatTimeout, DefaultHeartbeatTimeout)
	
	//initalize property for flush appendentries
	rf.nextIndex = make([]int, len(peers))
	
	rf.matchIndex = make([]int, len(peers))

	rf.flushcount = 0
	rf.isKilled = false;
	//rf.prevLogIndex = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.heartbeatTimer.Pause()
	log.Printf("Initalize Peer %d with total peers %d", me, len(rf.peers))
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

func (rf *Raft) GetCommitIndex () int {
	return rf.commitIndex
}

func (rf *Raft) GetlastApplied () int {
	return rf.lastApplied
}

func(rf *Raft) InitNextIndex() {
	for i:=0; i<len(rf.peers); i++ {
		rf.nextIndex[i] = rf.log.GetLastIndex()
	}
}

func(rf *Raft) InitmatchIndex() {
	for i:=0; i<len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) GetNextIndexForPeer(pid int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[pid]
}

func (rf *Raft) SetNextIndexForPeer(pid int, val int) {
	rf.nextIndex[pid] = val
}

func (rf *Raft) GetMatchIndexForPeer(pid int) int{
	return rf.matchIndex[pid]
}

func (rf *Raft) SetMatchIndexForPeer(pid int, index int) {
	rf.matchIndex[pid] = index
}

func (rf *Raft) DecNextIndexForPeer(pid int) {
	if(rf.nextIndex[pid]==0) { 
		return 
	}
	rf.nextIndex[pid]--
}

func (rf *Raft) SetCommitIndex(index int) {
	rf.commitIndex = index
}

func (rf *Raft) IncreaseCommitIndex() {
	rf.commitIndex++
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