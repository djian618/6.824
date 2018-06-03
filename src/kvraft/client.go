package raftkv

import (
	"labrpc"
	"crypto/rand"
	"math/big"
	"sync"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	n      int

	mu     sync.Mutex
	id     int64
	reqId  int
}

const (
	UNKOWN = -1
)


func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader  = UNKOWN
	ck.n       = len(ck.servers)
	ck.id      = nrand()
	ck.reqId   = 0
	return ck
}


//this command is used to send command to raftkvserver
//it will decode if it is put or get command and send it to 
//raftKv server
func (ck *Clerk) runServers(args interface{}) string{
	for si := 0; si < ck.n; si++ {
		if( (si == ck.leader) || (ck.leader == UNKOWN) ) {
			server := ck.servers[si]
			if server != nil {
			    //DPrintf("Server #%v", si)
			    switch args.(type){
			        case PutAppendArgs:
			        	var reply PutAppendReply
			        	var putAppendReq = args.(PutAppendArgs)
			        	//DPrintf("PutAppendArgs %v", putAppendReq)
			        	ok := server.Call("RaftKV.PutAppend", &putAppendReq, &reply)
			        	if ok {
				        	// DPrintf("PutAppend Reply WrongLeader %v", reply.WrongLeader)
			        		if( (!reply.WrongLeader)  && (reply.Err != TimeOutErr)  ) {
			        			DPrintf("Client PutAppend Success %v", reply)
			        			ck.leader = si
			        			return OK
			        		}
			        	}else {
			        		DPrintf("rpc failed")
			        	}
			        case GetArgs:
			        	var reply GetReply
			        	var getReq = args.(GetArgs)
			        	DPrintf("Client GetArgs %v", getReq)
			        	ok := server.Call("RaftKV.Get", &getReq, &reply)
			        	if ok {
				        	DPrintf("Client GetArgs Reply WrongLeader %v", reply.WrongLeader)			        		
			        		if( (!reply.WrongLeader) && (reply.Err != TimeOutErr) ) {
			        			DPrintf("Client Get Success %v", reply)			        			
			        			ck.leader = si
			        			return reply.Value
			        		} 
			        	}
			        default:
			            DPrintf("Unkown Type at runServers")
			    }
			}
		}
	}
	return Failed	
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	ck.reqId++
	getArgs := GetArgs{Key: key, Id: ck.id, ReqId: ck.reqId}
	ck.mu.Unlock()
	for{
		result := ck.runServers(getArgs)
		if(result != Failed) {
			return result
		}

	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	ck.reqId++
	putAppendArgs := PutAppendArgs{Key: key, Value: value, Op: op, Id: ck.id, ReqId: ck.reqId}
	ck.mu.Unlock()
	DPrintf("Client PutAppend %v", putAppendArgs)
	for{
		if(ck.runServers(putAppendArgs) != Failed) { 
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "PUT")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "APPEND")
}
