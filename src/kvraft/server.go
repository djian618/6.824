package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"strconv"
	//"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	Put = "PUT"
	Append = "APPEND"
	Get = "GET"
)

const RespondWallTim = 1000

type AgreeMsg struct {
	value string
	success bool
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	kvMap   map[string] string
	reqMap  map[string] string
	maxraftstate int // snapshot if log grows this big
	agreeCh chan AgreeMsg
	// Your definitions here.
	isLeader bool
}

func getReqKey(reqId int, id int64) string {
	reqIdS := strconv.FormatInt(int64(reqId), 10)
	idS := strconv.FormatInt(id, 10)
	return (reqIdS+" "+idS)
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf(getReqKey(args.ReqId, args.Id))
	if val, ok := kv.reqMap[getReqKey(args.ReqId, args.Id)]; ok {
		DPrintf("duplicated appear")
		reply.WrongLeader = false
		reply.Err = ""
		reply.Value = val
		return
	}
	_, _, ok := kv.rf.Start(Op{Command: Get, Key: args.Key, Value: "", Id: args.Id, ReqId: args.ReqId});
	DPrintf("Server %v recived Get %v, ok %v",kv.me, args, ok)	
	if(!ok) {
		reply.WrongLeader = true
		return
	}else {
		kv.isLeader = true
	}
	for{
		select {
			case agreeMsg := <- kv.agreeCh:
				DPrintf("Server #%v recived commited agreeMsg %v", kv.me, agreeMsg)	
				reply.WrongLeader = false
				reply.Err = ""
				reply.Value = agreeMsg.value
				return 
			// case <-time.After(RespondWallTim*time.Millisecond):
			// 	reply.WrongLeader = false
			// 	reply.Err = TimeOutErr
			// 	return
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if _, ok := kv.reqMap[getReqKey(args.ReqId, args.Id)]; ok {
		DPrintf("duplicated appear")
		return
	}
	_, _, ok := kv.rf.Start(Op{Command: args.Op, Key: args.Key, Value: args.Value, Id: args.Id, ReqId: args.ReqId});    
    DPrintf("Server#%v PutAppend key %v value %v", kv.me, args.Key, args.Value)	
	if(!ok) {
		reply.WrongLeader = true
		return
	}else {
		kv.isLeader = true
	}
	for{
		select {
			case <- kv.agreeCh:
				reply.WrongLeader = false
				reply.Err = OK
				return 
			// case <-time.After(RespondWallTim*time.Millisecond):
			// 	reply.WrongLeader = false
			// 	reply.Err = TimeOutErr
			// 	return
		}
	}	
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	// Your initialization code here.
	kv.kvMap  = make(map[string] string)
	kv.reqMap = make(map[string] string) 

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.agreeCh = make(chan AgreeMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.isLeader = false
	go func() {
		for m := range kv.applyCh {
		    //DPrintf("Server #%v recive from applyCh %v", me, m)			        											
			if v, ok := m.Command.(Op); ok {
				if(kv.isLeader) {
					opType := v.Command
					if _, ok := kv.reqMap[getReqKey(v.ReqId, v.Id)]; !ok{
						kv.reqMap[getReqKey(v.ReqId, v.Id)] = "ok"
						switch  opType {
							case Append:								
								kv.kvMap[v.Key] += v.Value
								kv.agreeCh <- AgreeMsg{value:"", success:true}
						        DPrintf("Server #%v Append Success %v", me, v)			        								
							case Put:
								kv.kvMap[v.Key] = v.Value
								kv.agreeCh <- AgreeMsg{value:"", success:true}
						        DPrintf("Server #%v Put Success %v", me, v)			        														
							case Get:
								value := kv.kvMap[v.Key]
								kv.reqMap[getReqKey(v.ReqId, v.Id)] = value
								kv.agreeCh <- AgreeMsg{value:value, success:true}
						        DPrintf("Server #%v Get Success %v", me, value)			        																				
						}
					}
				}
			}
		}	
	}()

	return kv
}
