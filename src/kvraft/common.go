package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	Failed = "FAILED"
	TimeOutErr  = "Time Out"			
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64
	ReqId int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64
	ReqId int	
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string

}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command string
	Key     string 
	Value   string
	Id      int64
	ReqId   int
}




