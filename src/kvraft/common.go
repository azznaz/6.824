package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq int
	LastWriteSeq int
	ClerkId  int64
}

type PutAppendReply struct {
	Err Err
	LeaderTerm int
	LeaderId int
}

type GetArgs struct {
	Key string
	Seq int
	// You'll have to add definitions here.
	ClerkId  int64
}

type GetReply struct {
	Err   Err
	LeaderTerm int
	Value string
	LeaderId int
}
