package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

var ID = 0

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestID int
	ClientID int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
	Command     interface{}
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
	Command     interface{}
}
