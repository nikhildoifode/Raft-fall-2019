package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(funcName string, format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(funcName + " " + format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type Reply struct {
	CommandType  int // 1 = Get, 2 = Put
	CommandValue string
	Command      interface{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// Added by me
	killChan chan int
	kvDB map[string]string // because key can be anything

	// temp
	getReplyChanCheck[10000] bool
	getReplyChan[10000] chan Reply
	clientID[10000] int

	//Debugging
	funcName string
}

/* Reference: Discussed with Krishna DK about having index specific entries */
func (kv *KVServer) clientListeningChannel(index int, args interface{}) (string, Err, bool) {
	exitTimer := time.NewTimer(time.Duration(3000) * time.Millisecond) // For select exit timer
	kv.mu.Lock()
	if !kv.getReplyChanCheck[index] {
		kv.getReplyChan[index] = make(chan Reply, 1)
		kv.getReplyChanCheck[index] = true
	}
	kv.mu.Unlock()
	select {
	case msg := <- kv.getReplyChan[index]:
		kv.mu.Lock()
		if kv.getReplyChanCheck[index] {
			kv.getReplyChanCheck[index] = false
			close(kv.getReplyChan[index])
		}
		kv.mu.Unlock()
		if args == msg.Command {
			// DPrintf(kv.funcName, "Received entry [%v], index [%d]", msg, index)
			return msg.CommandValue, OK, false
		}
		// DPrintf(kv.funcName, "Received Wrong entry [%v], index [%d]", msg, index)
		return "", ErrNoKey, true
	case <-kv.killChan:
		return "", ErrNoKey, true
	case <-exitTimer.C:
		kv.mu.Lock()
		if kv.getReplyChanCheck[index] {
			kv.getReplyChanCheck[index] = false
			close(kv.getReplyChan[index])
		}
		kv.mu.Unlock()
		return "", ErrNoKey, true
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNoKey
		reply.Value = ""
		return
	}
	// DPrintf(kv.funcName, "Sent Get to Raft [%d] [%v]", index, *args)
	// Adding it to the common functionality so that if there is partition same index will only create one channel
	value, err, leaderVal := kv.clientListeningChannel(index, *args)
	reply.WrongLeader = leaderVal
	reply.Err = err
	reply.Value = value
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
 	index, _, isLeader := kv.rf.Start(*args)
 	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNoKey
		return
	}
	// DPrintf(kv.funcName, "Sent Put to Raft [%d] [%v]", index, *args)
	// Adding it to the common functionality so that if there is partition same index will only create one channel
	_, err, leaderVal := kv.clientListeningChannel(index, *args)
	reply.WrongLeader = leaderVal
	reply.Err = err
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	// DPrintf(kv.funcName, "Kill server [%d]", kv.me)
	close(kv.killChan)
}

func (kv *KVServer) applyChListener() {
	// Separated from the Client RPC reply part so that it doesn't get stuck and we can loop forever
	// Added select mechanism so that we can kill
	for {
		select {
		case msg := <- kv.applyCh:
			reply := Reply{}
			reply.Command = msg.Command
			// Decide if it is Get Argument
			getArgs, ok := (msg.Command).(GetArgs)
			if ok {
				kv.mu.Lock()
				// Only send for the peer where someone is listening to PutAppend RPC reply
				if kv.getReplyChanCheck[msg.CommandIndex] {
					reply.CommandType = 1
					reply.CommandValue = kv.kvDB[getArgs.Key]
					// DPrintf(kv.funcName, "Received GetArgs [%v], index [%d]", getArgs, msg.CommandIndex)
					kv.getReplyChan[msg.CommandIndex] <- reply
				}
				kv.mu.Unlock()
			}
			// Decide if it is PutAppend Argument
			putAppendArgs, ok2 := (msg.Command).(PutAppendArgs)
			if ok2 {
				kv.mu.Lock()
				// Apply changes to the DB for each peer
				if kv.clientID[putAppendArgs.ClientID] < putAppendArgs.RequestID {
					if putAppendArgs.Op == "Put" {
						kv.kvDB[putAppendArgs.Key] = putAppendArgs.Value
					} else {
						kv.kvDB[putAppendArgs.Key] += putAppendArgs.Value // Append value
					}
					kv.clientID[putAppendArgs.ClientID] = putAppendArgs.RequestID
				}
				// Only send for the peer where someone is listening to PutAppend RPC reply
				if kv.getReplyChanCheck[msg.CommandIndex] {
					reply.CommandType = 2
					reply.CommandValue = ""
					// DPrintf(kv.funcName, "Received PutAppendArgs [%v], index [%d]", putAppendArgs, msg.CommandIndex)
					kv.getReplyChan[msg.CommandIndex] <- reply
				}
				kv.mu.Unlock()
			}
		case <-kv.killChan:
			return
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, funcName string) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	// DPrintf(funcName, "# of servers [%d], me [%d], maxraftstate [%d]", len(servers), me, maxraftstate)
	// Check if there's any problem with using separate structures for Get and Put instead of Op
	//labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Added 1000 since we are not continuously listening and we just changed lab 2 implementation
	// so that we don't launch for sending data to client to remove apply out of order error 
	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh, funcName)

	// You may need initialization code here.
	kv.killChan = make(chan int)
	kv.kvDB = make(map[string]string, 10000)
	go kv.applyChListener()

	//Debugging
	//kv.funcName = funcName

	return kv
}
