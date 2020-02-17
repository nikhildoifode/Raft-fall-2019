package raftkv

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd

	// You will have to modify this struct.
	leader int
	funcName string
	requestID int
	clientID int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd, funcName string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	// You'll have to add code here.
	ck.leader = -1
	ck.funcName = funcName
	ck.requestID = 0
	ck.clientID = ID
	ID++

	return ck
}

func (ck *Clerk) sendRPCToOne(rpcName string, args interface{}, reply interface{}, leader int) bool {
	return ck.servers[leader].Call(rpcName, args, reply)
}

func (ck *Clerk) sendRPCToAll(rpcName string, args interface{}) string {
	leader := 0
	if rpcName == "KVServer.PutAppend" {
		for {
			if leader == len(ck.servers) {
				leader = 0
			}
			reply := PutAppendReply{}
			ok := ck.sendRPCToOne(rpcName, args, &reply, leader)
			if ok && reply.Err == OK {
				ck.leader = leader
				return ""
			}
			leader++
			time.Sleep(150 * time.Millisecond)
		}
	} else {
		for {
			if leader == len(ck.servers) {
				leader = 0
			}
			reply := GetReply{}
			ok := ck.sendRPCToOne(rpcName, args, &reply, leader)
			if ok && reply.Err == OK {
				ck.leader = leader
				return reply.Value
			}
			leader++
			time.Sleep(150 * time.Millisecond)
		}
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	// DPrintf(ck.funcName + " Client", "Get Key [%s]", key)
	args := GetArgs{key}
	if ck.leader != -1 {
		reply := GetReply {}
		ok := ck.sendRPCToOne("KVServer.Get", &args, &reply, ck.leader)
		if ok && reply.Err == OK {
			return reply.Value
		}
		ck.leader = -1
	}

	return ck.sendRPCToAll( "KVServer.Get", &args)
}


//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.requestID++
	// DPrintf(ck.funcName + " Client", "PutAppend Key [%s], Value [%s], Op: [%s], Req ID: [%d], Cli ID [%d]", key, value, op, ck.requestID, ck.clientID)
	args := PutAppendArgs{key, value, op, ck.requestID, ck.clientID}
	if ck.leader != -1 {
		reply := PutAppendReply{}
		ok := ck.sendRPCToOne("KVServer.PutAppend", &args, &reply, ck.leader)
		if ok && reply.Err == OK {
			return
		}
		ck.leader = -1
	}
	ck.sendRPCToAll( "KVServer.PutAppend", &args)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
