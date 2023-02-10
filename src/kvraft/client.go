package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu             sync.Mutex
	leader         int
	clientID       int
	maxSequenceNum int
}

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
	ck.leader = 0
	ck.clientID = int(nrand())
	ck.maxSequenceNum = 0
	DPrintf("make clerk %v\n", ck.clientID)
	return ck
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
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{
		Key:        key,
		ClientID:   ck.clientID,
		SequenceID: ck.maxSequenceNum + 1,
	}
	DPrintf("client %v try to get %v\n", ck.clientID, key)
	reply := GetReply{}
	leaderId := ck.leader
	for ; ; leaderId = (leaderId + 1) % len(ck.servers) {
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			continue
		}
		ck.leader = leaderId
		ck.maxSequenceNum = args.SequenceID
		break
	}
	if reply.Err == ErrNoKey {
		return ""
	} else {
		return reply.Value
	}
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
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{
		Key:        key,
		Value:      value,
		Op:         op,
		ClientID:   ck.clientID,
		SequenceID: ck.maxSequenceNum + 1,
	}
	DPrintf("client %v try to put append op:%v key: %v value:%v\n", ck.clientID, args.Op, key, value)
	reply := PutAppendReply{}
	leaderId := ck.leader
	for ; ; leaderId = (leaderId + 1) % len(ck.servers) {
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			continue
		}
		ck.leader = leaderId
		ck.maxSequenceNum = args.SequenceID
		break
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
