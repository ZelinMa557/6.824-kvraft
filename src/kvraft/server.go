package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

const (
	TimeOut  = 1000
	Interval = 10
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type        string
	Client      int
	SequenceNum int
	Key         string
	Value       string
}

type OpResult struct {
	Value string
	Err   Err
}

type OpRecord struct {
	request Op
	result  OpResult
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	storage     map[string]string
	cmdChan     map[int]chan OpResult
	cmdRecord   map[int]OpRecord
	lastApplied int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:        "Get",
		Client:      args.ClientID,
		SequenceNum: args.SequenceID,
		Key:         args.Key,
	}
	index, _, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("%v start command %v\n", kv.me, args)
	ch := make(chan OpResult, 1)
	kv.mu.Lock()
	kv.cmdChan[index] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.cmdChan, index)
		close(ch)
		kv.mu.Unlock()
	}()
	t := time.NewTimer(TimeOut * time.Millisecond)
	defer t.Stop()
	finish_or_timeout := false
	for !finish_or_timeout {
		select {
		case result := <-ch:
			reply.Value, reply.Err = result.Value, result.Err
			finish_or_timeout = true
			DPrintf("%v finish %v\n", kv.me, index)
			break
		case <-t.C:
			reply.Value, reply.Err = "", ErrTimeOut
			finish_or_timeout = true
			DPrintf("%v %v tiem out\n", kv.me, index)
			break
		default:
			time.Sleep(Interval * time.Millisecond)
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:        args.Op,
		Client:      args.ClientID,
		SequenceNum: args.SequenceID,
		Key:         args.Key,
		Value:       args.Value,
	}
	index, _, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("%v start command %v\n", kv.me, args)
	ch := make(chan OpResult, 1)
	kv.mu.Lock()
	kv.cmdChan[index] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.cmdChan, index)
		close(ch)
		kv.mu.Unlock()
	}()
	t := time.NewTimer(TimeOut * time.Millisecond)
	defer t.Stop()
	finish_or_timeout := false
	for !finish_or_timeout {
		select {
		case result := <-ch:
			DPrintf("%v finish %v\n", kv.me, index)
			reply.Err = result.Err
			finish_or_timeout = true
			break
		case <-t.C:
			reply.Err = ErrTimeOut
			finish_or_timeout = true
			DPrintf("%v %v tiem out\n", kv.me, index)
			break
		default:
			time.Sleep(Interval * time.Millisecond)
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) ApplyWorker() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				cmd := msg.Command.(Op)
				res := OpResult{Err: OK}
				if record, ok := kv.cmdRecord[cmd.Client]; ok && record.request.SequenceNum == cmd.SequenceNum {
					if ch, ok := kv.cmdChan[msg.CommandIndex]; ok {
						ch <- res
					}
					kv.mu.Unlock()
					continue
				}
				switch cmd.Type {
				case "Get":
					if _, ok := kv.storage[cmd.Key]; !ok {
						res.Err = ErrNoKey
					} else {
						res.Value = kv.storage[cmd.Key]
					}
				case "Put":
					kv.storage[cmd.Key] = cmd.Value
				case "Append":
					if _, ok := kv.storage[cmd.Key]; !ok {
						kv.storage[cmd.Key] = cmd.Value
					} else {
						kv.storage[cmd.Key] += cmd.Value
					}
				}
				kv.cmdRecord[cmd.Client] = OpRecord{request: cmd, result: res}
				if ch, ok := kv.cmdChan[msg.CommandIndex]; ok {
					ch <- res
				} else {
				}
				kv.lastApplied++
				kv.mu.Unlock()
			} else if msg.SnapshotValid {

			}
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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.storage = make(map[string]string)
	kv.cmdChan = make(map[int]chan OpResult)
	kv.cmdRecord = make(map[int]OpRecord)
	kv.lastApplied = 0
	DPrintf("make server %v\n", kv.me)
	go kv.ApplyWorker()

	// You may need initialization code here.

	return kv
}
