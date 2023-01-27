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
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type RaftType int

const (
	Candidate RaftType = iota
	Follower
	Leader
)

const (
	none     = -1
	MaxTime  = 500
	interval = 100
)

type LogEntry struct {
	command string
	term    int
}

type RaftState struct {
	currentTerm   int
	votedFor      int
	log           []LogEntry
	commitedIndex int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       RaftState
	currentType RaftType
	startTime   time.Time
	getVoted    int

	stateLock sync.Mutex
	typeLock  sync.Mutex
	timeLock  sync.Mutex
	voteLock  sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	rf.typeLock.Lock()
	defer rf.typeLock.Unlock()
	term = rf.state.currentTerm
	if rf.killed() == false && rf.currentType == Leader {
		isleader = true
	} else {
		isleader = false
	}
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntryArgs struct {
	LeaderTerm int
}

type AppendEntryReply struct {
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	reply.Term = rf.state.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.state.currentTerm {
		fmt.Printf("%v not vote %v for term\n", rf.me, args.CandidateId)
		return
	}
	term, idx := rf.getLastTermIndex()
	if args.LastLogTerm < term {
		fmt.Printf("%v not vote %v for log term\n", rf.me, args.CandidateId)
		return
	} else if args.LastLogIndex < idx {
		fmt.Printf("%v not vote %v for log idx\n", rf.me, args.CandidateId)
		return
	}
	// in the same term, a server should only vote once at most.
	if args.Term == rf.state.currentTerm {
		if rf.state.votedFor != none || rf.state.votedFor != args.CandidateId {
			fmt.Printf("%v not vote %v for voted for %v\n", rf.me, args.CandidateId, rf.state.votedFor)
			return
		}
	} else {
		rf.state.currentTerm = args.Term
		rf.toFollower()
	}
	rf.state.votedFor = args.CandidateId
	reply.VoteGranted = true
	fmt.Printf("%v vote %v on term %v\n", rf.me, args.CandidateId, rf.state.currentTerm)
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.stateLock.Lock()
	rf.timeLock.Lock()
	if args.LeaderTerm >= rf.state.currentTerm {
		rf.startTime = time.Now()
		rf.toFollower()
	}
	rf.timeLock.Unlock()
	rf.stateLock.Unlock()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok && reply.VoteGranted {
		rf.voteLock.Lock()
		rf.getVoted += 1
		rf.voteLock.Unlock()
	}
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

// stateLock should got before use this function
func (rf *Raft) getLastTermIndex() (int, int) {
	term := 0
	if len(rf.state.log) > 0 {
		term = rf.state.log[len(rf.state.log)-1].term
	}
	return term, len(rf.state.log)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getTime() int64 {
	rf.timeLock.Lock()
	defer rf.timeLock.Unlock()
	return time.Since(rf.startTime).Milliseconds()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.typeLock.Lock()
		ty := rf.currentType
		rf.typeLock.Unlock()
		if ty == Leader {
			rf.LeaderBoardcast()
		} else if ty == Candidate {
			rf.voteLock.Lock()
			voted := rf.getVoted
			rf.voteLock.Unlock()
			if voted > len(rf.peers)/2 {
				rf.toLeader()
			} else if rf.getTime() > MaxTime {
				fmt.Println("raft", rf.me, "try to restart a election")
				rf.toCandidate()
			}
		} else if ty == Follower {
			if rf.getTime() > MaxTime {
				fmt.Println("raft", rf.me, "try to start a election")
				rf.toCandidate()
			}
		}
		if ty == Leader {
			time.Sleep(time.Millisecond * time.Duration(interval))
		} else {
			time.Sleep(time.Millisecond * time.Duration(interval/3))
		}
	}
}

func (rf *Raft) toCandidate() {
	time.Sleep(time.Millisecond * time.Duration(rand.Int()%interval))
	rf.stateLock.Lock()
	rf.state.currentTerm += 1
	rf.state.votedFor = rf.me
	term, idx := rf.getLastTermIndex()
	rf.stateLock.Unlock()

	rf.timeLock.Lock()
	rf.startTime = time.Now()
	rf.timeLock.Unlock()

	rf.typeLock.Lock()
	rf.currentType = Candidate
	rf.typeLock.Unlock()

	rf.voteLock.Lock()
	rf.getVoted = 1
	rf.voteLock.Unlock()

	rf.stateLock.Lock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{}
		args.CandidateId = rf.me
		args.Term = rf.state.currentTerm
		args.LastLogTerm, args.LastLogIndex = term, idx
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply)
	}
	rf.stateLock.Unlock()
}

func (rf *Raft) toLeader() {
	rf.stateLock.Lock()
	rf.state.nextIndex = make([]int, 0)
	rf.state.matchIndex = make([]int, 0)
	rf.stateLock.Unlock()
	rf.typeLock.Lock()
	rf.currentType = Leader
	fmt.Println(rf.me, "become leader")
	rf.typeLock.Unlock()
	rf.LeaderBoardcast()
}

func (rf *Raft) LeaderBoardcast() {
	rf.stateLock.Lock()
	term := rf.state.currentTerm
	rf.stateLock.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := AppendEntryArgs{}
			reply := AppendEntryReply{}
			args.LeaderTerm = term
			server := i
			go func() {
				rf.sendAppendEntry(server, &args, &reply)
			}()
		}
	}
}

func (rf *Raft) toFollower() {
	rf.typeLock.Lock()
	if rf.currentType != Follower {
		fmt.Printf("%v became follower\n", rf.me)
	}
	rf.currentType = Follower
	rf.typeLock.Unlock()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state.currentTerm = 0
	rf.state.votedFor = none
	rf.state.commitedIndex = 0
	rf.state.lastApplied = 0
	rf.currentType = Follower
	rf.startTime = time.Now()
	rf.getVoted = 0

	fmt.Println("init raft", me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
