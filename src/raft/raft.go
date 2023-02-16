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

	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
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
	Command interface{}
	Term    int
}

type RaftState struct {
	currentTerm       int
	votedFor          int
	log               []LogEntry
	commitedIndex     int
	lastApplied       int
	nextIndex         []int
	matchIndex        []int
	lastIncludedIndex int
	lastIncludedTerm  int
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

	applyChan chan ApplyMsg
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.state.currentTerm)
	e.Encode(rf.state.votedFor)
	// e.Encode(rf.state.commitedIndex)
	// e.Encode(rf.state.lastApplied)
	e.Encode(rf.state.lastIncludedIndex)
	e.Encode(rf.state.lastIncludedTerm)
	e.Encode(rf.state.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) GetPersistSize() int {
	return len(rf.persister.raftstate)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voted int
	var incIdx int
	var incTerm int
	// var commited int
	// var applied int
	var log []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&voted) != nil ||
		// d.Decode(&commited) != nil ||
		// d.Decode(&applied) != nil ||
		d.Decode(&incIdx) != nil ||
		d.Decode(&incTerm) != nil ||
		d.Decode(&log) != nil {
		panic("fail to decode\n")
	} else {
		rf.state.currentTerm = term
		rf.state.votedFor = voted
		// rf.state.commitedIndex = commited
		// rf.state.lastApplied = applied
		rf.state.lastIncludedIndex = incIdx
		rf.state.lastIncludedTerm = incTerm
		rf.state.log = log
	}
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
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	if index <= rf.state.lastIncludedIndex {
		return
	}
	if rf.state.lastIncludedIndex > 0 {
		last_idx := index - rf.state.lastIncludedIndex - 1
		rf.state.lastIncludedTerm = rf.state.log[last_idx].Term
		rf.state.log = rf.state.log[last_idx+1:]
	} else {
		rf.state.lastIncludedTerm = rf.state.log[index].Term
		rf.state.log = rf.state.log[index+1:]
	}
	rf.state.lastIncludedIndex = index
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.state.currentTerm)
	e.Encode(rf.state.votedFor)
	e.Encode(rf.state.lastIncludedIndex)
	e.Encode(rf.state.lastIncludedTerm)
	e.Encode(rf.state.log)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
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
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Succuss bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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
		return
	}
	// in the same term, a server should only vote once at most.
	if args.Term == rf.state.currentTerm {
		if rf.state.votedFor != none {
			return
		}
	} else {
		rf.state.currentTerm = args.Term
		rf.toFollower()
	}
	term, idx := rf.getLastTermIndex()
	if args.LastLogTerm < term {
		return
	} else if args.LastLogTerm == term && args.LastLogIndex < idx {
		return
	}
	rf.state.votedFor = args.CandidateId
	rf.persist()
	reply.VoteGranted = true
	rf.timeLock.Lock()
	rf.startTime = time.Now()
	rf.timeLock.Unlock()
	rf.toFollower()
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) offset() int {
	if rf.state.lastIncludedIndex == 0 {
		return 0
	} else {
		return rf.state.lastIncludedIndex + 1
	}
}

func (rf *Raft) lastLogIndex() int {
	if rf.state.lastIncludedIndex == 0 {
		return len(rf.state.log) - 1
	} else {
		return rf.state.lastIncludedIndex + len(rf.state.log)
	}
}
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	reply.Term = rf.state.currentTerm
	reply.Succuss = false
	// Reply false if term < currentTerm
	if args.Term < rf.state.currentTerm {
		return
	} else if args.Term > rf.state.currentTerm {
		rf.state.currentTerm = args.Term
		rf.persist()
	}
	// Heartbeat
	rf.timeLock.Lock()
	rf.startTime = time.Now()
	rf.timeLock.Unlock()
	rf.toFollower()
	if len(args.Entries) == 0 {
		reply.Succuss = true
		// If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.state.commitedIndex {
			rf.state.commitedIndex = min(args.LeaderCommit, rf.lastLogIndex())
		}
		return
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex >= 0 {
		if rf.lastLogIndex() < args.PrevLogIndex {
			return
		} else if args.PrevLogIndex == rf.state.lastIncludedIndex {
			if args.PrevLogTerm != rf.state.lastIncludedTerm {
				return
			}
		} else if args.PrevLogIndex < rf.state.lastIncludedIndex {
			return
		} else if rf.state.log[args.PrevLogIndex-rf.offset()].Term != args.PrevLogTerm {
			return
		}
	}
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	start := 0
	if len(rf.state.log) > 0 && rf.lastLogIndex() > args.PrevLogIndex {
		i := args.PrevLogIndex + 1 - rf.offset()
		for ; i < len(rf.state.log) && start < len(args.Entries); i++ {
			if rf.state.log[i].Term != args.Entries[start].Term {
				break
			}
			start++
		}
		if i != len(rf.state.log) && start != len(args.Entries) {
			rf.state.log = rf.state.log[:i]
			rf.persist()
		}
	}
	// Append any new entries not already in the log
	if start < len(args.Entries) {
		rf.state.log = append(rf.state.log, args.Entries[start:]...)
		rf.persist()
	}
	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.state.commitedIndex {
		rf.state.commitedIndex = min(args.LeaderCommit, rf.lastLogIndex())
	}
	reply.Succuss = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	reply.Term = rf.state.currentTerm
	// Reply immediately if term < currentTerm
	if args.Term < rf.state.currentTerm || args.LastIncludedIndex <= rf.state.lastIncludedIndex {
		return
	}
	// If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply
	same_idx := 0
	if rf.state.lastIncludedIndex == 0 {
		same_idx = args.LastIncludedIndex
	} else {
		same_idx = args.LastIncludedIndex - rf.state.lastIncludedIndex - 1
	}
	if same_idx < len(rf.state.log)-1 && rf.state.log[same_idx].Term == args.Term {
		rf.state.log = rf.state.log[same_idx+1:]
	} else {
		// Discard the entire log
		rf.state.log = []LogEntry{}
	}
	// Reset state machine using snapshot contents
	rf.state.lastIncludedIndex = args.LastIncludedIndex
	rf.state.lastIncludedTerm = args.LastIncludedTerm
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.state.currentTerm)
	e.Encode(rf.state.votedFor)
	e.Encode(rf.state.lastIncludedIndex)
	e.Encode(rf.state.lastIncludedTerm)
	e.Encode(rf.state.log)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, args.Data)
	msg := ApplyMsg{CommandValid: false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex}
	rf.applyChan <- msg
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
	rf.stateLock.Lock()
	if ok && reply.Term > rf.state.currentTerm {
		rf.state.currentTerm = reply.Term
		rf.state.votedFor = none
		rf.persist()
		rf.toFollower()
	}
	rf.stateLock.Unlock()
	return ok
}

func (rf *Raft) sendAppendEntry(server int) bool {
	if _, isLeader := rf.GetState(); !isLeader {
		return false
	}
	args := AppendEntryArgs{LeaderID: rf.me}
	reply := AppendEntryReply{}
	rf.stateLock.Lock()
	args.Term = rf.state.currentTerm
	args.LeaderCommit = min(rf.state.commitedIndex, rf.state.matchIndex[server])
	args.Entries = make([]LogEntry, 0)
	if rf.state.nextIndex[server] > rf.state.lastIncludedIndex && rf.state.nextIndex[server] <= rf.lastLogIndex() {
		start_idx := rf.state.nextIndex[server] - rf.offset()
		args.Entries = make([]LogEntry, len(rf.state.log)-start_idx)
		copy(args.Entries, rf.state.log[start_idx:])
		args.PrevLogIndex = rf.state.nextIndex[server] - 1
		if args.PrevLogIndex > rf.state.lastIncludedIndex {
			args.PrevLogTerm = rf.state.log[start_idx-1].Term
		} else {
			args.PrevLogTerm = rf.state.lastIncludedTerm
		}
	} else if rf.state.nextIndex[server] > 0 && rf.state.nextIndex[server] <= rf.state.lastIncludedIndex {
		go rf.sendInstallSnapshot(server)
	}
	rf.stateLock.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntry", &args, &reply)
	if _, isLeader := rf.GetState(); !isLeader {
		return false
	}
	if ok {
		if reply.Succuss && len(args.Entries) > 0 {
			rf.stateLock.Lock()
			if (args.PrevLogIndex + len(args.Entries)) > rf.state.matchIndex[server] {
				rf.state.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.state.nextIndex[server] = rf.state.matchIndex[server] + 1
			}
			rf.stateLock.Unlock()
		} else {
			if reply.Term > args.Term {
				rf.stateLock.Lock()
				rf.state.currentTerm = args.Term
				rf.state.votedFor = none
				rf.persist()
				rf.stateLock.Unlock()
				rf.toFollower()
			} else if len(args.Entries) > 0 {
				rf.stateLock.Lock()
				if (args.PrevLogIndex + len(args.Entries)) > rf.state.matchIndex[server] {
					rf.state.nextIndex[server] = (rf.state.nextIndex[server] + rf.state.matchIndex[server]) / 2
				}
				rf.stateLock.Unlock()
			}
		}
	}
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.stateLock.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.state.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.state.lastIncludedIndex,
		LastIncludedTerm:  rf.state.lastIncludedTerm,
		Data:              rf.persister.snapshot}
	reply := InstallSnapshotReply{}
	rf.stateLock.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	rf.stateLock.Lock()
	if ok && reply.Term > rf.state.currentTerm {
		rf.state.currentTerm = reply.Term
		rf.toFollower()
	} else if ok {
		rf.state.matchIndex[server] = max(rf.state.matchIndex[server], args.LastIncludedIndex)
		rf.state.nextIndex[server] = rf.state.matchIndex[server] + 1
	}
	rf.stateLock.Unlock()
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
	_, isLeader := rf.GetState()
	if !isLeader {
		return -1, -1, false
	}
	// Your code here (2B).
	rf.stateLock.Lock()
	defer func() {
		rf.stateLock.Unlock()
		rf.LeaderCommit()
	}()
	term := rf.state.currentTerm
	entry := LogEntry{Term: term, Command: command}
	rf.state.log = append(rf.state.log, entry)
	index := rf.lastLogIndex()
	rf.persist()
	rf.LeaderBoardcast()
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
	var term int
	var idx int
	if len(rf.state.log) > 0 {
		term, idx = rf.state.log[len(rf.state.log)-1].Term, rf.lastLogIndex()
	} else {
		term, idx = rf.state.lastIncludedTerm, rf.state.lastIncludedIndex
	}
	return term, idx
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
			rf.LeaderCommit()
		} else if ty == Candidate {
			rf.voteLock.Lock()
			voted := rf.getVoted
			rf.voteLock.Unlock()
			if voted > len(rf.peers)/2 {
				rf.toLeader()
			} else if rf.getTime() > MaxTime {
				rf.toCandidate()
			}
		} else if ty == Follower {
			if rf.getTime() > MaxTime {
				rf.toCandidate()
			}
		}
		time.Sleep(time.Millisecond * time.Duration(interval/5))
	}
}

func (rf *Raft) toCandidate() {
	rf.typeLock.Lock()
	rf.currentType = Candidate
	rf.typeLock.Unlock()
	time.Sleep(time.Millisecond * time.Duration(rand.Int()%(interval)))
	rf.typeLock.Lock()
	if rf.currentType != Candidate {
		rf.typeLock.Unlock()
		return
	}
	rf.typeLock.Unlock()

	rf.stateLock.Lock()
	rf.state.currentTerm += 1
	rf.state.votedFor = rf.me
	rf.persist()
	// term, idx := rf.getLastTermIndex()
	rf.stateLock.Unlock()

	rf.timeLock.Lock()
	rf.startTime = time.Now()
	rf.timeLock.Unlock()

	rf.voteLock.Lock()
	rf.getVoted = 1
	rf.voteLock.Unlock()

	rf.stateLock.Lock()
	term, idx := rf.getLastTermIndex()
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
	rf.state.nextIndex = make([]int, len(rf.peers))
	rf.state.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.state.nextIndex[i] = max(rf.lastLogIndex(), 1)
		rf.state.matchIndex[i] = 0
	}
	rf.stateLock.Unlock()
	rf.typeLock.Lock()
	rf.currentType = Leader
	rf.typeLock.Unlock()
	rf.LeaderBoardcast()
}

func (rf *Raft) LeaderBoardcast() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			server := i
			go func() {
				rf.sendAppendEntry(server)
			}()
		}
	}
}

func (rf *Raft) LeaderCommit() {
	rf.stateLock.Lock()
	arr := make([]int, 0)
	for i, v := range rf.state.matchIndex {
		if i != rf.me {
			arr = append(arr, v)
		} else {
			arr = append(arr, rf.lastLogIndex())
		}
	}
	sort.Ints(arr)
	nextCommit := arr[len(arr)/2]
	if nextCommit > rf.state.commitedIndex && nextCommit < rf.lastLogIndex()+1 && nextCommit >= rf.state.lastIncludedIndex {
		offset := rf.offset()
		if nextCommit == rf.state.lastIncludedIndex {
			if rf.state.lastIncludedTerm == rf.state.currentTerm {
				rf.state.commitedIndex = nextCommit
			}
		} else if rf.state.log[nextCommit-offset].Term == rf.state.currentTerm {
			rf.state.commitedIndex = nextCommit
		}
	}
	rf.stateLock.Unlock()
}

func (rf *Raft) ApplyWorker() {
	for rf.killed() == false {
		var jobs []ApplyMsg
		rf.stateLock.Lock()
		offset := rf.offset()
		if rf.state.lastApplied < rf.state.commitedIndex {
			if rf.state.lastApplied < rf.state.lastIncludedIndex {
				rf.state.lastApplied = min(rf.state.lastIncludedIndex, rf.state.commitedIndex)
			}
			if len(rf.state.log) > 0 && rf.state.lastApplied >= rf.state.lastIncludedIndex {
				// if len(rf.state.log) > 0 && rf.state.lastApplied+1-offset >= 0 {
				for i := rf.state.lastApplied + 1 - offset; i <= rf.state.commitedIndex-offset; i++ {
					msg := ApplyMsg{
						CommandValid: true,
						Command:      rf.state.log[i].Command,
						CommandIndex: i + offset,
					}
					jobs = append(jobs, msg)
				}
			}
		}
		rf.stateLock.Unlock()
		if len(jobs) > 0 {
			for i := 0; i < len(jobs); i++ {
				rf.applyChan <- jobs[i]
			}
			rf.stateLock.Lock()
			rf.state.lastApplied += len(jobs)
			rf.stateLock.Unlock()
		}
		time.Sleep(time.Millisecond * time.Duration(interval/4))
	}
}
func (rf *Raft) toFollower() {
	rf.typeLock.Lock()
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
	rf.state.log = append(rf.state.log, LogEntry{Term: 0})
	rf.state.lastIncludedIndex = 0
	rf.state.lastIncludedTerm = 0
	rf.currentType = Follower
	rf.startTime = time.Now()
	rf.getVoted = 0
	rf.applyChan = applyCh
	rf.persister = persister
	rf.readPersist(rf.persister.raftstate)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.ApplyWorker()

	return rf
}
