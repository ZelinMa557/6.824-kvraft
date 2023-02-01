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
	"sort"
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
	Command interface{}
	Term    int
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
		fmt.Printf("%v not vote %v for term %v %v\n", rf.me, args.CandidateId, args.Term, rf.state.currentTerm)
		return
	}
	// in the same term, a server should only vote once at most.
	if args.Term == rf.state.currentTerm {
		if rf.state.votedFor != none {
			fmt.Printf("%v not vote %v for voted for %v\n", rf.me, args.CandidateId, rf.state.votedFor)
			return
		}
	} else {
		rf.state.currentTerm = args.Term
		rf.toFollower()
	}
	term, idx := rf.getLastTermIndex()
	if args.LastLogTerm < term {
		fmt.Printf("%v not vote %v for log term mine:%v him: %v\n", rf.me, args.CandidateId, term, args.LastLogTerm)
		return
	} else if args.LastLogTerm == term && args.LastLogIndex < idx {
		fmt.Printf("%v not vote %v for log idx mine:%v him: %v\n", rf.me, args.CandidateId, idx, args.LastLogIndex)
		return
	}
	rf.state.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.timeLock.Lock()
	rf.startTime = time.Now()
	rf.timeLock.Unlock()
	rf.toFollower()
	fmt.Printf("%v vote %v on term %v\n", rf.me, args.CandidateId, rf.state.currentTerm)
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
	}
	// Heartbeat
	rf.timeLock.Lock()
	rf.startTime = time.Now()
	rf.toFollower()
	rf.timeLock.Unlock()
	if len(args.Entries) == 0 {
		reply.Succuss = true
		// If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.state.commitedIndex {
			rf.state.commitedIndex = min(args.LeaderCommit, len(rf.state.log)-1)
			fmt.Printf("follower %v commit to %v\n", rf.me, rf.state.commitedIndex)
		}
		return
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex >= 0 {
		if len(rf.state.log) < args.PrevLogIndex+1 {
			return
		} else if len(rf.state.log) == 0 && args.PrevLogIndex != -1 {
			return
		} else if rf.state.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			return
		}
	}
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	fmt.Printf("%v recv leader %v start %v end %v\n", rf.me, args.LeaderID, args.PrevLogIndex+1, len(args.Entries)+args.PrevLogIndex)
	start := 0
	if len(rf.state.log) > args.PrevLogIndex+1 {
		i := args.PrevLogIndex + 1
		for ; i < len(rf.state.log) && start < len(args.Entries); i++ {
			if rf.state.log[i].Term != args.Entries[start].Term {
				break
			}
			start++
		}
		if i != len(rf.state.log) && start != len(args.Entries) {
			if start < len(args.Entries) {
				fmt.Printf("%v conflict: mine %v leader %v idx: %v\n", rf.me, rf.state.log[i], args.Entries[start], i)
			} else {
				fmt.Printf("%v conflict: mine %v leader %v idx: %v\n", rf.me, rf.state.log[i], args.Entries[len(args.Entries)-1], i)
			}
			rf.state.log = rf.state.log[:i]
			fmt.Printf("%v conflict: last command after delete: %v idx: %v\n", rf.me, rf.state.log[len(rf.state.log)-1], len(rf.state.log)-1)
		}
	}
	// Append any new entries not already in the log
	if start < len(args.Entries) {
		fmt.Printf("Follower %v append log from %v to %v\n", rf.me, args.PrevLogIndex+1+start, args.PrevLogIndex+len(args.Entries))
		// copy := args.Entries[start:]
		// rf.state.log = append(rf.state.log, copy...)
		rf.state.log = append(rf.state.log, args.Entries[start:]...)
		for k, v := range rf.state.log {
			fmt.Printf("idx:%v command:%v ", k, v)
		}
		fmt.Printf("\n")
	}
	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.state.commitedIndex {
		rf.state.commitedIndex = min(args.LeaderCommit, len(rf.state.log)-1)
		fmt.Printf("follower commit to %v\n", rf.state.commitedIndex)
	}
	reply.Succuss = true
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
		rf.toFollower()
	}
	rf.stateLock.Unlock()
	return ok
}

func (rf *Raft) sendAppendEntry(server int) bool {
	args := AppendEntryArgs{LeaderID: rf.me}
	reply := AppendEntryReply{}
	rf.stateLock.Lock()
	args.Term = rf.state.currentTerm
	args.LeaderCommit = rf.state.commitedIndex
	args.Entries = make([]LogEntry, 0)
	if len(rf.state.log) > 0 && rf.state.nextIndex[server] <= (len(rf.state.log)-1) {
		args.Entries = rf.state.log[rf.state.nextIndex[server]:]
		args.PrevLogIndex = rf.state.nextIndex[server] - 1
		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = rf.state.log[args.PrevLogIndex].Term
		}
	}
	rf.stateLock.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntry", &args, &reply)
	if ok {
		if reply.Succuss && len(args.Entries) > 0 {
			rf.stateLock.Lock()
			if (args.PrevLogIndex + len(args.Entries)) > rf.state.matchIndex[server] {
				rf.state.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.state.nextIndex[server] = rf.state.matchIndex[server] + 1
				fmt.Printf("set follower %v next %v maxmatch %v\n", server, rf.state.nextIndex[server], rf.state.matchIndex[server])
			}
			rf.stateLock.Unlock()
		} else {
			if reply.Term > args.Term {
				rf.stateLock.Lock()
				rf.state.currentTerm = args.Term
				rf.state.votedFor = none
				rf.stateLock.Unlock()
				rf.toFollower()
			} else if len(args.Entries) > 0 {
				rf.stateLock.Lock()
				if (args.PrevLogIndex + len(args.Entries)) > rf.state.matchIndex[server] {
					rf.state.nextIndex[server] = (rf.state.nextIndex[server] + rf.state.matchIndex[server]) / 2
					fmt.Printf("no, set follower %v next %v maxmatch %v\n", server, rf.state.nextIndex[server], rf.state.matchIndex[server])
				}
				rf.stateLock.Unlock()
			}
		}
	}
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
	_, isLeader := rf.GetState()
	if !isLeader {
		return -1, -1, false
	}
	// Your code here (2B).
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	term := rf.state.currentTerm
	index := len(rf.state.log) + 1
	entry := LogEntry{Term: term, Command: command}
	rf.state.log = append(rf.state.log, entry)
	rf.LeaderBoardcast()
	fmt.Printf("start command %v %v %v\n", index, term, command)
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
		term = rf.state.log[len(rf.state.log)-1].Term
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
			rf.LeaderCommit()
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
	term, idx := rf.getLastTermIndex()
	fmt.Printf("%v really start election term %v\n", rf.me, rf.state.currentTerm)
	rf.stateLock.Unlock()

	rf.timeLock.Lock()
	rf.startTime = time.Now()
	rf.timeLock.Unlock()

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
	rf.state.nextIndex = make([]int, len(rf.peers))
	rf.state.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.state.nextIndex[i] = max(len(rf.state.log)-1, 0)
		rf.state.matchIndex[i] = -1
	}
	rf.stateLock.Unlock()
	rf.typeLock.Lock()
	rf.currentType = Leader
	fmt.Println(rf.me, "become leader")
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
			arr = append(arr, len(rf.state.log))
		}
	}
	sort.Ints(arr)
	nextCommit := arr[len(arr)/2]
	if nextCommit >= 0 && nextCommit < len(rf.state.log) {
		if nextCommit > rf.state.commitedIndex && rf.state.log[nextCommit].Term == rf.state.currentTerm {
			rf.state.commitedIndex = nextCommit
			fmt.Printf("Leader %v commit to %v\n", rf.me, nextCommit)
		}
	}
	rf.stateLock.Unlock()
}

func (rf *Raft) ApplyWorker() {
	for rf.killed() == false {
		var jobs []ApplyMsg
		rf.stateLock.Lock()
		if len(rf.state.log) > 0 && rf.state.lastApplied < rf.state.commitedIndex {
			for i := rf.state.lastApplied + 1; i <= rf.state.commitedIndex; i++ {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.state.log[i].Command,
					CommandIndex: i + 1,
				}
				jobs = append(jobs, msg)
			}
		}
		rf.stateLock.Unlock()
		if len(jobs) > 0 {
			for i := 0; i < len(jobs); i++ {
				rf.applyChan <- jobs[i]
				fmt.Printf("%v apply index:%v %v\n", rf.me, jobs[i].CommandIndex, jobs[i].Command)
			}
			rf.stateLock.Lock()
			rf.state.lastApplied += len(jobs)
			rf.stateLock.Unlock()
		}
		time.Sleep(time.Millisecond * time.Duration(interval/2))
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
	rf.state.lastApplied = -1
	rf.currentType = Follower
	rf.startTime = time.Now()
	rf.getVoted = 0
	rf.applyChan = applyCh

	fmt.Println("init raft", me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.ApplyWorker()

	return rf
}
