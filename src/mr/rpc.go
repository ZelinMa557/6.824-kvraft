package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// The job type
type JobType int

const (
	MapTask JobType = iota
	ReduceTask
)

type Job struct {
	JobType  JobType
	FileName string
	TaskId   int
	NReduce  int
	NMap     int
	Got      bool
}

type HeartBeatArgs struct {
}

type HeartBeatReply struct {
	Alive bool
}

type JobRequestArgs struct {
}

type JobRequestReply struct {
	Job Job
}

type WorkerRegistArgs struct {
}

type WorkerRegistReply struct {
	Id int
}

type ReportJobArgs struct {
	Job        Job
	FinishTime time.Time
	Successed  bool
	WorkerId   int
}

type ReportJobReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
