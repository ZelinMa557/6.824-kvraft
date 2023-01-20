package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	ScheduleDuration = time.Millisecond * 300
	MaxJobTime       = time.Millisecond * 10000
)

type JobState int

const (
	JobWait JobState = iota
	JobRun
	JobDone
)

type JobInfo struct {
	State     JobState
	StartTime time.Time
	WorkerId  int
}

type CoordinatorState int

const (
	MapPhase CoordinatorState = iota
	ReducePhase
	Done
)

type Coordinator struct {
	// Your definitions here.
	StateMutex      sync.Mutex
	MapMutex        sync.Mutex
	ReduceMutex     sync.Mutex
	MapJobsInfos    map[int]JobInfo
	ReduceJobsInfos map[int]JobInfo
	MapJobs         []Job
	ReduceJobs      []Job
	NMap            int
	NReduce         int
	Files           []string
	State           CoordinatorState
}

func (c *Coordinator) InitMapJobs(files []string) {
	fmt.Println("Initing map jobs")
	c.MapMutex.Lock()
	i := 0
	for i < len(files) {
		job := Job{
			JobType:  MapTask,
			FileName: files[i],
			TaskId:   i,
			NReduce:  c.NReduce,
			NMap:     c.NMap,
			Got:      true,
		}
		c.MapJobs = append(c.MapJobs, job)
		info := JobInfo{State: JobWait}
		c.MapJobsInfos[i] = info
		i++
	}
	c.MapMutex.Unlock()
	fmt.Println("Finish init map job")
}

func (c *Coordinator) InitReduceJobs() {
	fmt.Println("Initing Reduce job")
	c.ReduceMutex.Lock()
	defer c.ReduceMutex.Unlock()
	i := 0
	for i < c.NReduce {
		job := Job{
			JobType: ReduceTask,
			TaskId:  i,
			NReduce: c.NReduce,
			NMap:    c.NMap,
			Got:     true,
		}
		c.ReduceJobs = append(c.ReduceJobs, job)
		info := JobInfo{State: JobWait}
		c.ReduceJobsInfos[i] = info
		i++
	}
	fmt.Println("Finish initing reduce job")
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) WorkerRegist(args *WorkerRegistArgs, reply *WorkerRegistReply) error {
	reply.Id = 0
	fmt.Println("new worker")
	return nil
}
func (c *Coordinator) JobAssign(args *JobRequestArgs, reply *JobRequestReply) error {
	c.StateMutex.Lock()
	state := c.State
	c.StateMutex.Unlock()
	switch state {
	case MapPhase:
		c.MapMutex.Lock()
		i := 0
		for i < c.NMap {
			if c.MapJobsInfos[i].State == JobWait {
				reply.Job = c.MapJobs[i]
				info := c.MapJobsInfos[i]
				info.State = JobRun
				info.StartTime = time.Now()
				info.WorkerId = 0 // todo
				c.MapJobsInfos[i] = info
				c.MapMutex.Unlock()
				fmt.Println("Assign map job. file name: ", c.MapJobs[i].FileName, "start time: ", info.StartTime)
				return nil
			}
			i++
		}
		fmt.Println("Map phase : no job avalible")
		c.MapMutex.Unlock()
	case ReducePhase:
		c.ReduceMutex.Lock()
		i := 0
		for i < c.NReduce {
			if c.ReduceJobsInfos[i].State == JobWait {
				reply.Job = c.ReduceJobs[i]
				info := c.ReduceJobsInfos[i]
				info.State = JobRun
				info.StartTime = time.Now()
				info.WorkerId = 0 // todo
				c.ReduceJobsInfos[i] = info
				fmt.Println("Assign reduce job, id: ", i, "total: ", c.NReduce, "start time: ", info.StartTime)
				c.ReduceMutex.Unlock()
				return nil
			}
			i++
		}
		c.ReduceMutex.Unlock()
	}
	DummyJob := Job{Got: false}
	reply.Job = DummyJob
	return nil
}

func (c *Coordinator) ReportJob(args *ReportJobArgs, reply *ReportJobReply) error {
	c.StateMutex.Lock()
	state := c.State
	c.StateMutex.Unlock()
	job := args.Job
	id := job.TaskId
	if job.JobType == MapTask && state == MapPhase {
		c.MapMutex.Lock()
		defer c.MapMutex.Unlock()
		if c.MapJobsInfos[id].WorkerId == args.WorkerId {
			info := c.MapJobsInfos[id]
			if args.Successed {
				info.State = JobDone
				fmt.Println("Map job finish: ", c.MapJobs[id].FileName)
			} else {
				info.State = JobWait
				fmt.Println("Map job fail: ", c.MapJobs[id].FileName)
			}
			c.MapJobsInfos[id] = info
		}
	} else if job.JobType == ReduceTask && state == ReducePhase {
		c.ReduceMutex.Lock()
		defer c.ReduceMutex.Unlock()
		if c.ReduceJobsInfos[id].WorkerId == args.WorkerId {
			info := c.ReduceJobsInfos[id]
			if args.Successed {
				info.State = JobDone
				fmt.Println("Reduce job finish: ", c.ReduceJobs[id].TaskId)
			} else {
				info.State = JobWait
				fmt.Println("Reduce job fail: ", c.ReduceJobs[id].FileName)
			}
			c.ReduceJobsInfos[id] = info
		}
	}
	return nil
}

func (c *Coordinator) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	c.StateMutex.Lock()
	defer c.StateMutex.Unlock()
	if c.State == Done {
		reply.Alive = false
	} else {
		reply.Alive = true
	}
	return nil
}
func (c *Coordinator) schedule() {
	for c.State != Done {
		if c.State == MapPhase {
			finish := true
			i := 0
			c.MapMutex.Lock()
			for i < c.NMap {
				info := c.MapJobsInfos[i]
				if info.State != JobDone {
					finish = false
					if info.State == JobRun && time.Since(info.StartTime).Milliseconds() > MaxJobTime.Milliseconds() {
						info.State = JobWait
						c.MapJobsInfos[i] = info
						fmt.Println("Map job timeout: ", c.MapJobs[i].FileName)
					}
				}
				i++
			}
			c.MapMutex.Unlock()
			if finish {
				c.StateMutex.Lock()
				c.State = ReducePhase
				c.InitReduceJobs()
				c.StateMutex.Unlock()
			}
		} else if c.State == ReducePhase {
			finish := true
			i := 0
			c.ReduceMutex.Lock()
			for i < c.NReduce {
				info := c.ReduceJobsInfos[i]
				if info.State != JobDone {
					finish = false
					if info.State == JobRun && time.Since(info.StartTime).Milliseconds() > MaxJobTime.Milliseconds() {
						info.State = JobWait
						c.ReduceJobsInfos[i] = info
						fmt.Println("Reduce job timeout: ", c.ReduceJobs[i].TaskId)
					}
				}
				i++
			}
			c.ReduceMutex.Unlock()
			if finish {
				c.StateMutex.Lock()
				c.State = Done
				c.StateMutex.Unlock()
				fmt.Println("All job done!")
			}
		}
		time.Sleep(ScheduleDuration)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.StateMutex.Lock()
	defer c.StateMutex.Unlock()
	return c.State == Done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NMap = len(files)
	c.NReduce = nReduce
	c.MapJobsInfos = make(map[int]JobInfo)
	c.ReduceJobsInfos = make(map[int]JobInfo)
	c.InitMapJobs(files)
	fmt.Println("init coordinator NMap: ", len(files), " NReduce", nReduce)
	go c.schedule()
	c.server()
	return &c
}
