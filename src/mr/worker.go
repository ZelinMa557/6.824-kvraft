package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

const (
	REQUEST_INTERVAL = time.Millisecond * 100
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	RegistSuccess := w.Regist()
	if !RegistSuccess {
		fmt.Println("worker: fail to regist")
		return
	}
	w.Run()
}

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (w *worker) ReqJob() Job {
	args := JobRequestArgs{}
	jobReply := JobRequestReply{}
	job := Job{}
	ok := call("Coordinator.JobAssign", &args, &jobReply)
	if ok && jobReply.Job.Got == true {
		job = jobReply.Job
	} else {
		job.Got = false
	}
	return job
}

func (w *worker) DoJob(job Job) {
	switch job.JobType {
	case MapTask:
		w.DoMap(job)
	case ReduceTask:
		w.DoReduce(job)
	}
}

func (w *worker) DoMap(job Job) {
	if job.JobType != MapTask {
		fmt.Println("worker: should not preocess reduce task in DoMap")
		w.Report(job, false)
		return
	}
	// fmt.Println("worker: doing map job, filename: ", job.FileName)
	file, err := os.Open(job.FileName)
	if err != nil {
		w.Report(job, false)
		log.Fatalf("worker: cannot open %v", job.FileName)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		w.Report(job, false)
		log.Fatalf("worker: cannot read %v", job.FileName)
	}
	file.Close()
	kva := w.mapf(job.FileName, string(content))

	//construct the tmp files
	i := 0
	var TmpFileList []*os.File
	var EncoderList []*json.Encoder
	TmpNamePattern := fmt.Sprintf("tmp-out-%v-", job.TaskId)
	for i < job.NReduce {
		TmpFile, _ := ioutil.TempFile("", TmpNamePattern)
		TmpFileList = append(TmpFileList, TmpFile)
		enc := json.NewEncoder(TmpFile)
		EncoderList = append(EncoderList, enc)
		i++
	}
	// assign the kv pair to the reducer
	i = 0
	for i < len(kva) {
		ReduceIdx := ihash(kva[i].Key) % job.NReduce
		EncoderList[ReduceIdx].Encode(kva[i])
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			EncoderList[ReduceIdx].Encode(kva[j])
			j++
		}
		i = j
	}
	// rename the tmp files
	i = 0
	var OutName string
	for i < job.NReduce {
		OutName = fmt.Sprintf("mr-%v-%v", job.TaskId, i)
		os.Rename(TmpFileList[i].Name(), OutName)
		i++
	}
	w.Report(job, true)
}

func (w *worker) DoReduce(job Job) {
	if job.JobType != ReduceTask {
		fmt.Println("worker: should not preocess map task in DoReduce")
		w.Report(job, false)
		return
	}
	// fmt.Println("worker: doing reduce job, id: ", job.TaskId)
	var intermediate []KeyValue
	i := 0
	for i < job.NMap {
		filename := fmt.Sprintf("mr-%v-%v", i, job.TaskId)
		file, err := os.Open(filename)
		if err != nil {
			w.Report(job, false)
			log.Fatalf("worker: cannot open %v", filename)
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		i++
	}
	oname := fmt.Sprintf("mr-out-%v", job.TaskId)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	sort.Sort(ByKey(intermediate))
	i = 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	w.Report(job, true)
}

func (w *worker) Regist() bool {
	args := WorkerRegistArgs{}
	reply := WorkerRegistReply{}
	ok := call("Coordinator.WorkerRegist", &args, &reply)
	if ok {
		w.id = reply.Id
		return true
	} else {
		return false
	}
}

func (w *worker) Run() {
	for {
		args := HeartBeatArgs{}
		reply := HeartBeatReply{}
		ok := call("Coordinator.HeartBeat", &args, &reply)
		if !ok || reply.Alive == false {
			break
		}
		job := w.ReqJob()
		if job.Got {
			w.DoJob(job)
		} else {
			time.Sleep(10 * REQUEST_INTERVAL)
		}
		time.Sleep(REQUEST_INTERVAL)
	}
	fmt.Println("worker exit")
}

func (w *worker) Report(j Job, success bool) {
	args := ReportJobArgs{
		Job:       j,
		Successed: success,
		WorkerId:  w.id,
	}
	args.FinishTime = time.Now()
	reply := ReportJobReply{}
	call("Coordinator.ReportJob", &args, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
