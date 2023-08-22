package mr

/*

MapReduce

Copyright © 2022 Wenda Li

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish,
 distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
 subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

//
// We only wait for 25 seconds. This can be relaxed, whenever the task is computationally heavy or time
// consuming in general...
//
const WaitingTime = 25

//
// Here, we implement the struct as following:
// WorkerID: the running worker process ID
// TaskID: the specific Map/Reduce Task ID
// WorkType: Map/Reduce/Idle/Kill: 0, 1, 2, 3
// WorkStatus: NotAssigned, Running, Done: 0, 1, 2
// For implementation details, see documentation.
//

type Work struct {
	WorkerID   int64
	TaskID     int64
	WorkType   int
	WorkStatus int
	File       string
}

// Types of Works
const (
	Map int = iota
	Reduce
	Idle
	Kill
)

// Types of Work Status
const (
	NotAssigned int = iota
	Running
	Done
)

//
// Map keeps the lists of Maps, Reduce keeps the lists for Reduces
// mu: keep the lock away
// nReduces/nMaps: both are used for keeping the number of Maps and Reduces
//

type Coordinator struct {
	mu           sync.Mutex
	Map          []Work
	nTotalMap    int
	Reduce       []Work
	nTotalReduce int
}

//
// We implement the RPC handler here. The RPC handler is responsible for communication
//

func (c *Coordinator) GetnRemainReduce(Args *int, Reply *NRemainReduceReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	Reply.NReduce = len(c.Reduce)
	return nil
}

func (c *Coordinator) GetAJob(Args *GetAJobArgs, Reply *GetAJobReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// GC is an issue...
	var result *Work
	if c.nTotalMap > 0 {
		GotAJob := false
		for i, job := range c.Map {
			if job.WorkStatus == NotAssigned {
				result = &c.Map[i]
				result.WorkStatus = Running
				result.WorkerID = int64(Args.WorkerID)
				GotAJob = true
				break
			}
		}
		if !GotAJob {
			result = &Work{-1, -1, Idle, Done, ""}
			//fmt.Printf(">> The worker %v did not get the job %v... \n", Args.WorkerID, *result)
		}
	} else if c.nTotalReduce > 0 && c.nTotalMap == 0 {
		GotAJob := false
		for i, job := range c.Reduce {
			if job.WorkStatus == NotAssigned {
				result = &c.Reduce[i]
				result.WorkStatus = Running
				result.WorkerID = int64(Args.WorkerID)
				GotAJob = true
				break
			}
		}
		if !GotAJob {
			result = &Work{-1, -1, Idle, Done, ""}
			//fmt.Printf(">> The worker %v did not get the job %v... \n", Args.WorkerID, *result)
		}
	} else {
		result = &Work{-1, -1, Kill, Done, ""}
	}
	//fmt.Printf(">> The work %v is assigned to worker... \n", *result)
	Reply.WorkType = result.WorkType
	Reply.WorkID = int(result.TaskID)
	Reply.File = result.File

	//fmt.Printf(">> The work %v is assigned to worker... \n", *result)
	// Spawn a goroutine to wait for job to be done ...
	go c.WaitForJob(result)
	return nil
}

//
// We only wait for 25 seconds per task, if that won't work, we dispatch the task to another worker
//
func (c *Coordinator) WaitForJob(job *Work) error {
	if job.WorkType == Idle || job.WorkType == Kill {
		return nil
	}
	<-time.After(WaitingTime * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if job.WorkStatus == Running {
		job.WorkStatus = NotAssigned
		job.WorkerID = -1
		fmt.Println(">> Task timeout... The present worker is fired... Dispatch work to another worker... \n", *job)
	}
	return nil
}

//
// Report the result.
//
func (c *Coordinator) Report(Args *ReportArgs, Reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var job *Work
	if Args.TaskType == Map {
		job = &c.Map[Args.WorkID]
		//log.Printf("The work's running status is: %v \n", job.WorkStatus)
	} else if Args.TaskType == Reduce {
		job = &c.Reduce[Args.WorkID]
	} else {
		fmt.Println(">> Report Error: Incorrect type of tasks...")
		return nil
	}
	// If the job is not overtime AND job is running, terminate the job
	//log.Printf("The task id is %v\n", *job)
	if job.TaskID == int64(Args.WorkID) && job.WorkStatus == Running {
		//fmt.Printf(">> The job %v is done..., remaining maps: %v tasks..., reduces: %v ... \n", *job, c.nTotalMap, c.nTotalReduce)
		job.WorkStatus = Done
		if job.WorkType == Map && c.nTotalMap > 0 {
			c.nTotalMap -= 1
		} else if job.WorkType == Reduce && c.nTotalReduce > 0 {
			c.nTotalReduce -= 1
		}
	}
	// If all jobs are done, inform workers to terminate
	Reply.Kill = (c.nTotalMap == 0) && (c.nTotalReduce == 0)
	return nil
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nTotalMap == 0 && c.nTotalReduce == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nTotalMap = len(files)
	c.nTotalReduce = nReduce
	c.Map = make([]Work, 0, c.nTotalMap)
	c.Reduce = make([]Work, 0, c.nTotalReduce)

	for i := 0; i < c.nTotalMap; i++ {
		MapWork := Work{-1, int64(i), Map, NotAssigned, files[i]}
		c.Map = append(c.Map, MapWork)
	}

	for i := 0; i < nReduce; i++ {
		ReduceWork := Work{-1, int64(i), Reduce, NotAssigned, ""}
		c.Reduce = append(c.Reduce, ReduceWork)
	}

	c.server()

	Output, _ := filepath.Glob("mr-out*")
	for _, file := range Output {
		if err := os.Remove(file); err != nil {
			log.Fatalf(">> file %v cannot be removed ... \n", file)
		}
	}
	err := os.RemoveAll(IntermediateStorage)
	if err != nil {
		log.Fatalf("The intermediate storage cannot be removed: %v\n", IntermediateStorage)
	}
	err = os.Mkdir(IntermediateStorage, 0755)
	if err != nil {
		log.Fatalf("The intermediate storage cannot be created: %v\n", IntermediateStorage)
	}
	return &c
}
