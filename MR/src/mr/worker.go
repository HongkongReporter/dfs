package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"plugin"
	"sort"
	"time"
)

const SleepingTime = 10
const IntermediateStorage = "TmpStore"

var nReduce = 0

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	Args := 0
	Reply := NRemainReduceReply{}
	if Success := call("Coordinator.GetnRemainReduce", &Args, &Reply); !Success {
		log.Fatalf(">> Worker cannot contact the coordinator! RPC call GetnRemainReduce fails...")
	}
	nReduce = Reply.NReduce
	//log.Printf(">> Worker gets %v Reduce jobs\n", nReduce)
	for {
		Reply, Succ := ApplyForJob()
		if !Succ {
			fmt.Println(">> Cannot contact coordinator, please try again... ")
			return
		}
		ShouldKill, Success := false, true
		if Reply.WorkType == Kill {
			fmt.Println(">> The worker has been vested... The work has been all done... ")
			return
		} else if Reply.WorkType == Map {
			//fmt.Println(">> The worker has been deployed to a Map job! ")
			// Do the map part work
			MapWorker(mapf, Reply.File, Reply.WorkID)
			// Reporting the result. If not success, return immediately...
			ShouldKill, Success = ReportDone(Reply.WorkType, Reply.WorkID)
		} else if Reply.WorkType == Reduce {
			//fmt.Println(">> The worker has been deployed to a Reduce job! ")
			// Do the reduce part work
			ReduceWorker(reducef, Reply.File, Reply.WorkID)
			// Reporting the result. If not success, return immediately...
			ShouldKill, Success = ReportDone(Reply.WorkType, Reply.WorkID)
		}
		if ShouldKill {
			fmt.Printf(">> The work is done...\n")
			return
		} else if !Success {
			log.Fatalf(">> The worker cannot contact with coordinator... Connection failure...")
		}
		time.Sleep(time.Millisecond * SleepingTime)
	}

}

func MapWorker(mapf func(string, string) []KeyValue, File string, WorkID int) {
	fd, err := os.Open(File)
	if err != nil {
		log.Fatalf(">> Map worker cannot open file path: %v\n", File)
		return
	}
	line, err := ioutil.ReadAll(fd)
	if err != nil {
		log.Fatalf(">> Map worker cannot open file path: %v\n", File)
		return
	}
	kvp := mapf(File, string(line))

	//First, we create a intermediate storage files, since if fault ever gets triggered, we
	//can simply flush the old file and replace new one...
	IntermediateFilePath := fmt.Sprintf("%v/mr-%v", IntermediateStorage, WorkID)
	rfile := make([]*os.File, 0, nReduce)
	buffer := make([]*bufio.Writer, 0, nReduce)
	encoder := make([]*json.Encoder, 0, nReduce)

	// Write empty files to hard disks
	for i := 0; i < nReduce; i++ {
		PATH := fmt.Sprintf("%v-%v-%v", IntermediateFilePath, i, os.Getpid())
		file, err := os.Create(PATH)
		if err != nil {
			log.Fatalf(">> Map workers cannot create path! \n")
		}
		rfile = append(rfile, file)
		buf := bufio.NewWriter(file)
		buffer = append(buffer, buf)
		encoder = append(encoder, json.NewEncoder(buf))
	}

	// Hash the map function result into the Reduce files
	for _, kv := range kvp {
		hashIndex := ihash(kv.Key) % nReduce
		err := encoder[hashIndex].Encode(&kv)
		if err != nil {
			fmt.Println(">> Encoders do not work! ")
		}
	}

	//flush what we have into hard disks
	for _, buf := range buffer {
		err := buf.Flush()
		if err != nil {
			fmt.Println(">> Data cannot be flushed into hard disks! ")
		}
	}

	//Cleaning...
	for i, file := range rfile {
		file.Close()
		OutPATH := fmt.Sprintf("%v-%v", IntermediateFilePath, i)
		err := os.Rename(file.Name(), OutPATH)
		if err != nil {
			fmt.Println(">> The Map worker cannot rename the files...")
		}
	}
}

func ReduceWorker(reducef func(string, []string) string, File string, WorkID int) {
	match := fmt.Sprintf("%v/mr-%v-%v", IntermediateStorage, "*", WorkID)
	fd, err := filepath.Glob(match)
	if err != nil {
		log.Fatalf(">> Reduce files not found! \n")
	}
	var kv KeyValue
	kvp := make(map[string][]string)
	for _, PATH := range fd {
		file, err := os.Open(PATH)
		if err != nil {
			log.Fatalf(">> Reduce files can not be opened! \n")
		}
		decoder := json.NewDecoder(file)
		for decoder.More() {
			if err := decoder.Decode(&kv); err != nil {
				log.Fatalf(">> Decoding Error! \n")
			}
			kvp[kv.Key] = append(kvp[kv.Key], kv.Value)
		}
	}
	IntermediateFilePath := fmt.Sprintf("%v/mr-%v", IntermediateStorage, WorkID)
	rfile, err := os.Create(IntermediateFilePath)
	if err != nil {
		log.Fatalf(">> Reduce worker cannot create files!")
	}

	//we sort the keys in dictionary order, like in wc application
	Key := make([]string, 0, len(kvp))
	for k := range kvp {
		Key = append(Key, k)
	}
	sort.Strings(Key)

	for _, k := range Key {
		_, err := fmt.Fprintf(rfile, "%v %v\n", k, reducef(k, kvp[k]))
		if err != nil {
			log.Fatalf(">> Reduce worker cannot write to files!")
		}
	}
	rfile.Close()
	OutPATH := fmt.Sprintf("mr-out-%v", WorkID)
	err = os.Rename(rfile.Name(), OutPATH)
	if err != nil {
		fmt.Println(">> The Map worker cannot rename the files...")
	}
}

//
// We implement the application here, using RPC.
//
func ApplyForJob() (*GetAJobReply, bool) {
	Args := GetAJobArgs{os.Getpid()}
	Reply := GetAJobReply{}
	succ := call("Coordinator.GetAJob", &Args, &Reply)
	return &Reply, succ
}

func ReportDone(WorkType int, WorkID int) (bool, bool) {
	Args := ReportArgs{os.Getpid(), WorkType, WorkID}
	Reply := ReportReply{}
	Success := call("Coordinator.Report", &Args, &Reply)
	return Reply.Kill, Success
}

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

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
